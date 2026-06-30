/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	garagev1alpha1 "github.com/QuantumDancer/garage-operator/api/v1alpha1"
	"github.com/QuantumDancer/garage-operator/internal/garageadmin"
)

// reconcileLayout converges the Garage layout toward the desired roles. A purely additive
// change (new nodes, capacity/zone refinements) is applied immediately. A destructive change
// — one that removes a node from the layout — drains data off that node, so it is previewed
// and held behind the approval annotation, and only when approved is it applied and the
// now-surplus workload (StatefulSets / PVCs) torn down. status.Health must already be set:
// the destructive guardrail reads it.
func (r *GarageClusterReconciler) reconcileLayout(ctx context.Context, cluster *garagev1alpha1.GarageCluster, status *garagev1alpha1.GarageClusterStatus, layoutClient clusterAdmin, desired []nodeEndpoint) error {
	plan, err := layoutClient.PlanLayout(ctx, desiredRoles(desired))
	if err != nil {
		setCondition(status, conditionLayoutApplied, metav1.ConditionFalse, "LayoutError", err.Error())
		return err
	}

	if !plan.IsDestructive() {
		version := plan.CurrentVersion
		if plan.HasChanges() {
			// Clear anything a prior crash may have left staged first, so the apply commits exactly
			// this plan. PlanLayout diffs only the applied layout, so a change staged-but-never-applied
			// by a crashed pass (then reverted in spec) is invisible to the diff yet would ride along
			// in the version+1 union this ApplyLayout commits — applying a change the user never approved.
			if err := layoutClient.RevertStagedChanges(ctx); err != nil {
				setCondition(status, conditionLayoutApplied, metav1.ConditionFalse, "LayoutError", err.Error())
				return err
			}
			if err := layoutClient.StageLayoutChanges(ctx, plan.AdditiveChanges); err != nil {
				setCondition(status, conditionLayoutApplied, metav1.ConditionFalse, "LayoutError", err.Error())
				return err
			}
			if err := layoutClient.ApplyLayout(ctx, plan.TargetVersion); err != nil {
				setCondition(status, conditionLayoutApplied, metav1.ConditionFalse, "LayoutError", err.Error())
				return err
			}
			version = plan.TargetVersion
		}
		setCondition(status, conditionLayoutApplied, metav1.ConditionTrue, "LayoutApplied", fmt.Sprintf("Cluster layout version %d applied", version))
		meta.RemoveStatusCondition(&status.Conditions, conditionLayoutChangePending)
		status.Layout = buildLayoutStatus(desired, version)
		// Tear down any workload that is surplus to spec but already out of the layout. This branch
		// is reached only when the plan is non-destructive, i.e. no applied-layout node is outside
		// the desired set — so every spec-surplus node (a shrunk pool's high ordinals, a removed
		// pool's nodes) has already been drained and is safe to remove. Running it here on every
		// pass turns the one-time tail-of-apply teardown into an idempotent invariant that also
		// retries a teardown a prior approved pass left half-finished (REVIEW.md #6).
		if err := r.reconcileRemovedWorkload(ctx, cluster); err != nil {
			return err
		}
		return nil
	}

	return r.reconcileDestructiveLayout(ctx, cluster, status, layoutClient, desired, plan)
}

// desiredZoneRedundancy resolves the spec's zone-redundancy parameter, applying the operator
// default (Maximum) when the block is omitted or its mode is empty. kubebuilder cannot fire the
// nested mode default when the parent object is absent (the CRD nested-defaults gap), so the
// default is applied here in the controller.
func desiredZoneRedundancy(cluster *garagev1alpha1.GarageCluster) garageadmin.ZoneRedundancyValue {
	zr := cluster.Spec.ZoneRedundancy
	if zr == nil || zr.Mode == "" || zr.Mode == garagev1alpha1.ZoneRedundancyMaximum {
		return garageadmin.ZoneRedundancyValue{Maximum: true}
	}
	return garageadmin.ZoneRedundancyValue{AtLeast: zr.AtLeast}
}

// distinctZones counts the distinct zones across the desired node set.
func distinctZones(desired []nodeEndpoint) int {
	zones := make(map[string]struct{}, len(desired))
	for _, n := range desired {
		zones[n.zone] = struct{}{}
	}
	return len(zones)
}

// reconcileZoneRedundancy converges the layout's cross-zone replication parameter toward spec.
// The change is ungated (PLAN §10): it applies as soon as it is observed, guarded only by hard
// preconditions — it never requests more zones than the layout actually has (Garage rejects
// such a layout) and never starts the resulting rebalance from a degraded cluster. A blocked or
// deferred change leaves the parameter untouched: a too-high AtLeast is surfaced as a
// LayoutApplied=False misconfiguration, while an unhealthy cluster simply defers to a later
// pass. The cluster keeps serving throughout, so Ready is left to the caller. status.Health
// must already be set.
func (r *GarageClusterReconciler) reconcileZoneRedundancy(ctx context.Context, cluster *garagev1alpha1.GarageCluster, status *garagev1alpha1.GarageClusterStatus, layoutClient clusterAdmin, desired []nodeEndpoint) error {
	want := desiredZoneRedundancy(cluster)
	current, err := layoutClient.CurrentZoneRedundancy(ctx)
	if err != nil {
		return err
	}
	if current.Equal(want) {
		return nil
	}

	if !want.Maximum {
		// Guardrail: mode AtLeast requires a positive atLeast. The kubebuilder Minimum=1 marker is
		// only enforced when the field is present, so `{mode: AtLeast}` with atLeast omitted slips
		// through admission as 0. Refuse it here rather than ship a nonsensical atLeast:0 to Garage
		// (a validating webhook will reject it at admission in Phase 6).
		if want.AtLeast < 1 {
			msg := "Invalid zone redundancy: mode AtLeast requires atLeast >= 1"
			setCondition(status, conditionLayoutApplied, metav1.ConditionFalse, "ZoneRedundancyInvalid", msg)
			r.eventf(cluster, corev1.EventTypeWarning, "ZoneRedundancyBlocked", msg)
			return nil
		}
		// Guardrail: AtLeast can never exceed the number of zones in the layout — Garage would
		// reject the layout. Refuse without touching it and surface the misconfiguration.
		if zones := distinctZones(desired); want.AtLeast > zones {
			msg := fmt.Sprintf("Cannot set zone redundancy to %s: layout has only %d zone(s)", want, zones)
			setCondition(status, conditionLayoutApplied, metav1.ConditionFalse, "ZoneRedundancyInvalid", msg)
			r.eventf(cluster, corev1.EventTypeWarning, "ZoneRedundancyBlocked", msg)
			return nil
		}
	}

	// Guardrail: applying the change rebalances data across zones, so never start from a
	// degraded cluster. Defer until healthy; the steady-state requeue retries.
	if status.Health == nil || status.Health.Status != healthStatusHealthy {
		logf.FromContext(ctx).Info("Deferring zone-redundancy change until the cluster is healthy", "desired", want.String())
		return nil
	}

	version, err := layoutClient.SetZoneRedundancy(ctx, want)
	if err != nil {
		return err
	}
	if status.Layout != nil {
		status.Layout.Version = version
	}
	r.eventf(cluster, corev1.EventTypeNormal, "ZoneRedundancyApplied", fmt.Sprintf("Applied zone redundancy %s as layout version %d", want, version))
	logf.FromContext(ctx).Info("Applied zone redundancy", "redundancy", want.String(), "version", version)
	return nil
}

// reconcileDestructiveLayout handles a plan that drains one or more nodes. It enforces the
// safety guardrails, previews the change, and gates the apply behind the approval annotation.
func (r *GarageClusterReconciler) reconcileDestructiveLayout(ctx context.Context, cluster *garagev1alpha1.GarageCluster, status *garagev1alpha1.GarageClusterStatus, layoutClient clusterAdmin, desired []nodeEndpoint, plan *garageadmin.LayoutPlan) error {
	log := logf.FromContext(ctx)

	// Guardrail: with replicationFactor < 2 a drained node's data exists on no other node,
	// so removing it is unrecoverable data loss. Refuse outright, even if approved.
	if cluster.Spec.ReplicationFactor < 2 {
		msg := fmt.Sprintf("Refusing to remove %d node(s): replicationFactor %d leaves no replica to recover their data", len(plan.Removals), cluster.Spec.ReplicationFactor)
		r.blockLayoutChange(ctx, cluster, status, layoutClient, msg)
		return nil
	}

	// Guardrail: draining a node temporarily reduces redundancy, so never start from an
	// already-degraded cluster.
	if status.Health == nil || status.Health.Status != healthStatusHealthy {
		msg := "Refusing to remove nodes while the cluster is not healthy"
		r.blockLayoutChange(ctx, cluster, status, layoutClient, msg)
		return nil
	}

	// Stage the change so Garage can compute a preview of the resulting layout and rebalance.
	// Clear anything a prior crash may have left staged first, so the preview reflects exactly
	// this plan.
	if err := layoutClient.RevertStagedChanges(ctx); err != nil {
		return err
	}
	staged, err := plan.StagedChanges()
	if err != nil {
		return err
	}
	if err := layoutClient.StageLayoutChanges(ctx, staged); err != nil {
		return err
	}
	preview, err := layoutClient.PreviewStagedChanges(ctx)
	if err != nil {
		_ = layoutClient.RevertStagedChanges(ctx)
		return err
	}

	if cluster.Annotations[annotationApproveLayout] != strconv.FormatInt(plan.TargetVersion, 10) {
		// Not approved: discard the staged preview — nothing is committed until approved.
		if err := layoutClient.RevertStagedChanges(ctx); err != nil {
			return err
		}
		msg := pendingLayoutMessage(plan, preview)
		setCondition(status, conditionLayoutChangePending, metav1.ConditionTrue, "ApprovalRequired", msg)
		r.eventf(cluster, corev1.EventTypeNormal, "LayoutChangePending", msg)
		// Leave status.Layout untouched: until the drain is applied the live layout still holds
		// every node, so the carried-over last-applied layout is the accurate report.
		log.Info("Destructive layout change awaiting approval", "targetVersion", plan.TargetVersion, "removals", len(plan.Removals))
		return nil
	}

	// Approved: commit the drain, then tear down the now-surplus workload.
	if err := layoutClient.ApplyLayout(ctx, plan.TargetVersion); err != nil {
		setCondition(status, conditionLayoutApplied, metav1.ConditionFalse, "LayoutError", err.Error())
		return err
	}
	setCondition(status, conditionLayoutApplied, metav1.ConditionTrue, "LayoutApplied", fmt.Sprintf("Cluster layout version %d applied", plan.TargetVersion))
	meta.RemoveStatusCondition(&status.Conditions, conditionLayoutChangePending)
	r.eventf(cluster, corev1.EventTypeNormal, "LayoutChangeApplied", fmt.Sprintf("Applied layout version %d, drained %d node(s)", plan.TargetVersion, len(plan.Removals)))
	if err := r.reconcileRemovedWorkload(ctx, cluster); err != nil {
		return err
	}
	status.Layout = buildLayoutStatus(desired, plan.TargetVersion)
	log.Info("Applied destructive layout change", "version", plan.TargetVersion, "removals", len(plan.Removals))
	return nil
}

// blockLayoutChange records a refused destructive change: it discards any staged changes,
// sets the LayoutChangePending condition to False with the reason, and emits a Warning Event.
// The cluster itself stays Ready — it is healthy, the operator is simply declining the change.
// status.Layout is left untouched: the live layout still holds every node, so the carried-over
// last-applied layout remains the accurate report.
func (r *GarageClusterReconciler) blockLayoutChange(ctx context.Context, cluster *garagev1alpha1.GarageCluster, status *garagev1alpha1.GarageClusterStatus, layoutClient clusterAdmin, msg string) {
	setCondition(status, conditionLayoutChangePending, metav1.ConditionFalse, reasonBlocked, msg)
	r.eventf(cluster, corev1.EventTypeWarning, "LayoutChangeBlocked", msg)
	_ = layoutClient.RevertStagedChanges(ctx)
}

// reconcileRemovedWorkload removes the in-cluster workload for nodes that have been drained
// from the layout: it scales shrunk pools down to their desired replica count and deletes
// the StatefulSets of pools removed from spec entirely, deleting the orphaned PVCs in both
// cases. It is idempotent — a pool already at its desired size is left untouched — so it is
// safe to re-run after a crash between ApplyLayout and teardown.
//
// It runs both right after an approved destructive ApplyLayout (prompt teardown in the same
// pass) and at the end of the additive branch (every-pass retry/invariant). The additive call
// is safe because that branch is reached only when the applied layout already excludes every
// spec-surplus node — so this is keyed on spec but never tears down a node whose drain has not
// yet been applied; while a drain is still pending or unapproved the destructive branch runs
// instead and this is not called.
func (r *GarageClusterReconciler) reconcileRemovedWorkload(ctx context.Context, cluster *garagev1alpha1.GarageCluster) error {
	desiredReplicas := make(map[string]int32, len(cluster.Spec.NodePools))
	for i := range cluster.Spec.NodePools {
		pool := &cluster.Spec.NodePools[i]
		desiredReplicas[statefulSetName(cluster, pool)] = pool.Replicas
	}

	var sets appsv1.StatefulSetList
	if err := r.List(ctx, &sets, client.InNamespace(cluster.Namespace), client.MatchingLabels{labelCluster: cluster.Name}); err != nil {
		return err
	}

	for i := range sets.Items {
		ss := &sets.Items[i]
		current := replicaCount(ss)

		want, kept := desiredReplicas[ss.Name]
		if !kept {
			// Whole pool removed from spec: delete the StatefulSet and all its claims.
			if err := r.deleteStatefulSet(ctx, ss); err != nil {
				return err
			}
			if err := r.deleteClaims(ctx, cluster.Namespace, ss.Name, 0, current); err != nil {
				return err
			}
			continue
		}

		if current <= want {
			continue
		}
		// Pool scaled down: reduce replicas (deletes the highest-ordinal pods, which are the
		// drained nodes) and delete their orphaned claims.
		ss.Spec.Replicas = ptr.To(want)
		if err := r.Update(ctx, ss); err != nil {
			return err
		}
		if err := r.deleteClaims(ctx, cluster.Namespace, ss.Name, want, current); err != nil {
			return err
		}
	}
	return nil
}

func (r *GarageClusterReconciler) deleteStatefulSet(ctx context.Context, ss *appsv1.StatefulSet) error {
	// Background propagation: the StatefulSet object is removed promptly and its pods are
	// garbage-collected via their owner references. The drain has already moved the data off
	// these nodes, so the pods need no graceful ordering here.
	if err := r.Delete(ctx, ss, client.PropagationPolicy(metav1.DeletePropagationBackground)); err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	return nil
}

// deleteClaims deletes the per-pod PVCs (both the meta and data claims) for the ordinal range
// [from, to). StatefulSet scale-down never deletes volumeClaimTemplates PVCs, so the operator
// reclaims them explicitly once the node's data has been redistributed by the drain.
func (r *GarageClusterReconciler) deleteClaims(ctx context.Context, namespace, statefulSet string, from, to int32) error {
	for ordinal := from; ordinal < to; ordinal++ {
		for _, volume := range []string{volumeNameMeta, volumeNameData} {
			pvc := &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      claimName(volume, statefulSet, ordinal),
					Namespace: namespace,
				},
			}
			if err := r.Delete(ctx, pvc); err != nil && !apierrors.IsNotFound(err) {
				return err
			}
		}
	}
	return nil
}

func (r *GarageClusterReconciler) eventf(cluster *garagev1alpha1.GarageCluster, eventType, reason, message string) {
	if r.Recorder != nil {
		r.Recorder.Event(cluster, eventType, reason, message)
	}
}

// pendingLayoutMessage describes a gated destructive change for the LayoutChangePending
// condition: the target version, the nodes being removed, the exact annotation to set to
// approve, and Garage's own preview of the resulting layout.
func pendingLayoutMessage(plan *garageadmin.LayoutPlan, preview []string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "Destructive layout change to version %d awaiting approval: removing %d node(s) [%s]. ",
		plan.TargetVersion, len(plan.Removals), strings.Join(plan.Removals, ", "))
	fmt.Fprintf(&b, "Set annotation %s=%d to approve.", annotationApproveLayout, plan.TargetVersion)
	if len(preview) > 0 {
		fmt.Fprintf(&b, " Preview: %s", strings.Join(preview, "; "))
	}
	return b.String()
}
