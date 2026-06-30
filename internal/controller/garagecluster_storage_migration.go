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
	"slices"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	garagev1alpha1 "github.com/QuantumDancer/garage-operator/api/v1alpha1"
	"github.com/QuantumDancer/garage-operator/internal/garageadmin"
)

// reconcileStorageMigration carries out a volume change that the in-place path cannot serve —
// a size shrink, or a grow on a StorageClass that forbids expansion (Path B, PLAN.md §4.5).
// Such a change requires recreating a node's PersistentVolumeClaims, which a StatefulSet can
// only do for an empty volume, so the operator rolls through the pool one node at a time:
// drain the node from the layout (Garage redistributes its data), wait until the cluster is
// fully re-replicated, recreate the node's volumes at the new spec, and re-add the node so
// Garage refills it. The per-node progress lives in status.storageMigration.
//
// It returns active=true while a pool's migration is in progress, telling Reconcile to skip the
// generic layout reconciliation and requeue: the migration owns the layout for the node it is
// moving, and suppressing the generic path keeps it from applying the node's new (smaller/larger)
// capacity before the volume actually exists. A migration refused by a guardrail instead returns
// active=false once the blocked pool's capacity is pinned to its current (unmigrated) value, so an
// impossible storage edit no longer freezes every unrelated cluster operation (REVIEW.md #5); the
// refusal is surfaced via the StorageChangePending=False/Blocked condition. active=false also
// means there was simply nothing to migrate.
//
// desired carries the nodes discovered this pass (pod -> node id -> zone -> new capacity); the
// migration reads it to learn a node's current id and the role to re-add it with.
func (r *GarageClusterReconciler) reconcileStorageMigration(ctx context.Context, cluster *garagev1alpha1.GarageCluster, status *garagev1alpha1.GarageClusterStatus, layoutClient clusterAdmin, desired []nodeEndpoint) (active bool, err error) {
	step, err := r.migrationStep(ctx, cluster, status)
	if err != nil {
		return false, err
	}
	if step == nil {
		// Nothing to migrate: clear any leftover progress and the blocking condition a prior
		// guardrail may have set, then let the generic layout path run.
		status.StorageMigration = nil
		meta.RemoveStatusCondition(&status.Conditions, conditionStorageChangePending)
		return false, nil
	}

	switch step.phase {
	case garagev1alpha1.StorageMigrationDraining:
		return r.migrationDrain(ctx, cluster, status, layoutClient, desired, step)
	case garagev1alpha1.StorageMigrationAwaitingReplication:
		return r.migrationAwaitReplication(ctx, status, layoutClient, step)
	case garagev1alpha1.StorageMigrationRecreatingVolume:
		return r.migrationRecreateVolume(ctx, cluster, status, step)
	case garagev1alpha1.StorageMigrationAwaitingRejoin:
		return r.migrationAwaitRejoin(ctx, cluster, status, layoutClient, desired, step)
	default:
		// An unrecognized phase (e.g. a hand-edited status) restarts the node from the top; the
		// phases are idempotent, so resuming at Draining is safe.
		step.phase = garagev1alpha1.StorageMigrationDraining
		return r.migrationDrain(ctx, cluster, status, layoutClient, desired, step)
	}
}

// migrationStep is the node the migration is (or should be) working on, resolved against the
// live world. pool/ss are the live workload; ordinal is the node within the pool; phase is the
// stage to run.
type migrationStep struct {
	pool    *garagev1alpha1.NodePool
	ss      *appsv1.StatefulSet
	ordinal int32
	phase   garagev1alpha1.StorageMigrationPhase
}

// migrationStep resolves what to work on. An in-progress migration recorded in status is
// resumed at its phase; otherwise the pools are scanned in order and the first node whose
// volumes need migrating starts a fresh one at the Draining phase. It returns nil when no node
// needs migration. A migration never *starts* on a pool with a pending gated scale-down (a
// StatefulSet running more replicas than desired): the scale-down drain runs through the
// generic layout path, which the migration would suppress, so it is allowed to finish first.
func (r *GarageClusterReconciler) migrationStep(ctx context.Context, cluster *garagev1alpha1.GarageCluster, status *garagev1alpha1.GarageClusterStatus) (*migrationStep, error) {
	if m := status.StorageMigration; m != nil {
		pool := findPool(cluster, m.Pool)
		if pool == nil {
			return nil, nil // pool removed from spec mid-migration: abandon.
		}
		ss, err := r.getStatefulSet(ctx, cluster, pool)
		if err != nil || ss == nil {
			return nil, err
		}
		return &migrationStep{pool: pool, ss: ss, ordinal: m.Ordinal, phase: m.Phase}, nil
	}

	for i := range cluster.Spec.NodePools {
		pool := &cluster.Spec.NodePools[i]
		ss, err := r.getStatefulSet(ctx, cluster, pool)
		if err != nil {
			return nil, err
		}
		if ss == nil || ss.DeletionTimestamp != nil {
			continue
		}
		if replicaCount(ss) != pool.Replicas {
			continue // let a pending scale-down drain finish before migrating.
		}
		ordinal, found, err := r.poolMigrationTarget(ctx, cluster, pool, ss)
		if err != nil {
			return nil, err
		}
		if found {
			return &migrationStep{pool: pool, ss: ss, ordinal: ordinal, phase: garagev1alpha1.StorageMigrationDraining}, nil
		}
	}
	return nil, nil
}

// migrationDrain removes the node from the layout so Garage redistributes its data, after
// enforcing the safety guardrails. It is the only phase that checks the guardrails: once a
// node is past Draining the cluster is expected to be transiently degraded, so re-checking
// health later would wrongly block the very migration that caused the dip.
func (r *GarageClusterReconciler) migrationDrain(ctx context.Context, cluster *garagev1alpha1.GarageCluster, status *garagev1alpha1.GarageClusterStatus, layoutClient clusterAdmin, desired []nodeEndpoint, step *migrationStep) (bool, error) {
	log := logf.FromContext(ctx)

	// Guardrail: draining a node temporarily removes a replica, so with replicationFactor < 2
	// the node's data exists nowhere else and recreating its volume is unrecoverable loss.
	if cluster.Spec.ReplicationFactor < 2 {
		return r.blockMigration(cluster, status, desired, step.pool,
			fmt.Sprintf("Refusing storage migration of pool %q: replicationFactor %d leaves no replica to recover a drained node's data",
				step.pool.Name, cluster.Spec.ReplicationFactor)), nil
	}

	// Guardrail: the migration recreates the node's PVCs on the pool's desired StorageClass. If
	// that class is empty or does not exist (a typo, or an explicit ""), the recreated PVC would
	// never bind — and by the time that shows the node is already drained and wiped, leaving it
	// stuck unrecoverably. Refuse before the destructive drain, mirroring the in-place path, which
	// validates the class up front. A nil class (the cluster default) is resolved by the API server
	// at bind time and is always allowed.
	if reason, err := r.unresolvableStorageClass(ctx, step.pool); err != nil {
		return false, err
	} else if reason != "" {
		return r.blockMigration(cluster, status, desired, step.pool,
			fmt.Sprintf("Refusing storage migration of pool %q: %s", step.pool.Name, reason)), nil
	}

	node, ok := nodeEndpointForOrdinal(desired, cluster, step.pool, step.ordinal)
	if !ok {
		// The node is not reachable this pass; wait for it to come back before draining.
		return true, nil
	}

	ids, err := layoutClient.AppliedLayoutNodeIDs(ctx)
	if err != nil {
		return false, err
	}
	if slices.Contains(ids, node.nodeID) {
		// Still in the layout: the drain has not happened yet, so refuse to start it from an
		// already-degraded cluster (a drain reduces redundancy further).
		if status.Health == nil || status.Health.Status != healthStatusHealthy {
			return r.blockMigration(cluster, status, desired, step.pool,
				fmt.Sprintf("Refusing to start storage migration of pool %q node %d while the cluster is not healthy", step.pool.Name, step.ordinal)), nil
		}
		if err := layoutClient.RemoveNode(ctx, node.nodeID); err != nil {
			return false, err
		}
		r.eventf(cluster, corev1.EventTypeNormal, "StorageMigrationDraining",
			fmt.Sprintf("Draining pool %q node %d from the layout to recreate its volumes", step.pool.Name, step.ordinal))
		log.Info("Draining node for storage migration", "pool", step.pool.Name, "ordinal", step.ordinal)
	}

	// Drained (just now or on a prior pass): advance to wait for re-replication.
	meta.RemoveStatusCondition(&status.Conditions, conditionStorageChangePending)
	r.setMigrationStatus(status, step, garagev1alpha1.StorageMigrationAwaitingReplication,
		fmt.Sprintf("Draining node %d; waiting for the cluster to re-replicate", step.ordinal))
	// Stamp the drain time on first entry to AwaitingReplication so the destroy gate can enforce
	// minResyncSettle. Stamping here covers both the just-drained and the already-drained
	// idempotent re-entry paths; the first stamp is the conservative one (it never moves later).
	if status.StorageMigration.DrainedAt == nil {
		status.StorageMigration.DrainedAt = ptr.To(metav1.Now())
	}
	return true, nil
}

// migrationAwaitReplication holds until the drained data has actually moved onto the remaining
// nodes before this node's volume is destroyed. partitionsAllOk is NOT sufficient: Garage
// computes it as a pure node-connectivity count (write_sets.all(node_up) in src/rpc/system.rs,
// where node_up is just peer is_up), and once RemoveNode applies the new layout the responsible
// nodes are the already-up survivors — so partitionsAllOk reads full within ~10s of the drain
// while block-resync is still copying data in the background. Destroying the volume then would
// lose the not-yet-moved data. So the destroy is gated on three conditions together:
//  1. fullyReplicated(health) — necessary connectivity precondition,
//  2. minResyncSettle has elapsed since the drain — so block-resync has certainly been queued
//     (closes the race where resync has not started yet, so no worker is busy yet), and
//  3. no block-resync worker is active on any node — the real "data has moved" signal.
//
// All three fail closed: a Health or ListActiveWorkers error returns an error so the reconcile
// requeues rather than advancing to the destructive RecreatingVolume phase.
func (r *GarageClusterReconciler) migrationAwaitReplication(ctx context.Context, status *garagev1alpha1.GarageClusterStatus, layoutClient clusterAdmin, step *migrationStep) (bool, error) {
	health, err := layoutClient.Health(ctx)
	if err != nil {
		return false, err
	}
	if !fullyReplicated(health) {
		r.setMigrationStatus(status, step, garagev1alpha1.StorageMigrationAwaitingReplication,
			fmt.Sprintf("Waiting for re-replication after draining node %d (partitions all-ok %d/%d)", step.ordinal, health.PartitionsAllOk, health.Partitions))
		return true, nil
	}

	if at := status.StorageMigration.DrainedAt; at != nil && time.Since(at.Time) < minResyncSettle {
		r.setMigrationStatus(status, step, garagev1alpha1.StorageMigrationAwaitingReplication,
			fmt.Sprintf("Settling after draining node %d before destroying its volumes", step.ordinal))
		return true, nil
	}

	workers, err := layoutClient.ListActiveWorkers(ctx, maintenanceNode)
	if err != nil {
		return false, err
	}
	if resyncActive(workers) {
		r.setMigrationStatus(status, step, garagev1alpha1.StorageMigrationAwaitingReplication,
			fmt.Sprintf("Waiting for block resync to drain before destroying node %d volumes (%d worker(s) active)", step.ordinal, len(workers)))
		return true, nil
	}

	r.setMigrationStatus(status, step, garagev1alpha1.StorageMigrationRecreatingVolume,
		fmt.Sprintf("Recreating volumes for node %d", step.ordinal))
	return true, nil
}

// resyncActive reports whether any active worker is a block-resync worker — Garage's
// data-movement worker, named e.g. "block resync worker". ListActiveWorkers already filters to
// busy/throttled workers, so a returned resync worker means data is still being copied and the
// drained node's volume must not be destroyed yet. We match by name and scope strictly to resync:
// other active workers (scrub, compaction) do not move drained data, so conflating them would
// wrongly stall the migration.
func resyncActive(workers []garageadmin.WorkerSummary) bool {
	for _, w := range workers {
		if strings.Contains(strings.ToLower(w.Name), "resync") {
			return true
		}
	}
	return false
}

// migrationRecreateVolume recreates the node's volumes at the new spec. It first makes the
// StatefulSet's immutable volumeClaimTemplates reflect the new sizes (orphan-recreating the
// StatefulSet if not), then deletes the node's pod and PVCs so the StatefulSet recreates them
// fresh from the new template. The order — delete the claims first, then the pod — lets the
// pvc-protection finalizer hold the claims until the pod releases them, so the recreated pod
// cannot rebind the old volumes.
func (r *GarageClusterReconciler) migrationRecreateVolume(ctx context.Context, cluster *garagev1alpha1.GarageCluster, status *garagev1alpha1.GarageClusterStatus, step *migrationStep) (bool, error) {
	log := logf.FromContext(ctx)

	if !templateMatchesPool(step.ss, step.pool) {
		// The live template still provisions the old sizes; orphan-recreate so the next reconcile
		// rebuilds the StatefulSet with the new template (running pods/PVCs are kept).
		if err := r.orphanDeleteStatefulSet(ctx, step.ss); err != nil {
			return false, err
		}
		r.eventf(cluster, corev1.EventTypeNormal, "StorageMigrationRecreating",
			fmt.Sprintf("Recreating StatefulSet for pool %q with the new volume template", step.pool.Name))
		return true, nil
	}

	state, err := r.ordinalVolumeState(ctx, cluster, step.pool, step.ordinal)
	if err != nil {
		return false, err
	}
	switch state {
	case ordinalSwapped:
		// The fresh PVCs at the new size exist: the node is ready to rejoin.
		r.setMigrationStatus(status, step, garagev1alpha1.StorageMigrationAwaitingRejoin,
			fmt.Sprintf("Waiting for node %d to rejoin and refill", step.ordinal))
		return true, nil
	case ordinalReprovisioning:
		// The old PVCs are gone; the StatefulSet is recreating them. Wait without touching the
		// (recreated, still-Pending) pod.
		return true, nil
	default: // ordinalTearDown
		// Delete the old claims, then the pod that holds them; pvc-protection keeps the claims
		// until the pod releases them, so the recreated pod cannot rebind the old volumes. Both
		// deletes are idempotent, so re-entering here after a partial teardown simply retries.
		ssName := step.ss.Name
		if err := r.deleteClaims(ctx, cluster.Namespace, ssName, step.ordinal, step.ordinal+1); err != nil {
			return false, err
		}
		if err := r.deletePod(ctx, cluster.Namespace, podName(ssName, step.ordinal)); err != nil {
			return false, err
		}
		r.eventf(cluster, corev1.EventTypeNormal, "StorageMigrationRecreating",
			fmt.Sprintf("Recreating volumes for pool %q node %d at the new size", step.pool.Name, step.ordinal))
		log.Info("Recreating node volumes for storage migration", "pool", step.pool.Name, "ordinal", step.ordinal)
		return true, nil
	}
}

// migrationAwaitRejoin re-adds the recreated node to the layout and waits for Garage to refill
// it. The recreated node comes up with a fresh identity (its metadata volume was wiped), so the
// new id is read from the freshly-discovered nodes and assigned the node's new-capacity role.
// When the cluster is fully re-replicated again the node is done; clearing status.storageMigration
// lets the next reconcile pick the next node, or finish.
func (r *GarageClusterReconciler) migrationAwaitRejoin(ctx context.Context, cluster *garagev1alpha1.GarageCluster, status *garagev1alpha1.GarageClusterStatus, layoutClient clusterAdmin, desired []nodeEndpoint, step *migrationStep) (bool, error) {
	log := logf.FromContext(ctx)

	node, ok := nodeEndpointForOrdinal(desired, cluster, step.pool, step.ordinal)
	if !ok {
		// The recreated pod is not reachable yet; wait for it to come up with its new identity.
		return true, nil
	}

	ids, err := layoutClient.AppliedLayoutNodeIDs(ctx)
	if err != nil {
		return false, err
	}
	if !slices.Contains(ids, node.nodeID) {
		if err := layoutClient.AddNode(ctx, desiredRole(node)); err != nil {
			return false, err
		}
		r.eventf(cluster, corev1.EventTypeNormal, "StorageMigrationRejoining",
			fmt.Sprintf("Re-adding pool %q node %d to the layout to refill it", step.pool.Name, step.ordinal))
		log.Info("Re-adding node after storage migration", "pool", step.pool.Name, "ordinal", step.ordinal)
		r.setMigrationStatus(status, step, garagev1alpha1.StorageMigrationAwaitingRejoin,
			fmt.Sprintf("Refilling node %d after rejoining the layout", step.ordinal))
		return true, nil
	}

	health, err := layoutClient.Health(ctx)
	if err != nil {
		return false, err
	}
	if !fullyReplicated(health) {
		r.setMigrationStatus(status, step, garagev1alpha1.StorageMigrationAwaitingRejoin,
			fmt.Sprintf("Refilling node %d (partitions all-ok %d/%d)", step.ordinal, health.PartitionsAllOk, health.Partitions))
		return true, nil
	}

	r.eventf(cluster, corev1.EventTypeNormal, "StorageMigrationNodeComplete",
		fmt.Sprintf("Completed storage migration of pool %q node %d", step.pool.Name, step.ordinal))
	log.Info("Completed node storage migration", "pool", step.pool.Name, "ordinal", step.ordinal)
	// Clear progress; the next reconcile migrates the next node, or finds none and finishes.
	status.StorageMigration = nil
	return true, nil
}

// blockMigration records a guardrail refusal of a pool's storage migration: it sets
// StorageChangePending=False/Blocked, emits a Warning Event, and clears any in-progress record
// (nothing is mid-flight — the migration never started).
//
// It then pins the blocked pool's node capacities in desired to their current applied-layout value
// and returns active=false, so Reconcile falls through to the generic layout/zone/maintenance
// steps instead of freezing: only the affected pool's capacity change is suppressed, not every
// unrelated cluster operation (REVIEW.md #5). The pin is what makes unfreezing safe — without it
// reconcileLayout would advertise the pool's new (still-unmigrated) capacity to Garage, and a
// blocked grow would claim more disk than the node actually has, over-filling it. If the pool's
// nodes cannot all be pinned (no prior layout entry to pin to), it returns active=true instead,
// preserving the old safe-but-frozen behavior rather than risk advertising an unmigrated size.
func (r *GarageClusterReconciler) blockMigration(cluster *garagev1alpha1.GarageCluster, status *garagev1alpha1.GarageClusterStatus, desired []nodeEndpoint, pool *garagev1alpha1.NodePool, msg string) bool {
	setCondition(status, conditionStorageChangePending, metav1.ConditionFalse, reasonBlocked, msg)
	r.eventf(cluster, corev1.EventTypeWarning, "StorageChangeBlocked", msg)
	status.StorageMigration = nil
	return !pinBlockedPoolCapacityToCurrent(cluster, status, desired, pool)
}

// pinBlockedPoolCapacityToCurrent rewrites the blocked pool's node capacities in desired back to
// their current applied-layout value, so reconcileLayout does not advance an unmigrated pool's
// capacity (a blocked grow would otherwise advertise more disk than the node has, since its volume
// has not been recreated yet). It matches the pool's discovered nodes by pod-name prefix and reads
// the current value from status.Layout, which echoes the last applied layout. It returns true only
// if every one of the pool's discovered nodes had a current value to pin to; a node missing from
// status.Layout cannot be pinned, so the caller keeps the whole reconcile frozen as a fallback.
func pinBlockedPoolCapacityToCurrent(cluster *garagev1alpha1.GarageCluster, status *garagev1alpha1.GarageClusterStatus, desired []nodeEndpoint, pool *garagev1alpha1.NodePool) bool {
	if status.Layout == nil {
		return false
	}
	currentByPod := make(map[string]resource.Quantity, len(status.Layout.Nodes))
	for _, n := range status.Layout.Nodes {
		currentByPod[n.Pod] = n.Capacity
	}
	prefix := statefulSetName(cluster, pool) + "-"
	allPinned := true
	for i := range desired {
		if !strings.HasPrefix(desired[i].pod, prefix) {
			continue
		}
		if cur, ok := currentByPod[desired[i].pod]; ok {
			desired[i].capacity = cur
		} else {
			allPinned = false
		}
	}
	return allPinned
}

// setMigrationStatus stamps the current node/phase/message onto status. It rebuilds the whole
// block each pass, so DrainedAt — stamped once at the drain and read by the destroy gate — is
// carried over from the prior status when that status is for the same node (same pool and
// ordinal); otherwise it would be dropped on every reconcile and the settle window could never
// elapse.
func (r *GarageClusterReconciler) setMigrationStatus(status *garagev1alpha1.GarageClusterStatus, step *migrationStep, phase garagev1alpha1.StorageMigrationPhase, message string) {
	var drainedAt *metav1.Time
	if prev := status.StorageMigration; prev != nil && prev.Pool == step.pool.Name && prev.Ordinal == step.ordinal {
		drainedAt = prev.DrainedAt
	}
	status.StorageMigration = &garagev1alpha1.StorageMigrationStatus{
		Pool:      step.pool.Name,
		Ordinal:   step.ordinal,
		Phase:     phase,
		Message:   message,
		DrainedAt: drainedAt,
	}
}

// poolMigrationTarget returns the lowest ordinal in the pool whose volumes need a migration —
// a StorageClass change, a shrink, or a grow on a StorageClass that cannot expand — comparing
// each node's live PVCs against the desired pool spec. Expandable grows are excluded: those are
// the in-place path's job (and the StatefulSet is recreated only once Path A has patched every
// PVC). A node whose PVC is not yet provisioned is skipped rather than treated as a target.
//
// classifyVolume (garagecluster_storage.go) encodes the same in-place-vs-migration classification
// against the StatefulSet template rather than a node's live PVCs; keep the two in lockstep when
// this logic changes.
func (r *GarageClusterReconciler) poolMigrationTarget(ctx context.Context, cluster *garagev1alpha1.GarageCluster, pool *garagev1alpha1.NodePool, ss *appsv1.StatefulSet) (int32, bool, error) {
	volumes := poolVolumes(pool)
	for ordinal := int32(0); ordinal < replicaCount(ss); ordinal++ {
		for _, v := range volumes {
			pvc, err := r.claim(ctx, cluster.Namespace, ss.Name, v.name, ordinal)
			if err != nil {
				return 0, false, err
			}
			if pvc == nil {
				continue // not provisioned yet; cannot classify.
			}
			// A StorageClass change can never be served in place (storageClassName is immutable),
			// so it is a migration regardless of any size change.
			if !pvcClassMatches(pvc, v.spec.StorageClass) {
				return ordinal, true, nil
			}
			current := pvc.Spec.Resources.Requests[corev1.ResourceStorage]
			switch v.spec.Size.Cmp(current) {
			case 0:
				continue
			case -1:
				return ordinal, true, nil // shrink: always a migration.
			}
			// Grow: a migration only when the class cannot expand in place.
			expandable, scReason, err := r.storageClassExpandable(ctx, cluster.Namespace, ss.Name, v.name)
			if err != nil {
				return 0, false, err
			}
			if !expandable && scReason != "" {
				return ordinal, true, nil
			}
		}
	}
	return 0, false, nil
}

// unresolvableStorageClass returns a human-readable reason when a StorageClass the pool's volumes
// explicitly request cannot back a PVC — it is empty, or no such StorageClass exists — or "" when
// every requested class is resolvable. A nil class (the cluster default) is always allowed: the
// API server resolves it at bind time, so there is nothing for the operator to validate.
func (r *GarageClusterReconciler) unresolvableStorageClass(ctx context.Context, pool *garagev1alpha1.NodePool) (string, error) {
	for _, class := range []*string{pool.Storage.Data.StorageClass, pool.Storage.Meta.StorageClass} {
		if class == nil {
			continue
		}
		if *class == "" {
			return "a volume requests an empty StorageClass, which cannot provision a volume", nil
		}
		var sc storagev1.StorageClass
		if err := r.Get(ctx, client.ObjectKey{Name: *class}, &sc); err != nil {
			if apierrors.IsNotFound(err) {
				return fmt.Sprintf("StorageClass %q was not found", *class), nil
			}
			return "", err
		}
	}
	return "", nil
}

// ordinalVolumeState classifies a node's volumes during the recreate phase.
type ordinalVolumeState int

const (
	// ordinalTearDown — at least one old PVC is still present (at the wrong size or already
	// deleting), so the node's pod and PVCs must be (re-)deleted to clear the way.
	ordinalTearDown ordinalVolumeState = iota
	// ordinalReprovisioning — the old PVCs are gone and the StatefulSet is recreating them at
	// the new size; wait without touching the (recreated) pod.
	ordinalReprovisioning
	// ordinalSwapped — fresh PVCs at the new size exist; the node can rejoin.
	ordinalSwapped
)

// ordinalVolumeState inspects a node's data and meta PVCs against the desired pool sizes to
// drive the recreate phase idempotently. The distinction that matters is whether an *old* claim
// is still around: while a wrong-size or Terminating claim exists, its holder pod is the old one
// and must be deleted (deletePod is idempotent, so a delete that failed mid-teardown is retried).
// Once every old claim is gone the StatefulSet owns the recreation, and the pod must be left
// alone — deleting it then would loop-delete the fresh, still-Pending replacement.
func (r *GarageClusterReconciler) ordinalVolumeState(ctx context.Context, cluster *garagev1alpha1.GarageCluster, pool *garagev1alpha1.NodePool, ordinal int32) (ordinalVolumeState, error) {
	ssName := statefulSetName(cluster, pool)
	volumes := poolVolumes(pool)
	allMatch := true
	oldClaimPresent := false
	for _, v := range volumes {
		pvc, err := r.claim(ctx, cluster.Namespace, ssName, v.name, ordinal)
		if err != nil {
			return 0, err
		}
		switch {
		case pvc == nil:
			allMatch = false // deleted; the StatefulSet will reprovision it.
		case pvc.DeletionTimestamp != nil:
			allMatch = false
			oldClaimPresent = true // still draining; its holder pod must go.
		case v.spec.Size.Cmp(pvc.Spec.Resources.Requests[corev1.ResourceStorage]) != 0,
			!pvcClassMatches(pvc, v.spec.StorageClass):
			allMatch = false
			oldClaimPresent = true // old claim at the wrong size or StorageClass.
		}
	}
	switch {
	case allMatch:
		return ordinalSwapped, nil
	case oldClaimPresent:
		return ordinalTearDown, nil
	default:
		return ordinalReprovisioning, nil
	}
}

// claim returns the named per-pod PVC, or nil when it does not exist.
func (r *GarageClusterReconciler) claim(ctx context.Context, namespace, statefulSet, volume string, ordinal int32) (*corev1.PersistentVolumeClaim, error) {
	var pvc corev1.PersistentVolumeClaim
	key := client.ObjectKey{Name: claimName(volume, statefulSet, ordinal), Namespace: namespace}
	if err := r.Get(ctx, key, &pvc); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	return &pvc, nil
}

// deletePod deletes a single StatefulSet-managed pod; the StatefulSet recreates it. NotFound is
// ignored so the call is idempotent across requeues.
func (r *GarageClusterReconciler) deletePod(ctx context.Context, namespace, name string) error {
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace}}
	if err := r.Delete(ctx, pod); err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	return nil
}

// getStatefulSet returns the pool's StatefulSet, or nil when it does not exist yet.
func (r *GarageClusterReconciler) getStatefulSet(ctx context.Context, cluster *garagev1alpha1.GarageCluster, pool *garagev1alpha1.NodePool) (*appsv1.StatefulSet, error) {
	var ss appsv1.StatefulSet
	key := client.ObjectKey{Name: statefulSetName(cluster, pool), Namespace: cluster.Namespace}
	if err := r.Get(ctx, key, &ss); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	return &ss, nil
}

// templateMatchesPool reports whether the StatefulSet's volumeClaimTemplates already provision
// the pool's desired data and meta sizes and StorageClasses. The class comparison is exact (an
// omitted class is a nil template class) rather than the resolved-default comparison used against
// live PVCs, because the template mirrors the spec verbatim — the API server never rewrites it.
func templateMatchesPool(ss *appsv1.StatefulSet, pool *garagev1alpha1.NodePool) bool {
	data, okData := templateStorageRequest(ss, volumeNameData)
	mta, okMeta := templateStorageRequest(ss, volumeNameMeta)
	return okData && okMeta &&
		data.Cmp(pool.Storage.Data.Size) == 0 &&
		mta.Cmp(pool.Storage.Meta.Size) == 0 &&
		templateClassMatches(ss, volumeNameData, pool.Storage.Data.StorageClass) &&
		templateClassMatches(ss, volumeNameMeta, pool.Storage.Meta.StorageClass)
}

// templateClassMatches reports whether the StatefulSet's volumeClaimTemplate for the volume
// already requests the desired StorageClass, comparing the two *string values exactly (both nil,
// or both the same name).
func templateClassMatches(ss *appsv1.StatefulSet, volume string, desired *string) bool {
	tmpl := templateStorageClass(ss, volume)
	if (tmpl == nil) != (desired == nil) {
		return false
	}
	return tmpl == nil || *tmpl == *desired
}

// templateStorageClass returns the StorageClass the StatefulSet's volumeClaimTemplate for the
// named volume requests, or nil when the template has no such claim or requests no class.
func templateStorageClass(ss *appsv1.StatefulSet, volume string) *string {
	if vct := templateClaim(ss, volume); vct != nil {
		return vct.Spec.StorageClassName
	}
	return nil
}

// nodeEndpointForOrdinal finds the discovered node for a pool ordinal by its pod name.
func nodeEndpointForOrdinal(desired []nodeEndpoint, cluster *garagev1alpha1.GarageCluster, pool *garagev1alpha1.NodePool, ordinal int32) (nodeEndpoint, bool) {
	want := podName(statefulSetName(cluster, pool), ordinal)
	for _, n := range desired {
		if n.pod == want {
			return n, true
		}
	}
	return nodeEndpoint{}, false
}

// desiredRole builds the layout role to re-add a migrated node with, carrying its new
// (post-migration) capacity.
func desiredRole(n nodeEndpoint) garageadmin.DesiredRole {
	return garageadmin.DesiredRole{NodeID: n.nodeID, Zone: n.zone, Capacity: n.capacity.Value()}
}

// fullyReplicated reports whether every partition is connected to all its responsible storage
// nodes — the signal that a drain or refill has finished and the cluster is fully redundant.
func fullyReplicated(h *garageadmin.GetClusterHealthResponse) bool {
	return h.Partitions > 0 && h.PartitionsAllOk == h.Partitions
}

// migrationReadyMessage describes why the cluster is busy for the Ready condition while a
// migration is active: the per-node progress when one is in flight, or the guardrail's
// blocking reason when one is refused.
func migrationReadyMessage(status *garagev1alpha1.GarageClusterStatus) string {
	if m := status.StorageMigration; m != nil {
		if m.Message != "" {
			return m.Message
		}
		return fmt.Sprintf("Migrating pool %q node %d", m.Pool, m.Ordinal)
	}
	if cond := meta.FindStatusCondition(status.Conditions, conditionStorageChangePending); cond != nil {
		return cond.Message
	}
	return "Storage migration in progress"
}

func findPool(cluster *garagev1alpha1.GarageCluster, name string) *garagev1alpha1.NodePool {
	for i := range cluster.Spec.NodePools {
		if cluster.Spec.NodePools[i].Name == name {
			return &cluster.Spec.NodePools[i]
		}
	}
	return nil
}

func podName(statefulSet string, ordinal int32) string {
	return fmt.Sprintf("%s-%d", statefulSet, ordinal)
}
