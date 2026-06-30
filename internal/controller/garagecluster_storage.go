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

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	garagev1alpha1 "github.com/QuantumDancer/garage-operator/api/v1alpha1"
)

// conditionStorageChangePending is set (False, reason Blocked) when a storage migration is
// refused by a safety guardrail (replicationFactor < 2, or an unhealthy cluster). The
// in-place grow and the migration's happy path set no condition: they apply and report only
// via Events, mirroring how an additive layout change is silent on success. The migration
// step (PLAN.md §4.5) owns this condition; reconcileStorage (the in-place path) never sets it.
const conditionStorageChangePending = "StorageChangePending"

// reasonBlocked is the condition reason shared by every guardrail that refuses a destructive
// layout or storage change (replicationFactor, health, an unresolvable StorageClass).
const reasonBlocked = "Blocked"

// storageAction classifies how a volume's requested size change must be served.
type storageAction int

const (
	// storageNoChange — the live volume already matches the desired size.
	storageNoChange storageAction = iota
	// storageGrowInPlace — the size increased on an expansion-capable StorageClass, so the
	// existing PVCs can be patched larger (Path A, PLAN.md §5.4).
	storageGrowInPlace
	// storageMigrate — a shrink, a grow the StorageClass cannot expand, or a StorageClass change
	// (whose immutable storageClassName forbids an in-place edit), so the volume must be recreated
	// through the migration path (Path B, PLAN.md §4.5).
	storageMigrate
)

// poolVolume pairs one of a pool's managed volumes with its storage spec.
type poolVolume struct {
	name string
	spec garagev1alpha1.StorageSpec
}

// poolVolumes returns a pool's managed volumes (data then meta) in the fixed order every storage
// path iterates them, so the data/meta pair lives in exactly one place.
func poolVolumes(pool *garagev1alpha1.NodePool) []poolVolume {
	return []poolVolume{
		{volumeNameData, pool.Storage.Data},
		{volumeNameMeta, pool.Storage.Meta},
	}
}

// reconcileStorage grows the data/meta volumes of each pool in place when their spec sizes
// have increased on an expansion-capable StorageClass (Path A, PLAN.md §5.4). Because a
// StatefulSet's volumeClaimTemplates are immutable, growing a volume is a two-part operation
// performed together:
//
//  1. patch every existing per-pod PVC up to the new size (CSI then expands the volume), and
//  2. delete the StatefulSet with an orphan cascade so the next reconcile recreates it with the
//     larger template — keeping the running pods and PVCs untouched.
//
// It returns recreate=true once it has orphan-deleted at least one StatefulSet, so the caller
// requeues and lets ensureWorkload recreate it cleanly rather than racing the deletion.
//
// A change the in-place path cannot serve — a shrink, or a grow on a StorageClass without
// allowVolumeExpansion — is left untouched here and handled by the migration step
// (reconcileStorageMigration), which has Admin API access to drain the node first. The
// operator never blocks on the CSI resize completing (PVC.status.capacity): expansion is
// assumed online, so reporting the patched request and recreated template is the whole of the
// in-place contract here.
func (r *GarageClusterReconciler) reconcileStorage(ctx context.Context, cluster *garagev1alpha1.GarageCluster, _ *garagev1alpha1.GarageClusterStatus) (recreate bool, err error) {
	log := logf.FromContext(ctx)

	for i := range cluster.Spec.NodePools {
		pool := &cluster.Spec.NodePools[i]
		var ss appsv1.StatefulSet
		key := client.ObjectKey{Name: statefulSetName(cluster, pool), Namespace: cluster.Namespace}
		if getErr := r.Get(ctx, key, &ss); getErr != nil {
			// No StatefulSet yet: a fresh cluster is sized correctly at creation, so there is
			// nothing to grow. A transient read error is surfaced for retry.
			if apierrors.IsNotFound(getErr) {
				continue
			}
			return false, getErr
		}

		// Already orphan-deleted on a prior pass and not yet garbage-collected: leave it to be
		// recreated once it is gone, rather than re-issuing the delete (and re-emitting the
		// StorageExpanded event) on every requeue while the old object lingers in Terminating.
		if ss.DeletionTimestamp != nil {
			continue
		}

		// Defer the grow while a gated scale-down is pending for this pool. A live StatefulSet
		// running more replicas than desired means ensureStatefulSet refused to scale it down
		// because the surplus nodes have not been drained from the layout yet. Orphan-deleting
		// now would recreate the StatefulSet at the reduced replica count (desiredStatefulSet
		// uses pool.Replicas), deleting those pods without a drain and bypassing the
		// reconcileDestructiveLayout guardrail — silent data loss. Let the gated drain reduce the
		// pool first; the grow then proceeds once the live and desired counts agree.
		if replicaCount(&ss) > pool.Replicas {
			log.Info("Deferring storage grow until a pending scale-down is drained", "pool", pool.Name)
			continue
		}

		grew, migrate, growErr := r.planPoolStorage(ctx, cluster, pool, &ss)
		if growErr != nil {
			return false, growErr
		}
		if migrate {
			// The migration step owns this pool: leave the volumes entirely untouched so a
			// contradictory edit (grow one volume, shrink the other) never half-applies in place
			// before the migration recreates both from the new template.
			continue
		}
		if !grew {
			continue
		}

		if delErr := r.orphanDeleteStatefulSet(ctx, &ss); delErr != nil {
			return false, delErr
		}
		r.eventf(cluster, corev1.EventTypeNormal, "StorageExpanded",
			fmt.Sprintf("Expanded storage for pool %q; recreating StatefulSet with the larger volume template", pool.Name))
		log.Info("Expanded pool storage in place", "pool", pool.Name)
		recreate = true
	}

	return recreate, nil
}

// planPoolStorage inspects the pool's data and meta volumes against the live StatefulSet and,
// for any that grew in place, patches the existing PVCs up. It returns grew=true when at least
// one volume was expanded (so the StatefulSet must be recreated), or migrate=true when any
// volume's change must instead go through the migration path (a shrink, or a grow the
// StorageClass cannot expand). migrate routes the *whole* pool to migration: its volumes are
// left untouched here so the migration recreates every PVC from the new template uniformly.
func (r *GarageClusterReconciler) planPoolStorage(ctx context.Context, cluster *garagev1alpha1.GarageCluster, pool *garagev1alpha1.NodePool, ss *appsv1.StatefulSet) (grew bool, migrate bool, err error) {
	volumes := poolVolumes(pool)

	// Classify every volume before mutating anything, so routing the pool to migration leaves
	// the other volume unpatched.
	type growth struct {
		name    string
		desired resource.Quantity
	}
	var grows []growth
	for _, v := range volumes {
		action, ok, classifyErr := r.classifyVolume(ctx, cluster.Namespace, ss, v.name, v.spec)
		if classifyErr != nil {
			return false, false, classifyErr
		}
		if !ok {
			// Not yet classifiable (the volume's PVC is not provisioned yet): defer the whole
			// pool rather than grow only the other volume and recreate against a template this
			// volume's PVC has not caught up to. The next reconcile retries once the claim appears.
			return false, false, nil
		}
		switch action {
		case storageMigrate:
			return false, true, nil
		case storageGrowInPlace:
			grows = append(grows, growth{name: v.name, desired: v.spec.Size})
		}
	}

	for _, g := range grows {
		if err := r.expandClaims(ctx, cluster.Namespace, ss.Name, g.name, replicaCount(ss), g.desired); err != nil {
			return false, false, err
		}
		grew = true
	}
	return grew, false, nil
}

// classifyVolume decides how a single volume's change is served. A StorageClass change is checked
// first: a PVC's storageClassName is immutable, so an explicitly-requested class that differs from
// what the live volume was provisioned with always routes to migration, whatever the size does.
// Otherwise the desired size is compared against what the live StatefulSet template provisions: a
// grow is served in place only when the backing StorageClass allows expansion; a shrink, or a grow
// the class cannot expand, goes through the migration path. The bool return is false when
// classification is not yet possible (the claim is not provisioned), so the caller defers.
//
// poolMigrationTarget (garagecluster_storage_migration.go) encodes the same in-place-vs-migration
// classification against a node's live PVCs rather than the StatefulSet template; keep the two in
// lockstep when this logic changes.
func (r *GarageClusterReconciler) classifyVolume(ctx context.Context, namespace string, ss *appsv1.StatefulSet, volume string, spec garagev1alpha1.StorageSpec) (storageAction, bool, error) {
	drifted, ok, err := r.classMigrationNeeded(ctx, namespace, ss.Name, volume, spec.StorageClass)
	if err != nil {
		return storageNoChange, false, err
	}
	if !ok {
		// An explicit class was requested but the ordinal-0 PVC is not provisioned yet, so the live
		// class is unknown. Defer rather than guess.
		return storageNoChange, false, nil
	}
	if drifted {
		return storageMigrate, true, nil
	}

	current, ok := templateStorageRequest(ss, volume)
	if !ok {
		// The template should always carry both claims; if not, the StatefulSet predates this
		// operator's invariants — leave it alone rather than guess.
		return storageNoChange, true, nil
	}
	switch spec.Size.Cmp(current) {
	case 0:
		return storageNoChange, true, nil
	case -1:
		// A PVC can never shrink in place, so any shrink is a migration regardless of the class.
		return storageMigrate, true, nil
	}
	expandable, scReason, err := r.storageClassExpandable(ctx, namespace, ss.Name, volume)
	if err != nil {
		return storageNoChange, false, err
	}
	if expandable {
		return storageGrowInPlace, true, nil
	}
	if scReason == "" {
		// Transient: the ordinal-0 claim is not provisioned yet, so expansion support cannot be
		// determined. Defer rather than misclassify.
		return storageNoChange, false, nil
	}
	// The class cannot expand the volume, so even a grow must recreate it through migration.
	return storageMigrate, true, nil
}

// classMigrationNeeded reports whether the volume's explicitly-requested StorageClass differs
// from the class its live ordinal-0 PVC was provisioned with — a change a StatefulSet's immutable
// volumeClaimTemplates can only serve by recreating the volume through the migration path. The
// bool ok is false only when an explicit class is requested but the ordinal-0 PVC is not yet
// provisioned (so the live class is unknown); the caller should defer. A nil desired (the cluster
// default) never drifts and is always comparable: the operator deliberately does not diff a moving
// default (PLAN.md §4.5), so removing the storageClass field is a no-op until another change drives
// a migration.
func (r *GarageClusterReconciler) classMigrationNeeded(ctx context.Context, namespace, statefulSet, volume string, desired *string) (drifted bool, ok bool, err error) {
	if desired == nil {
		return false, true, nil
	}
	pvc, err := r.claim(ctx, namespace, statefulSet, volume, 0)
	if err != nil {
		return false, false, err
	}
	if pvc == nil {
		return false, false, nil
	}
	return !pvcClassMatches(pvc, desired), true, nil
}

// pvcClassMatches reports whether a live PVC already carries the explicitly-requested StorageClass.
// A nil desired (the cluster default) matches unconditionally, and an indeterminate live class ("",
// a classless volume) is treated as a match — the operator never migrates a classless volume on a
// guess. The live class is the class the API server resolved at provisioning time (it records the
// resolved default on the PVC), so pinning the field to the value that was already the default does
// not read as drift.
func pvcClassMatches(pvc *corev1.PersistentVolumeClaim, desired *string) bool {
	if desired == nil {
		return true
	}
	live := ""
	if pvc.Spec.StorageClassName != nil {
		live = *pvc.Spec.StorageClassName
	}
	if live == "" {
		return true
	}
	return live == *desired
}

// templateClaim returns the StatefulSet's volumeClaimTemplate for the named volume, or nil when the
// template has no such claim. It is the single find-by-name traversal the size and class readers
// (templateStorageRequest, templateStorageClass) share.
func templateClaim(ss *appsv1.StatefulSet, volume string) *corev1.PersistentVolumeClaim {
	for i := range ss.Spec.VolumeClaimTemplates {
		if ss.Spec.VolumeClaimTemplates[i].Name == volume {
			return &ss.Spec.VolumeClaimTemplates[i]
		}
	}
	return nil
}

// templateStorageRequest returns the storage request the StatefulSet's volumeClaimTemplate for
// the named volume currently asks for. The second return is false when the template has no such
// claim or no storage request.
func templateStorageRequest(ss *appsv1.StatefulSet, volume string) (resource.Quantity, bool) {
	vct := templateClaim(ss, volume)
	if vct == nil {
		return resource.Quantity{}, false
	}
	q, ok := vct.Spec.Resources.Requests[corev1.ResourceStorage]
	return q, ok
}

// storageClassExpandable reports whether the StorageClass backing a volume's PVCs permits
// expansion. It reads the class name from the live ordinal-0 PVC (the API server records the
// resolved default there), since that is the class the volumes were actually provisioned with.
// When expansion is refused it returns a non-empty, human-readable reason for the blocking
// condition. A return of (false, "", nil) means "not yet determinable" — a transient state the
// caller should defer on, not surface as a refusal.
func (r *GarageClusterReconciler) storageClassExpandable(ctx context.Context, namespace, statefulSet, volume string) (bool, string, error) {
	var pvc corev1.PersistentVolumeClaim
	key := client.ObjectKey{Name: claimName(volume, statefulSet, 0), Namespace: namespace}
	if err := r.Get(ctx, key, &pvc); err != nil {
		if apierrors.IsNotFound(err) {
			// The StatefulSet exists but its first claim does not yet — too early to act. Signal a
			// transient deferral (empty reason) so the next reconcile retries once it appears.
			return false, "", nil
		}
		return false, "", err
	}

	scName := ""
	if pvc.Spec.StorageClassName != nil {
		scName = *pvc.Spec.StorageClassName
	}
	if scName == "" {
		return false, "the volume has no StorageClass, so expansion support cannot be determined", nil
	}

	var sc storagev1.StorageClass
	if err := r.Get(ctx, client.ObjectKey{Name: scName}, &sc); err != nil {
		if apierrors.IsNotFound(err) {
			return false, fmt.Sprintf("StorageClass %q was not found", scName), nil
		}
		return false, "", err
	}
	if sc.AllowVolumeExpansion == nil || !*sc.AllowVolumeExpansion {
		return false, fmt.Sprintf("StorageClass %q does not allow volume expansion", scName), nil
	}
	return true, "", nil
}

// expandClaims patches every per-pod PVC of a volume up to the desired size. A claim already at
// or above the target is left untouched, so the call is idempotent and safe to repeat after a
// partial failure.
func (r *GarageClusterReconciler) expandClaims(ctx context.Context, namespace, statefulSet, volume string, replicas int32, desired resource.Quantity) error {
	for ordinal := range replicas {
		var pvc corev1.PersistentVolumeClaim
		key := client.ObjectKey{Name: claimName(volume, statefulSet, ordinal), Namespace: namespace}
		if err := r.Get(ctx, key, &pvc); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return err
		}
		current := pvc.Spec.Resources.Requests[corev1.ResourceStorage]
		if desired.Cmp(current) <= 0 {
			continue
		}
		if pvc.Spec.Resources.Requests == nil {
			pvc.Spec.Resources.Requests = corev1.ResourceList{}
		}
		pvc.Spec.Resources.Requests[corev1.ResourceStorage] = desired
		if err := r.Update(ctx, &pvc); err != nil {
			return err
		}
	}
	return nil
}

// orphanDeleteStatefulSet deletes the StatefulSet object while leaving its pods and PVCs in
// place (orphan cascade). The next reconcile recreates it from the now-grown spec, and the
// recreated StatefulSet re-adopts the running pods; since only the immutable volumeClaimTemplates
// differ, the pod template is unchanged and the pods are not restarted.
func (r *GarageClusterReconciler) orphanDeleteStatefulSet(ctx context.Context, ss *appsv1.StatefulSet) error {
	if err := r.Delete(ctx, ss, client.PropagationPolicy(metav1.DeletePropagationOrphan)); err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	return nil
}

// claimName builds the PVC name Kubernetes derives for a StatefulSet's volumeClaimTemplate:
// "<volume>-<statefulSet>-<ordinal>".
func claimName(volume, statefulSet string, ordinal int32) string {
	return fmt.Sprintf("%s-%s-%d", volume, statefulSet, ordinal)
}

// replicaCount returns the StatefulSet's current replica count, treating a nil pointer as zero.
func replicaCount(ss *appsv1.StatefulSet) int32 {
	if ss.Spec.Replicas == nil {
		return 0
	}
	return *ss.Spec.Replicas
}
