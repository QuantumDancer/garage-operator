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
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	garagev1alpha1 "github.com/QuantumDancer/garage-operator/api/v1alpha1"
)

// conditionStorageChangePending is set (False, reason Blocked) when a requested storage change
// cannot be carried out in place — a size shrink, or a StorageClass that forbids expansion.
// The happy-path grow sets no condition: per the design it applies immediately and is reported
// only via an Event, mirroring how an additive layout change is silent on success.
const conditionStorageChangePending = "StorageChangePending"

// reconcileStorage grows the data/meta volumes of each pool in place when their spec sizes
// have increased (Path A, PLAN.md §5.4). Because a StatefulSet's volumeClaimTemplates are
// immutable, growing a volume is a two-part operation that this function performs together:
//
//  1. patch every existing per-pod PVC up to the new size (CSI then expands the volume), and
//  2. delete the StatefulSet with an orphan cascade so the next reconcile recreates it with the
//     larger template — keeping the running pods and PVCs untouched.
//
// It returns recreate=true once it has orphan-deleted at least one StatefulSet, so the caller
// requeues and lets ensureWorkload recreate it cleanly rather than racing the deletion.
//
// A shrink, or a StorageClass without allowVolumeExpansion, cannot be served in place: the
// volume is left as-is and a StorageChangePending=False/Blocked condition + Event explain why
// (a shrink is routed to the future migration path, PLAN.md §4.5). The operator never blocks on
// the CSI resize completing (PVC.status.capacity): expansion is assumed online, so reporting the
// patched request and recreated template is the whole of the operator's contract here.
func (r *GarageClusterReconciler) reconcileStorage(ctx context.Context, cluster *garagev1alpha1.GarageCluster, status *garagev1alpha1.GarageClusterStatus) (recreate bool, err error) {
	log := logf.FromContext(ctx)
	var blocked []string

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

		grew, reason, growErr := r.planPoolStorage(ctx, cluster, pool, &ss)
		if growErr != nil {
			return false, growErr
		}
		if reason != "" {
			blocked = append(blocked, reason)
			// A blocked pool is left entirely untouched — neither patched nor recreated — so a
			// contradictory edit (e.g. grow one volume, shrink the other) never applies half of
			// itself. The user resolves the block first.
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

	if len(blocked) > 0 {
		msg := blocked[0]
		if len(blocked) > 1 {
			msg = fmt.Sprintf("%s (and %d more)", blocked[0], len(blocked)-1)
		}
		setCondition(status, conditionStorageChangePending, metav1.ConditionFalse, "Blocked", msg)
		r.eventf(cluster, corev1.EventTypeWarning, "StorageChangeBlocked", msg)
	} else {
		meta.RemoveStatusCondition(&status.Conditions, conditionStorageChangePending)
	}
	return recreate, nil
}

// planPoolStorage inspects the pool's data and meta volumes against the live StatefulSet and,
// for any that grew, patches the existing PVCs up. It returns grew=true when at least one
// volume was expanded (so the StatefulSet must be recreated), or a non-empty reason when the
// change is refused (shrink, or a StorageClass that forbids expansion).
func (r *GarageClusterReconciler) planPoolStorage(ctx context.Context, cluster *garagev1alpha1.GarageCluster, pool *garagev1alpha1.NodePool, ss *appsv1.StatefulSet) (grew bool, reason string, err error) {
	volumes := []struct {
		name string
		spec garagev1alpha1.StorageSpec
	}{
		{volumeNameData, pool.Storage.Data},
		{volumeNameMeta, pool.Storage.Meta},
	}

	// Classify every volume before mutating anything, so a refusal on one volume blocks the
	// whole pool without having already patched the other.
	type growth struct {
		name    string
		desired resource.Quantity
	}
	var grows []growth
	for _, v := range volumes {
		current, ok := templateStorageRequest(ss, v.name)
		if !ok {
			// The template should always carry both claims; if not, the StatefulSet predates this
			// operator's invariants — leave it alone rather than guess.
			continue
		}
		switch v.spec.Size.Cmp(current) {
		case 0:
			continue
		case -1:
			return false, fmt.Sprintf("pool %q %s volume cannot shrink from %s to %s in place; use a storage migration",
				pool.Name, v.name, current.String(), v.spec.Size.String()), nil
		}
		if expandable, scReason, scErr := r.storageClassExpandable(ctx, cluster.Namespace, ss.Name, v.name); scErr != nil {
			return false, "", scErr
		} else if !expandable {
			return false, fmt.Sprintf("pool %q %s volume cannot grow: %s", pool.Name, v.name, scReason), nil
		}
		grows = append(grows, growth{name: v.name, desired: v.spec.Size})
	}

	for _, g := range grows {
		if err := r.expandClaims(ctx, cluster.Namespace, ss.Name, g.name, replicaCount(ss), g.desired); err != nil {
			return false, "", err
		}
		grew = true
	}
	return grew, "", nil
}

// templateStorageRequest returns the storage request the StatefulSet's volumeClaimTemplate for
// the named volume currently asks for. The second return is false when the template has no such
// claim or no storage request.
func templateStorageRequest(ss *appsv1.StatefulSet, volume string) (resource.Quantity, bool) {
	for i := range ss.Spec.VolumeClaimTemplates {
		vct := &ss.Spec.VolumeClaimTemplates[i]
		if vct.Name != volume {
			continue
		}
		q, ok := vct.Spec.Resources.Requests[corev1.ResourceStorage]
		return q, ok
	}
	return resource.Quantity{}, false
}

// storageClassExpandable reports whether the StorageClass backing a volume's PVCs permits
// expansion. It reads the class name from the live ordinal-0 PVC (the API server records the
// resolved default there), since that is the class the volumes were actually provisioned with.
// When expansion is not allowed it returns a human-readable reason for the blocking condition.
func (r *GarageClusterReconciler) storageClassExpandable(ctx context.Context, namespace, statefulSet, volume string) (bool, string, error) {
	var pvc corev1.PersistentVolumeClaim
	key := client.ObjectKey{Name: claimName(volume, statefulSet, 0), Namespace: namespace}
	if err := r.Get(ctx, key, &pvc); err != nil {
		if apierrors.IsNotFound(err) {
			// The StatefulSet exists but its first claim does not yet — too early to act. Report
			// not-expandable with a transient reason; the next reconcile retries once it appears.
			return false, "its PersistentVolumeClaim is not provisioned yet", nil
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
