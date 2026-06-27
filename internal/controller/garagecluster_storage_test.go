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
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	garagev1alpha1 "github.com/QuantumDancer/garage-operator/api/v1alpha1"
)

const expandableStorageClass = "expandable"

// storageFixture assembles the world reconcileStorage reads: a cluster whose spec carries the
// desired (post-edit) sizes, the live StatefulSet still at the old sizes, the per-pod PVCs the
// StatefulSet would have created, and the StorageClass backing them.
type storageFixture struct {
	cluster *garagev1alpha1.GarageCluster
	objs    []client.Object
}

// newStorageFixture builds a single-pool cluster whose spec asks for desiredData/desiredMeta,
// with a live StatefulSet (and its PVCs) provisioned at currentData (meta starts at 1Gi) on a
// StorageClass whose expansion support is set by allowExpansion.
func newStorageFixture(currentData, desiredData, desiredMeta string, replicas int32, allowExpansion bool) *storageFixture {
	const currentMeta = "1Gi"
	cluster := &garagev1alpha1.GarageCluster{
		ObjectMeta: metav1.ObjectMeta{Name: testClusterName, Namespace: testClusterNS},
		Spec: garagev1alpha1.GarageClusterSpec{
			NodePools: []garagev1alpha1.NodePool{{
				Name:     testPoolName,
				Replicas: replicas,
				Storage: garagev1alpha1.NodePoolStorage{
					Data: garagev1alpha1.StorageSpec{Size: resource.MustParse(desiredData)},
					Meta: garagev1alpha1.StorageSpec{Size: resource.MustParse(desiredMeta)},
				},
			}},
		},
	}

	// The live StatefulSet reflects the pre-edit sizes, so build it from a pool still at the
	// current sizes.
	livePool := cluster.Spec.NodePools[0]
	livePool.Storage = garagev1alpha1.NodePoolStorage{
		Data: garagev1alpha1.StorageSpec{Size: resource.MustParse(currentData)},
		Meta: garagev1alpha1.StorageSpec{Size: resource.MustParse(currentMeta)},
	}
	ss := desiredStatefulSet(cluster, &livePool)

	objs := make([]client.Object, 0, 2+2*replicas)
	objs = append(objs, ss, &storagev1.StorageClass{
		ObjectMeta:           metav1.ObjectMeta{Name: expandableStorageClass},
		AllowVolumeExpansion: ptr.To(allowExpansion),
	})
	for ordinal := range replicas {
		objs = append(objs,
			storageClaim(ss.Name, volumeNameData, ordinal, currentData),
			storageClaim(ss.Name, volumeNameMeta, ordinal, currentMeta),
		)
	}
	return &storageFixture{cluster: cluster, objs: objs}
}

func storageClaim(statefulSet, volume string, ordinal int32, size string) *corev1.PersistentVolumeClaim {
	return &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: claimName(volume, statefulSet, ordinal), Namespace: testClusterNS},
		Spec: corev1.PersistentVolumeClaimSpec{
			StorageClassName: ptr.To(expandableStorageClass),
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse(size)},
			},
		},
	}
}

func newStorageReconciler(t *testing.T, objs ...client.Object) (*GarageClusterReconciler, client.Client) {
	t.Helper()
	scheme := bucketTestScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()
	return &GarageClusterReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(100)}, c
}

func claimSize(t *testing.T, c client.Client, statefulSet, volume string, ordinal int32) resource.Quantity {
	t.Helper()
	var pvc corev1.PersistentVolumeClaim
	key := types.NamespacedName{Name: claimName(volume, statefulSet, ordinal), Namespace: testClusterNS}
	if err := c.Get(context.Background(), key, &pvc); err != nil {
		t.Fatalf("get claim %s: %v", key.Name, err)
	}
	return pvc.Spec.Resources.Requests[corev1.ResourceStorage]
}

func ssName() string {
	return testClusterName + "-" + testPoolName
}

func TestReconcileStorageGrowsDataAndMeta(t *testing.T) {
	fx := newStorageFixture("1Gi", "2Gi", "5Gi", 1, true)
	r, c := newStorageReconciler(t, fx.objs...)
	status := &garagev1alpha1.GarageClusterStatus{}

	recreate, err := r.reconcileStorage(context.Background(), fx.cluster, status)
	if err != nil {
		t.Fatalf("reconcileStorage: %v", err)
	}
	if !recreate {
		t.Fatal("expected recreate=true after a grow")
	}

	if got := claimSize(t, c, ssName(), volumeNameData, 0); got.Cmp(resource.MustParse("2Gi")) != 0 {
		t.Errorf("data claim = %s, want 2Gi", got.String())
	}
	if got := claimSize(t, c, ssName(), volumeNameMeta, 0); got.Cmp(resource.MustParse("5Gi")) != 0 {
		t.Errorf("meta claim = %s, want 5Gi", got.String())
	}

	// The StatefulSet must have been orphan-deleted so the next reconcile recreates it with the
	// larger volume template.
	var ss appsv1.StatefulSet
	err = c.Get(context.Background(), types.NamespacedName{Name: ssName(), Namespace: testClusterNS}, &ss)
	if !apierrors.IsNotFound(err) {
		t.Errorf("expected StatefulSet to be deleted, got err=%v", err)
	}

	if meta.FindStatusCondition(status.Conditions, conditionStorageChangePending) != nil {
		t.Error("expected no StorageChangePending condition on a clean grow")
	}
}

func TestReconcileStorageGrowsEveryReplicaClaim(t *testing.T) {
	fx := newStorageFixture("1Gi", "3Gi", "1Gi", 3, true)
	r, c := newStorageReconciler(t, fx.objs...)

	recreate, err := r.reconcileStorage(context.Background(), fx.cluster, &garagev1alpha1.GarageClusterStatus{})
	if err != nil {
		t.Fatalf("reconcileStorage: %v", err)
	}
	if !recreate {
		t.Fatal("expected recreate=true")
	}

	for ordinal := range int32(3) {
		if got := claimSize(t, c, ssName(), volumeNameData, ordinal); got.Cmp(resource.MustParse("3Gi")) != 0 {
			t.Errorf("data claim ordinal %d = %s, want 3Gi", ordinal, got.String())
		}
		// Meta was unchanged, so its claims must be left exactly as they were.
		if got := claimSize(t, c, ssName(), volumeNameMeta, ordinal); got.Cmp(resource.MustParse("1Gi")) != 0 {
			t.Errorf("meta claim ordinal %d = %s, want 1Gi (untouched)", ordinal, got.String())
		}
	}
}

func TestReconcileStorageBlocksShrink(t *testing.T) {
	// Spec asks for less than the live StatefulSet provisioned.
	fx := newStorageFixture("2Gi", "1Gi", "1Gi", 1, true)
	r, c := newStorageReconciler(t, fx.objs...)
	status := &garagev1alpha1.GarageClusterStatus{}

	recreate, err := r.reconcileStorage(context.Background(), fx.cluster, status)
	if err != nil {
		t.Fatalf("reconcileStorage: %v", err)
	}
	if recreate {
		t.Fatal("expected recreate=false for a refused shrink")
	}

	// The claim and StatefulSet are left untouched.
	if got := claimSize(t, c, ssName(), volumeNameData, 0); got.Cmp(resource.MustParse("2Gi")) != 0 {
		t.Errorf("data claim = %s, want 2Gi (unchanged)", got.String())
	}
	var ss appsv1.StatefulSet
	if err := c.Get(context.Background(), types.NamespacedName{Name: ssName(), Namespace: testClusterNS}, &ss); err != nil {
		t.Errorf("StatefulSet should still exist: %v", err)
	}

	cond := meta.FindStatusCondition(status.Conditions, conditionStorageChangePending)
	if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != "Blocked" {
		t.Fatalf("expected StorageChangePending=False/Blocked, got %+v", cond)
	}
}

func TestReconcileStorageBlocksNonExpandableClass(t *testing.T) {
	fx := newStorageFixture("1Gi", "2Gi", "1Gi", 1, false)
	r, c := newStorageReconciler(t, fx.objs...)
	status := &garagev1alpha1.GarageClusterStatus{}

	recreate, err := r.reconcileStorage(context.Background(), fx.cluster, status)
	if err != nil {
		t.Fatalf("reconcileStorage: %v", err)
	}
	if recreate {
		t.Fatal("expected recreate=false when the StorageClass forbids expansion")
	}
	if got := claimSize(t, c, ssName(), volumeNameData, 0); got.Cmp(resource.MustParse("1Gi")) != 0 {
		t.Errorf("data claim = %s, want 1Gi (unchanged)", got.String())
	}
	cond := meta.FindStatusCondition(status.Conditions, conditionStorageChangePending)
	if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != "Blocked" {
		t.Fatalf("expected StorageChangePending=False/Blocked, got %+v", cond)
	}
}

func TestReconcileStorageNoopWhenSizesMatch(t *testing.T) {
	fx := newStorageFixture("1Gi", "1Gi", "1Gi", 1, true)
	r, c := newStorageReconciler(t, fx.objs...)
	// Seed a stale Blocked condition to prove a converged reconcile clears it.
	status := &garagev1alpha1.GarageClusterStatus{}
	setCondition(status, conditionStorageChangePending, metav1.ConditionFalse, "Blocked", "stale")

	recreate, err := r.reconcileStorage(context.Background(), fx.cluster, status)
	if err != nil {
		t.Fatalf("reconcileStorage: %v", err)
	}
	if recreate {
		t.Fatal("expected recreate=false when sizes already match")
	}
	if got := claimSize(t, c, ssName(), volumeNameData, 0); got.Cmp(resource.MustParse("1Gi")) != 0 {
		t.Errorf("data claim = %s, want 1Gi (unchanged)", got.String())
	}
	if meta.FindStatusCondition(status.Conditions, conditionStorageChangePending) != nil {
		t.Error("expected the stale StorageChangePending condition to be cleared")
	}
}

// TestReconcileStorageDefersGrowWhileScaleDownPending guards the dangerous interaction where a
// grow coincides with a not-yet-drained scale-down: orphan-recreating the StatefulSet would
// bring it back at the reduced replica count, deleting undrained nodes and bypassing the gated
// drain. The grow must defer until the live and desired replica counts agree.
func TestReconcileStorageDefersGrowWhileScaleDownPending(t *testing.T) {
	// Live StatefulSet runs 3 replicas; the user grows data and scales the pool down to 2.
	fx := newStorageFixture("1Gi", "2Gi", "1Gi", 3, true)
	fx.cluster.Spec.NodePools[0].Replicas = 2
	r, c := newStorageReconciler(t, fx.objs...)

	recreate, err := r.reconcileStorage(context.Background(), fx.cluster, &garagev1alpha1.GarageClusterStatus{})
	if err != nil {
		t.Fatalf("reconcileStorage: %v", err)
	}
	if recreate {
		t.Fatal("expected recreate=false: the grow must defer to the gated drain")
	}

	var ss appsv1.StatefulSet
	if err := c.Get(context.Background(), types.NamespacedName{Name: ssName(), Namespace: testClusterNS}, &ss); err != nil {
		t.Fatalf("StatefulSet must not be orphan-deleted: %v", err)
	}
	if got := claimSize(t, c, ssName(), volumeNameData, 0); got.Cmp(resource.MustParse("1Gi")) != 0 {
		t.Errorf("data claim = %s, want 1Gi: PVCs must not grow while a scale-down is pending", got.String())
	}
}

// TestReconcileStorageSkipsTerminatingStatefulSet covers the requeue window after an orphan
// delete: while the old StatefulSet lingers in Terminating (awaiting GC), the grow must not be
// re-issued, so the delete and StorageExpanded event fire only once.
func TestReconcileStorageSkipsTerminatingStatefulSet(t *testing.T) {
	fx := newStorageFixture("1Gi", "2Gi", "1Gi", 1, true)
	ss := fx.objs[0].(*appsv1.StatefulSet)
	now := metav1.Now()
	ss.DeletionTimestamp = &now
	// The fake client requires a finalizer on an object carrying a deletionTimestamp.
	ss.Finalizers = []string{"kubernetes.io/orphan"}
	r, c := newStorageReconciler(t, fx.objs...)

	recreate, err := r.reconcileStorage(context.Background(), fx.cluster, &garagev1alpha1.GarageClusterStatus{})
	if err != nil {
		t.Fatalf("reconcileStorage: %v", err)
	}
	if recreate {
		t.Fatal("expected recreate=false for a StatefulSet already being deleted")
	}
	if got := claimSize(t, c, ssName(), volumeNameData, 0); got.Cmp(resource.MustParse("1Gi")) != 0 {
		t.Errorf("data claim = %s, want 1Gi: a terminating StatefulSet must not be re-grown", got.String())
	}
}

// TestReconcileStorageNoStatefulSetYet covers a freshly-created cluster whose StatefulSet has
// not been provisioned: there is nothing to grow and the call is a no-op.
func TestReconcileStorageNoStatefulSetYet(t *testing.T) {
	fx := newStorageFixture("1Gi", "2Gi", "1Gi", 1, true)
	// Drop the StatefulSet (and its claims) from the world, keeping only the cluster + class.
	r, _ := newStorageReconciler(t, fx.cluster, &storagev1.StorageClass{
		ObjectMeta:           metav1.ObjectMeta{Name: expandableStorageClass},
		AllowVolumeExpansion: ptr.To(true),
	})
	recreate, err := r.reconcileStorage(context.Background(), fx.cluster, &garagev1alpha1.GarageClusterStatus{})
	if err != nil {
		t.Fatalf("reconcileStorage: %v", err)
	}
	if recreate {
		t.Fatal("expected recreate=false when no StatefulSet exists yet")
	}
}
