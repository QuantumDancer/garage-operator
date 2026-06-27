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
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	garagev1alpha1 "github.com/QuantumDancer/garage-operator/api/v1alpha1"
)

// This envtest spec proves the storage-growth path is wired into Reconcile against a real API
// server: editing a pool's data size grows the existing PVCs and recreates the StatefulSet with
// the larger volume template. envtest has no StatefulSet controller, so the spec provisions the
// per-pod PVCs by hand to stand in for the ones a real cluster's controller would create.
var _ = Describe("GarageCluster storage growth integration", Ordered, func() {
	const (
		itNamespace = "garage-cluster-storage-it"
		clusterName = "growcluster"
		scName      = "growable"
	)

	reconciler := func() *GarageClusterReconciler {
		return &GarageClusterReconciler{
			Client: k8sClient,
			Scheme: k8sClient.Scheme(),
			NewAdminClient: func(string, string) (clusterAdmin, error) {
				return &fakeClusterAdmin{nodeID: nodeSelf, recorder: &meshRecorder{}, layout: newFakeLayout()}, nil
			},
			Recorder: record.NewFakeRecorder(100),
		}
	}

	clusterKey := client.ObjectKey{Name: clusterName, Namespace: itNamespace}
	ssKey := client.ObjectKey{Name: clusterName + "-default", Namespace: itNamespace}
	dataClaim := client.ObjectKey{Name: claimName(volumeNameData, ssKey.Name, 0), Namespace: itNamespace}
	metaClaim := client.ObjectKey{Name: claimName(volumeNameMeta, ssKey.Name, 0), Namespace: itNamespace}

	expectClaimSize := func(key client.ObjectKey, want string) {
		var pvc corev1.PersistentVolumeClaim
		Expect(k8sClient.Get(ctx, key, &pvc)).To(Succeed())
		got := pvc.Spec.Resources.Requests[corev1.ResourceStorage]
		Expect(got.Cmp(resource.MustParse(want))).To(Equal(0), "claim %s = %s, want %s", key.Name, got.String(), want)
	}

	BeforeAll(func() {
		Expect(k8sClient.Create(ctx, &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{Name: itNamespace},
		})).To(Succeed())

		Expect(k8sClient.Create(ctx, &storagev1.StorageClass{
			ObjectMeta:           metav1.ObjectMeta{Name: scName},
			Provisioner:          "kubernetes.io/no-provisioner",
			AllowVolumeExpansion: ptr.To(true),
		})).To(Succeed())

		cluster := &garagev1alpha1.GarageCluster{
			ObjectMeta: metav1.ObjectMeta{Name: clusterName, Namespace: itNamespace},
			Spec:       newBasicClusterSpec(1, 1),
		}
		cluster.Spec.NodePools[0].Storage.Data.StorageClass = ptr.To(scName)
		cluster.Spec.NodePools[0].Storage.Meta.StorageClass = ptr.To(scName)
		Expect(k8sClient.Create(ctx, cluster)).To(Succeed())
	})

	AfterAll(func() {
		cluster := &garagev1alpha1.GarageCluster{ObjectMeta: metav1.ObjectMeta{Name: clusterName, Namespace: itNamespace}}
		Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, cluster))).To(Succeed())
	})

	It("provisions the StatefulSet at the initial size", func() {
		_, err := reconciler().Reconcile(ctx, reconcile.Request{NamespacedName: clusterKey})
		Expect(err).NotTo(HaveOccurred())

		var ss appsv1.StatefulSet
		Expect(k8sClient.Get(ctx, ssKey, &ss)).To(Succeed())
		got, _ := templateStorageRequest(&ss, volumeNameData)
		Expect(got.Cmp(resource.MustParse("1Gi"))).To(Equal(0))
	})

	It("stands in for the StatefulSet controller by creating the per-pod PVCs", func() {
		for _, key := range []client.ObjectKey{dataClaim, metaClaim} {
			pvc := &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{Name: key.Name, Namespace: itNamespace},
				Spec: corev1.PersistentVolumeClaimSpec{
					AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
					StorageClassName: ptr.To(scName),
					Resources: corev1.VolumeResourceRequirements{
						Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")},
					},
				},
			}
			Expect(k8sClient.Create(ctx, pvc)).To(Succeed())

			// The API server only permits resizing a *bound* claim, so mark it bound the way a
			// real provisioner would — envtest has no controller to do it.
			pvc.Status.Phase = corev1.ClaimBound
			pvc.Status.Capacity = corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")}
			Expect(k8sClient.Status().Update(ctx, pvc)).To(Succeed())
		}
	})

	It("grows the data volume in place and requeues to recreate the StatefulSet", func() {
		By("editing the pool's data size")
		var cluster garagev1alpha1.GarageCluster
		Expect(k8sClient.Get(ctx, clusterKey, &cluster)).To(Succeed())
		cluster.Spec.NodePools[0].Storage.Data.Size = resource.MustParse("2Gi")
		Expect(k8sClient.Update(ctx, &cluster)).To(Succeed())

		result, err := reconciler().Reconcile(ctx, reconcile.Request{NamespacedName: clusterKey})
		Expect(err).NotTo(HaveOccurred())
		Expect(result.RequeueAfter).To(Equal(workloadRequeue))

		By("patching the existing data PVC up to the new size")
		expectClaimSize(dataClaim, "2Gi")
		By("leaving the unchanged meta PVC alone")
		expectClaimSize(metaClaim, "1Gi")

		By("orphan-deleting the StatefulSet so it can be recreated with the larger template")
		// envtest runs no garbage collector, so an orphan deletion does not finalize: the object
		// is left with a deletionTimestamp and the orphan finalizer. Assert the deletion was
		// initiated; the next step plays the GC's part.
		var ss appsv1.StatefulSet
		Expect(k8sClient.Get(ctx, ssKey, &ss)).To(Succeed())
		Expect(ss.DeletionTimestamp).NotTo(BeNil())
	})

	It("recreates the StatefulSet with the larger volume template", func() {
		By("standing in for the garbage collector to finalize the orphan deletion")
		var ss appsv1.StatefulSet
		Expect(k8sClient.Get(ctx, ssKey, &ss)).To(Succeed())
		ss.Finalizers = nil
		Expect(k8sClient.Update(ctx, &ss)).To(Succeed())
		Eventually(func() bool {
			return apierrors.IsNotFound(k8sClient.Get(ctx, ssKey, &ss))
		}).Should(BeTrue())

		_, err := reconciler().Reconcile(ctx, reconcile.Request{NamespacedName: clusterKey})
		Expect(err).NotTo(HaveOccurred())

		Expect(k8sClient.Get(ctx, ssKey, &ss)).To(Succeed())
		got, _ := templateStorageRequest(&ss, volumeNameData)
		Expect(got.Cmp(resource.MustParse("2Gi"))).To(Equal(0))
	})
})

// This envtest spec proves the annotation-triggered maintenance path is wired into Reconcile
// against a real API server, and that the generated CRD status schema accepts the maintenance
// block. It drives a single-node cluster to Ready, then triggers a snapshot and a repair via
// annotations and asserts the recorded outcome survives a status round-trip.
var _ = Describe("GarageCluster maintenance integration", Ordered, func() {
	const (
		itNamespace = "garage-cluster-maint-it"
		clusterName = "maintcluster"
	)

	reconciler := func() *GarageClusterReconciler {
		return &GarageClusterReconciler{
			Client: k8sClient,
			Scheme: k8sClient.Scheme(),
			NewAdminClient: func(string, string) (clusterAdmin, error) {
				return &fakeClusterAdmin{nodeID: nodeSelf, recorder: &meshRecorder{}, layout: newFakeLayout()}, nil
			},
			Recorder: record.NewFakeRecorder(100),
		}
	}

	clusterKey := client.ObjectKey{Name: clusterName, Namespace: itNamespace}
	ssKey := client.ObjectKey{Name: clusterName + "-default", Namespace: itNamespace}

	BeforeAll(func() {
		Expect(k8sClient.Create(ctx, &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{Name: itNamespace},
		})).To(Succeed())
		Expect(k8sClient.Create(ctx, &garagev1alpha1.GarageCluster{
			ObjectMeta: metav1.ObjectMeta{Name: clusterName, Namespace: itNamespace},
			Spec:       newBasicClusterSpec(1, 1),
		})).To(Succeed())
	})

	AfterAll(func() {
		cluster := &garagev1alpha1.GarageCluster{ObjectMeta: metav1.ObjectMeta{Name: clusterName, Namespace: itNamespace}}
		Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, cluster))).To(Succeed())
	})

	It("drives the cluster to Ready", func() {
		_, err := reconciler().Reconcile(ctx, reconcile.Request{NamespacedName: clusterKey})
		Expect(err).NotTo(HaveOccurred())

		By("marking the StatefulSet ready (envtest has no kubelet to do it)")
		var ss appsv1.StatefulSet
		Expect(k8sClient.Get(ctx, ssKey, &ss)).To(Succeed())
		ss.Status.ObservedGeneration = ss.Generation
		ss.Status.Replicas = 1
		ss.Status.ReadyReplicas = 1
		Expect(k8sClient.Status().Update(ctx, &ss)).To(Succeed())

		_, err = reconciler().Reconcile(ctx, reconcile.Request{NamespacedName: clusterKey})
		Expect(err).NotTo(HaveOccurred())
	})

	It("runs a snapshot and a repair when the annotations are set, recording the outcome", func() {
		var cluster garagev1alpha1.GarageCluster
		Expect(k8sClient.Get(ctx, clusterKey, &cluster)).To(Succeed())
		cluster.Annotations = map[string]string{
			annotationSnapshot: "snap-1",
			annotationRepair:   repairBlocks,
		}
		Expect(k8sClient.Update(ctx, &cluster)).To(Succeed())

		_, err := reconciler().Reconcile(ctx, reconcile.Request{NamespacedName: clusterKey})
		Expect(err).NotTo(HaveOccurred())

		Expect(k8sClient.Get(ctx, clusterKey, &cluster)).To(Succeed())
		Expect(cluster.Status.Maintenance).NotTo(BeNil())
		Expect(cluster.Status.Maintenance.Snapshot.ObservedTrigger).To(Equal("snap-1"))
		Expect(cluster.Status.Maintenance.Snapshot.Result).To(Equal(resultSucceeded))
		Expect(cluster.Status.Maintenance.Repair.Type).To(Equal(repairBlocks))
		Expect(cluster.Status.Maintenance.Repair.State).To(Equal(stateLaunched))
	})
})
