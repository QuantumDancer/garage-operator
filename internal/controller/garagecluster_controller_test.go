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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	apiresource "k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	garagev1alpha1 "github.com/QuantumDancer/garage-operator/api/v1alpha1"
	"github.com/QuantumDancer/garage-operator/internal/garageadmin"
)

// fakeClusterAdmin stands in for the Garage Admin API so reconcile logic can run in envtest,
// where no real Garage process exists.
type fakeClusterAdmin struct {
	nodeID string
}

func (f *fakeClusterAdmin) NodeID(context.Context) (string, error) { return f.nodeID, nil }

func (f *fakeClusterAdmin) EnsureLayout(context.Context, []garageadmin.DesiredRole) (bool, int64, error) {
	return true, 1, nil
}

func (f *fakeClusterAdmin) Health(context.Context) (*garageadmin.GetClusterHealthResponse, error) {
	return &garageadmin.GetClusterHealthResponse{
		Status:           "healthy",
		ConnectedNodes:   1,
		KnownNodes:       1,
		Partitions:       256,
		PartitionsQuorum: 256,
	}, nil
}

var _ = Describe("GarageCluster Controller", Ordered, func() {
	const (
		resourceName      = "test-resource"
		resourceNamespace = "default"
		rpcSecretName     = "test-resource-rpc-token"
		missingName       = "missing"
	)

	ctx := context.Background()
	key := types.NamespacedName{Name: resourceName, Namespace: resourceNamespace}

	reconcilerWithFakeAdmin := func() *GarageClusterReconciler {
		return &GarageClusterReconciler{
			Client: k8sClient,
			Scheme: k8sClient.Scheme(),
			NewAdminClient: func(string, string) (clusterAdmin, error) {
				return &fakeClusterAdmin{nodeID: "node-self"}, nil
			},
		}
	}

	BeforeAll(func() {
		resource := &garagev1alpha1.GarageCluster{
			ObjectMeta: metav1.ObjectMeta{Name: resourceName, Namespace: resourceNamespace},
			Spec: garagev1alpha1.GarageClusterSpec{
				Image:             garagev1alpha1.GarageImage{Repository: "dxflrs/amd64_garage", Tag: "v2.0.0"},
				DBEngine:          "lmdb",
				BlockSize:         1048576,
				ReplicationFactor: 1,
				ConsistencyMode:   "consistent",
				CompressionLevel:  1,
				NodePools: []garagev1alpha1.NodePool{{
					Name:     "default",
					Replicas: 1,
					Storage: garagev1alpha1.NodePoolStorage{
						Data: garagev1alpha1.StorageSpec{Size: apiresource.MustParse("1Gi")},
						Meta: garagev1alpha1.StorageSpec{Size: apiresource.MustParse("1Gi")},
					},
				}},
			},
		}
		Expect(k8sClient.Create(ctx, resource)).To(Succeed())
	})

	AfterAll(func() {
		resource := &garagev1alpha1.GarageCluster{}
		Expect(k8sClient.Get(ctx, key, resource)).To(Succeed())
		Expect(k8sClient.Delete(ctx, resource)).To(Succeed())
	})

	It("provisions the workload and waits for pods before applying layout", func() {
		result, err := reconcilerWithFakeAdmin().Reconcile(ctx, reconcile.Request{NamespacedName: key})
		Expect(err).NotTo(HaveOccurred())
		Expect(result.RequeueAfter).To(Equal(workloadRequeue))

		By("creating the ConfigMap, headless Service, generated Secrets and StatefulSet")
		var cm corev1.ConfigMap
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "test-resource-config", Namespace: resourceNamespace}, &cm)).To(Succeed())
		Expect(cm.Data["garage.toml"]).To(ContainSubstring("replication_factor = 1"))

		var headless corev1.Service
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "test-resource-headless", Namespace: resourceNamespace}, &headless)).To(Succeed())
		Expect(headless.Spec.ClusterIP).To(Equal(corev1.ClusterIPNone))

		for _, secretName := range []string{rpcSecretName, "test-resource-admin-token"} {
			var secret corev1.Secret
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: secretName, Namespace: resourceNamespace}, &secret)).To(Succeed())
			Expect(secret.Data["token"]).NotTo(BeEmpty())
		}

		var ss appsv1.StatefulSet
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "test-resource-default", Namespace: resourceNamespace}, &ss)).To(Succeed())
		Expect(*ss.Spec.Replicas).To(Equal(int32(1)))

		By("reporting WorkloadReady=False while pods are not ready")
		var cluster garagev1alpha1.GarageCluster
		Expect(k8sClient.Get(ctx, key, &cluster)).To(Succeed())
		Expect(meta.IsStatusConditionTrue(cluster.Status.Conditions, conditionWorkloadReady)).To(BeFalse())
		Expect(cluster.Status.Layout).To(BeNil())
	})

	It("does not regenerate the RPC secret on subsequent reconciles", func() {
		var before corev1.Secret
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: rpcSecretName, Namespace: resourceNamespace}, &before)).To(Succeed())

		_, err := reconcilerWithFakeAdmin().Reconcile(ctx, reconcile.Request{NamespacedName: key})
		Expect(err).NotTo(HaveOccurred())

		var after corev1.Secret
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: rpcSecretName, Namespace: resourceNamespace}, &after)).To(Succeed())
		Expect(after.Data["token"]).To(Equal(before.Data["token"]))
	})

	It("applies the layout and reports Ready once pods are ready", func() {
		By("marking the StatefulSet ready (envtest has no kubelet to do it)")
		var ss appsv1.StatefulSet
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "test-resource-default", Namespace: resourceNamespace}, &ss)).To(Succeed())
		ss.Status.ObservedGeneration = ss.Generation
		ss.Status.Replicas = 1
		ss.Status.ReadyReplicas = 1
		Expect(k8sClient.Status().Update(ctx, &ss)).To(Succeed())

		result, err := reconcilerWithFakeAdmin().Reconcile(ctx, reconcile.Request{NamespacedName: key})
		Expect(err).NotTo(HaveOccurred())
		Expect(result.RequeueAfter).To(BeZero())

		var cluster garagev1alpha1.GarageCluster
		Expect(k8sClient.Get(ctx, key, &cluster)).To(Succeed())
		Expect(meta.IsStatusConditionTrue(cluster.Status.Conditions, conditionReady)).To(BeTrue())
		Expect(meta.IsStatusConditionTrue(cluster.Status.Conditions, conditionLayoutApplied)).To(BeTrue())
		Expect(cluster.Status.Layout).NotTo(BeNil())
		Expect(cluster.Status.Layout.Version).To(Equal(int64(1)))
		Expect(cluster.Status.Layout.Nodes).To(HaveLen(1))
		Expect(cluster.Status.Layout.Nodes[0].NodeID).To(Equal("node-self"))
		Expect(cluster.Status.Health).NotTo(BeNil())
		Expect(cluster.Status.Health.Status).To(Equal("healthy"))
		Expect(cluster.Status.Health.PartitionsQuorum).To(Equal("256/256"))
	})

	It("is idempotent: a converged reconcile does not error", func() {
		_, err := reconcilerWithFakeAdmin().Reconcile(ctx, reconcile.Request{NamespacedName: key})
		Expect(err).NotTo(HaveOccurred())
	})

	It("ignores a deleted resource", func() {
		_, err := reconcilerWithFakeAdmin().Reconcile(ctx, reconcile.Request{
			NamespacedName: types.NamespacedName{Name: missingName, Namespace: resourceNamespace},
		})
		Expect(err).NotTo(HaveOccurred())
	})

	It("surfaces a NotFound for a never-created resource without requeue", func() {
		result, err := reconcilerWithFakeAdmin().Reconcile(ctx, reconcile.Request{
			NamespacedName: types.NamespacedName{Name: missingName, Namespace: resourceNamespace},
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(result.IsZero()).To(BeTrue())
		// Sanity: the missing resource truly does not exist.
		err = k8sClient.Get(ctx, types.NamespacedName{Name: missingName, Namespace: resourceNamespace}, &garagev1alpha1.GarageCluster{})
		Expect(apierrors.IsNotFound(err)).To(BeTrue())
	})
})
