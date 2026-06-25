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
	"strings"

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

// meshRecorder captures the peer lists ConnectNodes was called with, shared across the
// per-pod fake admin clients a single reconcile creates.
type meshRecorder struct {
	calls int
	peers [][]string
}

// fakeClusterAdmin stands in for the Garage Admin API so reconcile logic can run in envtest,
// where no real Garage process exists.
type fakeClusterAdmin struct {
	nodeID   string
	recorder *meshRecorder
}

func (f *fakeClusterAdmin) NodeID(context.Context) (string, error) { return f.nodeID, nil }

func (f *fakeClusterAdmin) ConnectNodes(_ context.Context, peers []string) error {
	if f.recorder != nil {
		f.recorder.calls++
		f.recorder.peers = append(f.recorder.peers, peers)
	}
	return nil
}

func (f *fakeClusterAdmin) EnsureLayout(context.Context, []garageadmin.DesiredRole) (bool, int64, error) {
	return true, 1, nil
}

// newBasicClusterSpec returns a single-pool GarageCluster spec (no S3, default storage) with
// the given replication factor and replica count, as the API server would present it after
// defaulting. Shared by the single- and multi-node reconcile suites.
func newBasicClusterSpec(replicationFactor, replicas int32) garagev1alpha1.GarageClusterSpec {
	return garagev1alpha1.GarageClusterSpec{
		Image:             garagev1alpha1.GarageImage{Repository: defaultImageRepository, Tag: defaultImageTag},
		DBEngine:          "lmdb",
		BlockSize:         1048576,
		ReplicationFactor: replicationFactor,
		ConsistencyMode:   "consistent",
		CompressionLevel:  1,
		NodePools: []garagev1alpha1.NodePool{{
			Name:     "default",
			Replicas: replicas,
			Storage: garagev1alpha1.NodePoolStorage{
				Data: garagev1alpha1.StorageSpec{Size: apiresource.MustParse("1Gi")},
				Meta: garagev1alpha1.StorageSpec{Size: apiresource.MustParse("1Gi")},
			},
		}},
	}
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
		defaultSSName     = "test-resource-default"
		missingName       = "missing"
	)

	ctx := context.Background()
	key := types.NamespacedName{Name: resourceName, Namespace: resourceNamespace}

	mesh := &meshRecorder{}
	reconcilerWithFakeAdmin := func() *GarageClusterReconciler {
		return &GarageClusterReconciler{
			Client: k8sClient,
			Scheme: k8sClient.Scheme(),
			NewAdminClient: func(string, string) (clusterAdmin, error) {
				return &fakeClusterAdmin{nodeID: "node-self", recorder: mesh}, nil
			},
		}
	}

	BeforeAll(func() {
		resource := &garagev1alpha1.GarageCluster{
			ObjectMeta: metav1.ObjectMeta{Name: resourceName, Namespace: resourceNamespace},
			Spec:       newBasicClusterSpec(1, 1),
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
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: defaultSSName, Namespace: resourceNamespace}, &ss)).To(Succeed())
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
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: defaultSSName, Namespace: resourceNamespace}, &ss)).To(Succeed())
		ss.Status.ObservedGeneration = ss.Generation
		ss.Status.Replicas = 1
		ss.Status.ReadyReplicas = 1
		Expect(k8sClient.Status().Update(ctx, &ss)).To(Succeed())

		result, err := reconcilerWithFakeAdmin().Reconcile(ctx, reconcile.Request{NamespacedName: key})
		Expect(err).NotTo(HaveOccurred())
		// A converged cluster requeues periodically so status.health is re-polled even
		// though Garage emits no Kubernetes event when its internal health changes.
		Expect(result.RequeueAfter).To(Equal(steadyStateRequeue))

		var cluster garagev1alpha1.GarageCluster
		Expect(k8sClient.Get(ctx, key, &cluster)).To(Succeed())
		Expect(meta.IsStatusConditionTrue(cluster.Status.Conditions, conditionReady)).To(BeTrue())
		Expect(meta.IsStatusConditionTrue(cluster.Status.Conditions, conditionLayoutApplied)).To(BeTrue())
		Expect(cluster.Status.Layout).NotTo(BeNil())
		Expect(cluster.Status.Layout.Version).To(Equal(int64(1)))
		Expect(cluster.Status.Layout.Nodes).To(HaveLen(1))
		Expect(cluster.Status.Layout.Nodes[0].NodeID).To(Equal("node-self"))

		By("not attempting to peer a single-node cluster")
		Expect(mesh.calls).To(Equal(0))
		Expect(meta.IsStatusConditionTrue(cluster.Status.Conditions, conditionPeersConnected)).To(BeTrue())

		Expect(cluster.Status.Health).NotTo(BeNil())
		Expect(cluster.Status.Health.Status).To(Equal("healthy"))
		Expect(cluster.Status.Health.PartitionsQuorum).To(Equal("256/256"))
	})

	It("is idempotent: a converged reconcile does not error", func() {
		_, err := reconcilerWithFakeAdmin().Reconcile(ctx, reconcile.Request{NamespacedName: key})
		Expect(err).NotTo(HaveOccurred())
	})

	It("rolls the StatefulSet pod template when the Garage config changes", func() {
		ssName := types.NamespacedName{Name: defaultSSName, Namespace: resourceNamespace}
		var before appsv1.StatefulSet
		Expect(k8sClient.Get(ctx, ssName, &before)).To(Succeed())
		hashBefore := before.Spec.Template.Annotations[annotationConfigHash]
		Expect(hashBefore).NotTo(BeEmpty())

		By("editing a rendered config field on the spec")
		var cluster garagev1alpha1.GarageCluster
		Expect(k8sClient.Get(ctx, key, &cluster)).To(Succeed())
		cluster.Spec.CompressionLevel = 5
		Expect(k8sClient.Update(ctx, &cluster)).To(Succeed())

		_, err := reconcilerWithFakeAdmin().Reconcile(ctx, reconcile.Request{NamespacedName: key})
		Expect(err).NotTo(HaveOccurred())

		By("propagating a new config hash into the StatefulSet pod template")
		var after appsv1.StatefulSet
		Expect(k8sClient.Get(ctx, ssName, &after)).To(Succeed())
		Expect(after.Spec.Template.Annotations[annotationConfigHash]).NotTo(Equal(hashBefore))
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

// nodeIDFromBaseURL derives a deterministic, per-pod Garage node id from a pod's admin
// baseURL ("http://<pod>.<headless>.<ns>.svc:<port>"), so a multi-replica fake cluster yields
// three distinct nodes the way a real one would.
func nodeIDFromBaseURL(baseURL string) string {
	host := strings.TrimPrefix(baseURL, "http://")
	host, _, _ = strings.Cut(host, ":")
	pod, _, _ := strings.Cut(host, ".")
	return "id-" + pod
}

var _ = Describe("GarageCluster multi-node mesh", Ordered, func() {
	const (
		resourceName      = "trio"
		resourceNamespace = "default"
	)

	ctx := context.Background()
	key := types.NamespacedName{Name: resourceName, Namespace: resourceNamespace}

	mesh := &meshRecorder{}
	reconciler := func() *GarageClusterReconciler {
		return &GarageClusterReconciler{
			Client: k8sClient,
			Scheme: k8sClient.Scheme(),
			NewAdminClient: func(baseURL, _ string) (clusterAdmin, error) {
				return &fakeClusterAdmin{nodeID: nodeIDFromBaseURL(baseURL), recorder: mesh}, nil
			},
		}
	}

	BeforeAll(func() {
		resource := &garagev1alpha1.GarageCluster{
			ObjectMeta: metav1.ObjectMeta{Name: resourceName, Namespace: resourceNamespace},
			Spec:       newBasicClusterSpec(3, 3),
		}
		Expect(k8sClient.Create(ctx, resource)).To(Succeed())
	})

	AfterAll(func() {
		resource := &garagev1alpha1.GarageCluster{}
		Expect(k8sClient.Get(ctx, key, resource)).To(Succeed())
		Expect(k8sClient.Delete(ctx, resource)).To(Succeed())
	})

	It("peers the non-first nodes into a mesh before applying the 3-node layout", func() {
		By("provisioning the workload (pods not yet ready)")
		_, err := reconciler().Reconcile(ctx, reconcile.Request{NamespacedName: key})
		Expect(err).NotTo(HaveOccurred())
		Expect(mesh.calls).To(Equal(0), "must not peer before pods are ready")

		By("marking the StatefulSet ready (envtest has no kubelet to do it)")
		var ss appsv1.StatefulSet
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "trio-default", Namespace: resourceNamespace}, &ss)).To(Succeed())
		ss.Status.ObservedGeneration = ss.Generation
		ss.Status.Replicas = 3
		ss.Status.ReadyReplicas = 3
		Expect(k8sClient.Status().Update(ctx, &ss)).To(Succeed())

		_, err = reconciler().Reconcile(ctx, reconcile.Request{NamespacedName: key})
		Expect(err).NotTo(HaveOccurred())

		By("connecting node-0 to the two other nodes by their RPC DNS")
		Expect(mesh.calls).To(Equal(1))
		Expect(mesh.peers[0]).To(ConsistOf(
			"id-trio-default-1@trio-default-1.trio-headless.default.svc:3901",
			"id-trio-default-2@trio-default-2.trio-headless.default.svc:3901",
		))

		By("reporting a 3-node layout and Ready=True")
		var cluster garagev1alpha1.GarageCluster
		Expect(k8sClient.Get(ctx, key, &cluster)).To(Succeed())
		Expect(meta.IsStatusConditionTrue(cluster.Status.Conditions, conditionPeersConnected)).To(BeTrue())
		Expect(meta.IsStatusConditionTrue(cluster.Status.Conditions, conditionReady)).To(BeTrue())
		Expect(cluster.Status.Layout.Nodes).To(HaveLen(3))
	})
})
