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

// fakeLayout simulates the cluster-wide Garage layout. A single instance is shared across
// the per-pod fake admin clients one reconcile creates and persists across reconciles, so
// PlanLayout/ApplyLayout behave like a real cluster converging over successive passes:
// applying commits the last-planned desired set as the new applied layout and bumps the
// version, which is what later PlanLayout calls diff against.
type fakeLayout struct {
	version     int64
	applied     map[string]struct{}
	lastDesired []garageadmin.DesiredRole
	applyCalls  int
	previewMsg  []string
}

func newFakeLayout() *fakeLayout {
	return &fakeLayout{applied: map[string]struct{}{}, previewMsg: []string{"fake preview"}}
}

// plan diffs the desired roles against the applied layout, mirroring garageadmin.PlanLayout:
// desired nodes absent from the layout are additive, applied nodes no longer desired are
// removals. AdditiveChanges only needs the right length here — the controller acts on its
// count, not its contents.
func (l *fakeLayout) plan(desired []garageadmin.DesiredRole) *garageadmin.LayoutPlan {
	l.lastDesired = desired
	wanted := make(map[string]struct{}, len(desired))
	plan := &garageadmin.LayoutPlan{CurrentVersion: l.version}
	for _, d := range desired {
		wanted[d.NodeID] = struct{}{}
		if _, ok := l.applied[d.NodeID]; !ok {
			plan.AdditiveChanges = append(plan.AdditiveChanges, garageadmin.NodeRoleChangeRequest{})
		}
	}
	for id := range l.applied {
		if _, ok := wanted[id]; !ok {
			plan.Removals = append(plan.Removals, id)
		}
	}
	if plan.HasChanges() {
		plan.TargetVersion = l.version + 1
	}
	return plan
}

func (l *fakeLayout) apply(version int64) {
	l.version = version
	l.applied = make(map[string]struct{}, len(l.lastDesired))
	for _, d := range l.lastDesired {
		l.applied[d.NodeID] = struct{}{}
	}
	l.applyCalls++
}

// fakeMaintenance records the maintenance calls one or more fake admin clients receive and
// supplies the responses they return. A single instance is shared across the per-pod clients a
// reconcile creates (like fakeLayout), so a test can drive and assert maintenance across passes.
type fakeMaintenance struct {
	snapshotCalls int
	repairCalls   int
	repairTypes   []string

	// snapshotResult / repairResult override the default single-node success when set.
	snapshotResult *garageadmin.MultiNodeResult
	repairResult   *garageadmin.MultiNodeResult

	// workers is returned from ListActiveWorkers (default: none, i.e. a finished repair).
	workers []garageadmin.WorkerSummary
}

// fakeClusterAdmin stands in for the Garage Admin API so reconcile logic can run in envtest,
// where no real Garage process exists.
type fakeClusterAdmin struct {
	nodeID   string
	recorder *meshRecorder
	layout   *fakeLayout
	maint    *fakeMaintenance

	// health, when set, overrides the default healthy response so migration tests can drive the
	// partition counts the re-replication wait reads.
	health *garageadmin.GetClusterHealthResponse
}

func (f *fakeClusterAdmin) CreateMetadataSnapshot(context.Context, string) (garageadmin.MultiNodeResult, error) {
	if f.maint != nil {
		f.maint.snapshotCalls++
		if f.maint.snapshotResult != nil {
			return *f.maint.snapshotResult, nil
		}
	}
	return garageadmin.MultiNodeResult{Succeeded: []string{f.nodeID}}, nil
}

func (f *fakeClusterAdmin) LaunchRepair(_ context.Context, _, repairType string) (garageadmin.MultiNodeResult, error) {
	// Mirror the real client, which rejects an unknown type before contacting Garage.
	if !garageadmin.IsValidRepairType(repairType) {
		return garageadmin.MultiNodeResult{}, garageadmin.ErrUnknownRepairType
	}
	if f.maint != nil {
		f.maint.repairCalls++
		f.maint.repairTypes = append(f.maint.repairTypes, repairType)
		if f.maint.repairResult != nil {
			return *f.maint.repairResult, nil
		}
	}
	return garageadmin.MultiNodeResult{Succeeded: []string{f.nodeID}}, nil
}

func (f *fakeClusterAdmin) ListActiveWorkers(context.Context, string) ([]garageadmin.WorkerSummary, error) {
	if f.maint != nil {
		return f.maint.workers, nil
	}
	return nil, nil
}

func (f *fakeClusterAdmin) NodeID(context.Context) (string, error) { return f.nodeID, nil }

func (f *fakeClusterAdmin) ConnectNodes(_ context.Context, peers []string) error {
	if f.recorder != nil {
		f.recorder.calls++
		f.recorder.peers = append(f.recorder.peers, peers)
	}
	return nil
}

func (f *fakeClusterAdmin) PlanLayout(_ context.Context, desired []garageadmin.DesiredRole) (*garageadmin.LayoutPlan, error) {
	return f.layout.plan(desired), nil
}

func (f *fakeClusterAdmin) StageLayoutChanges(context.Context, []garageadmin.NodeRoleChangeRequest) error {
	return nil
}

func (f *fakeClusterAdmin) PreviewStagedChanges(context.Context) ([]string, error) {
	return f.layout.previewMsg, nil
}

func (f *fakeClusterAdmin) ApplyLayout(_ context.Context, version int64) error {
	f.layout.apply(version)
	return nil
}

func (f *fakeClusterAdmin) RevertStagedChanges(context.Context) error { return nil }

func (f *fakeClusterAdmin) AppliedLayoutNodeIDs(context.Context) ([]string, error) {
	ids := make([]string, 0, len(f.layout.applied))
	for id := range f.layout.applied {
		ids = append(ids, id)
	}
	return ids, nil
}

func (f *fakeClusterAdmin) RemoveNode(_ context.Context, nodeID string) error {
	delete(f.layout.applied, nodeID)
	f.layout.version++
	f.layout.applyCalls++
	return nil
}

func (f *fakeClusterAdmin) AddNode(_ context.Context, role garageadmin.DesiredRole) error {
	f.layout.applied[role.NodeID] = struct{}{}
	f.layout.version++
	f.layout.applyCalls++
	return nil
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
	if f.health != nil {
		return f.health, nil
	}
	return &garageadmin.GetClusterHealthResponse{
		Status:           "healthy",
		ConnectedNodes:   1,
		KnownNodes:       1,
		Partitions:       256,
		PartitionsQuorum: 256,
		PartitionsAllOk:  256,
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
	layout := newFakeLayout()
	reconcilerWithFakeAdmin := func() *GarageClusterReconciler {
		return &GarageClusterReconciler{
			Client: k8sClient,
			Scheme: k8sClient.Scheme(),
			NewAdminClient: func(string, string) (clusterAdmin, error) {
				return &fakeClusterAdmin{nodeID: nodeSelf, recorder: mesh, layout: layout}, nil
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
	layout := newFakeLayout()
	reconciler := func() *GarageClusterReconciler {
		return &GarageClusterReconciler{
			Client: k8sClient,
			Scheme: k8sClient.Scheme(),
			NewAdminClient: func(baseURL, _ string) (clusterAdmin, error) {
				return &fakeClusterAdmin{nodeID: nodeIDFromBaseURL(baseURL), recorder: mesh, layout: layout}, nil
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

// createPVC simulates a volumeClaimTemplates PVC that a real StatefulSet controller would
// create. envtest runs no such controller, so the drain tests stand them up explicitly to
// assert the operator reclaims the right ones.
func createPVC(ctx context.Context, namespace, name string) {
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: apiresource.MustParse("1Gi")},
			},
		},
	}
	Expect(k8sClient.Create(ctx, pvc)).To(Succeed())
}

// pvcExists reports whether a PVC is present and not being deleted. envtest enables the
// StorageObjectInUseProtection admission plugin, which stamps every PVC with the
// kubernetes.io/pvc-protection finalizer, but runs no controller to clear it — so a deleted
// PVC lingers in Terminating. A non-zero deletion timestamp therefore means "gone".
//
//nolint:unparam // namespace is a parameter for symmetry with createPVC; tests use "default".
func pvcExists(ctx context.Context, namespace, name string) bool {
	var pvc corev1.PersistentVolumeClaim
	if err := k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: namespace}, &pvc); err != nil {
		return false
	}
	return pvc.DeletionTimestamp.IsZero()
}

var _ = Describe("GarageCluster destructive layout (scale-down)", Ordered, func() {
	const (
		resourceName      = "drain"
		resourceNamespace = "default"
		ssName            = "drain-default"
	)

	ctx := context.Background()
	key := types.NamespacedName{Name: resourceName, Namespace: resourceNamespace}

	layout := newFakeLayout()
	mesh := &meshRecorder{}
	reconciler := func() *GarageClusterReconciler {
		return &GarageClusterReconciler{
			Client: k8sClient,
			Scheme: k8sClient.Scheme(),
			NewAdminClient: func(baseURL, _ string) (clusterAdmin, error) {
				return &fakeClusterAdmin{nodeID: nodeIDFromBaseURL(baseURL), recorder: mesh, layout: layout}, nil
			},
		}
	}

	markReady := func(replicas int32) {
		var ss appsv1.StatefulSet
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: ssName, Namespace: resourceNamespace}, &ss)).To(Succeed())
		ss.Status.ObservedGeneration = ss.Generation
		ss.Status.Replicas = replicas
		ss.Status.ReadyReplicas = replicas
		Expect(k8sClient.Status().Update(ctx, &ss)).To(Succeed())
	}

	BeforeAll(func() {
		resource := &garagev1alpha1.GarageCluster{
			ObjectMeta: metav1.ObjectMeta{Name: resourceName, Namespace: resourceNamespace},
			Spec:       newBasicClusterSpec(3, 3),
		}
		Expect(k8sClient.Create(ctx, resource)).To(Succeed())

		By("converging the 3-node cluster to Ready")
		_, err := reconciler().Reconcile(ctx, reconcile.Request{NamespacedName: key})
		Expect(err).NotTo(HaveOccurred())
		markReady(3)
		_, err = reconciler().Reconcile(ctx, reconcile.Request{NamespacedName: key})
		Expect(err).NotTo(HaveOccurred())
		Expect(layout.applyCalls).To(Equal(1))

		By("standing up the PVCs the StatefulSet controller would have created")
		for ord := range 3 {
			for _, vol := range []string{volumeNameMeta, volumeNameData} {
				createPVC(ctx, resourceNamespace, fmt.Sprintf("%s-%s-%d", vol, ssName, ord))
			}
		}
	})

	AfterAll(func() {
		resource := &garagev1alpha1.GarageCluster{}
		Expect(k8sClient.Get(ctx, key, resource)).To(Succeed())
		Expect(k8sClient.Delete(ctx, resource)).To(Succeed())
	})

	It("holds the scale-down behind the approval annotation", func() {
		var cluster garagev1alpha1.GarageCluster
		Expect(k8sClient.Get(ctx, key, &cluster)).To(Succeed())
		cluster.Spec.NodePools[0].Replicas = 2
		Expect(k8sClient.Update(ctx, &cluster)).To(Succeed())

		_, err := reconciler().Reconcile(ctx, reconcile.Request{NamespacedName: key})
		Expect(err).NotTo(HaveOccurred())

		By("not scaling the StatefulSet down")
		var ss appsv1.StatefulSet
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: ssName, Namespace: resourceNamespace}, &ss)).To(Succeed())
		Expect(*ss.Spec.Replicas).To(Equal(int32(3)))

		By("reporting LayoutChangePending and not applying the drain")
		Expect(k8sClient.Get(ctx, key, &cluster)).To(Succeed())
		Expect(meta.IsStatusConditionTrue(cluster.Status.Conditions, conditionLayoutChangePending)).To(BeTrue())
		Expect(layout.applyCalls).To(Equal(1))

		By("keeping the cluster Ready while the change is pending")
		Expect(meta.IsStatusConditionTrue(cluster.Status.Conditions, conditionReady)).To(BeTrue())
	})

	It("ignores an approval annotation for a stale target version", func() {
		var cluster garagev1alpha1.GarageCluster
		Expect(k8sClient.Get(ctx, key, &cluster)).To(Succeed())
		cluster.Annotations = map[string]string{annotationApproveLayout: "99"}
		Expect(k8sClient.Update(ctx, &cluster)).To(Succeed())

		_, err := reconciler().Reconcile(ctx, reconcile.Request{NamespacedName: key})
		Expect(err).NotTo(HaveOccurred())
		Expect(layout.applyCalls).To(Equal(1))

		Expect(k8sClient.Get(ctx, key, &cluster)).To(Succeed())
		Expect(meta.IsStatusConditionTrue(cluster.Status.Conditions, conditionLayoutChangePending)).To(BeTrue())
	})

	It("drains the node and reclaims its PVCs once approved", func() {
		var cluster garagev1alpha1.GarageCluster
		Expect(k8sClient.Get(ctx, key, &cluster)).To(Succeed())
		// The pending change targets version current(1)+1 = 2.
		cluster.Annotations = map[string]string{annotationApproveLayout: "2"}
		Expect(k8sClient.Update(ctx, &cluster)).To(Succeed())

		_, err := reconciler().Reconcile(ctx, reconcile.Request{NamespacedName: key})
		Expect(err).NotTo(HaveOccurred())

		By("applying the drain exactly once")
		Expect(layout.applyCalls).To(Equal(2))

		By("scaling the StatefulSet down to the desired replica count")
		var ss appsv1.StatefulSet
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: ssName, Namespace: resourceNamespace}, &ss)).To(Succeed())
		Expect(*ss.Spec.Replicas).To(Equal(int32(2)))

		By("deleting the drained node's PVCs but keeping the survivors'")
		for _, vol := range []string{volumeNameMeta, volumeNameData} {
			Expect(pvcExists(ctx, resourceNamespace, fmt.Sprintf("%s-%s-2", vol, ssName))).To(BeFalse())
			Expect(pvcExists(ctx, resourceNamespace, fmt.Sprintf("%s-%s-0", vol, ssName))).To(BeTrue())
			Expect(pvcExists(ctx, resourceNamespace, fmt.Sprintf("%s-%s-1", vol, ssName))).To(BeTrue())
		}

		By("clearing the pending condition and reporting the new layout")
		Expect(k8sClient.Get(ctx, key, &cluster)).To(Succeed())
		Expect(meta.FindStatusCondition(cluster.Status.Conditions, conditionLayoutChangePending)).To(BeNil())
		Expect(meta.IsStatusConditionTrue(cluster.Status.Conditions, conditionLayoutApplied)).To(BeTrue())
		Expect(cluster.Status.Layout.Version).To(Equal(int64(2)))
		Expect(cluster.Status.Layout.Nodes).To(HaveLen(2))
	})
})

var _ = Describe("GarageCluster destructive layout (rf=1 blocked)", Ordered, func() {
	const (
		resourceName      = "rf1"
		resourceNamespace = "default"
		ssName            = "rf1-default"
	)

	ctx := context.Background()
	key := types.NamespacedName{Name: resourceName, Namespace: resourceNamespace}

	layout := newFakeLayout()
	reconciler := func() *GarageClusterReconciler {
		return &GarageClusterReconciler{
			Client: k8sClient,
			Scheme: k8sClient.Scheme(),
			NewAdminClient: func(baseURL, _ string) (clusterAdmin, error) {
				return &fakeClusterAdmin{nodeID: nodeIDFromBaseURL(baseURL), layout: layout}, nil
			},
		}
	}

	markReady := func(replicas int32) {
		var ss appsv1.StatefulSet
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: ssName, Namespace: resourceNamespace}, &ss)).To(Succeed())
		ss.Status.ObservedGeneration = ss.Generation
		ss.Status.Replicas = replicas
		ss.Status.ReadyReplicas = replicas
		Expect(k8sClient.Status().Update(ctx, &ss)).To(Succeed())
	}

	BeforeAll(func() {
		resource := &garagev1alpha1.GarageCluster{
			ObjectMeta: metav1.ObjectMeta{Name: resourceName, Namespace: resourceNamespace},
			Spec:       newBasicClusterSpec(1, 2),
		}
		Expect(k8sClient.Create(ctx, resource)).To(Succeed())
		_, err := reconciler().Reconcile(ctx, reconcile.Request{NamespacedName: key})
		Expect(err).NotTo(HaveOccurred())
		markReady(2)
		_, err = reconciler().Reconcile(ctx, reconcile.Request{NamespacedName: key})
		Expect(err).NotTo(HaveOccurred())
		Expect(layout.applyCalls).To(Equal(1))
	})

	AfterAll(func() {
		resource := &garagev1alpha1.GarageCluster{}
		Expect(k8sClient.Get(ctx, key, resource)).To(Succeed())
		Expect(k8sClient.Delete(ctx, resource)).To(Succeed())
	})

	It("refuses the removal even when approved", func() {
		var cluster garagev1alpha1.GarageCluster
		Expect(k8sClient.Get(ctx, key, &cluster)).To(Succeed())
		cluster.Spec.NodePools[0].Replicas = 1
		// Pre-approve the would-be target version; the rf guardrail must still refuse.
		cluster.Annotations = map[string]string{annotationApproveLayout: "2"}
		Expect(k8sClient.Update(ctx, &cluster)).To(Succeed())

		_, err := reconciler().Reconcile(ctx, reconcile.Request{NamespacedName: key})
		Expect(err).NotTo(HaveOccurred())

		By("not applying and not scaling down")
		Expect(layout.applyCalls).To(Equal(1))
		var ss appsv1.StatefulSet
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: ssName, Namespace: resourceNamespace}, &ss)).To(Succeed())
		Expect(*ss.Spec.Replicas).To(Equal(int32(2)))

		By("reporting LayoutChangePending=False with reason Blocked")
		Expect(k8sClient.Get(ctx, key, &cluster)).To(Succeed())
		cond := meta.FindStatusCondition(cluster.Status.Conditions, conditionLayoutChangePending)
		Expect(cond).NotTo(BeNil())
		Expect(cond.Status).To(Equal(metav1.ConditionFalse))
		Expect(cond.Reason).To(Equal("Blocked"))
	})
})

var _ = Describe("GarageCluster destructive layout (whole-pool removal)", Ordered, func() {
	const (
		resourceName      = "rmpool"
		resourceNamespace = "default"
		ssA               = "rmpool-a"
		ssB               = "rmpool-b"
	)

	ctx := context.Background()
	key := types.NamespacedName{Name: resourceName, Namespace: resourceNamespace}

	layout := newFakeLayout()
	reconciler := func() *GarageClusterReconciler {
		return &GarageClusterReconciler{
			Client: k8sClient,
			Scheme: k8sClient.Scheme(),
			NewAdminClient: func(baseURL, _ string) (clusterAdmin, error) {
				return &fakeClusterAdmin{nodeID: nodeIDFromBaseURL(baseURL), layout: layout}, nil
			},
		}
	}

	markReady := func(name string) {
		var ss appsv1.StatefulSet
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: resourceNamespace}, &ss)).To(Succeed())
		ss.Status.ObservedGeneration = ss.Generation
		ss.Status.Replicas = 1
		ss.Status.ReadyReplicas = 1
		Expect(k8sClient.Status().Update(ctx, &ss)).To(Succeed())
	}

	twoPoolSpec := func() garagev1alpha1.GarageClusterSpec {
		spec := newBasicClusterSpec(2, 1)
		spec.NodePools[0].Name = "a"
		pool := spec.NodePools[0]
		pool.Name = "b"
		spec.NodePools = append(spec.NodePools, pool)
		return spec
	}

	BeforeAll(func() {
		resource := &garagev1alpha1.GarageCluster{
			ObjectMeta: metav1.ObjectMeta{Name: resourceName, Namespace: resourceNamespace},
			Spec:       twoPoolSpec(),
		}
		Expect(k8sClient.Create(ctx, resource)).To(Succeed())

		_, err := reconciler().Reconcile(ctx, reconcile.Request{NamespacedName: key})
		Expect(err).NotTo(HaveOccurred())
		markReady(ssA)
		markReady(ssB)
		_, err = reconciler().Reconcile(ctx, reconcile.Request{NamespacedName: key})
		Expect(err).NotTo(HaveOccurred())
		Expect(layout.applyCalls).To(Equal(1))

		for _, vol := range []string{volumeNameMeta, volumeNameData} {
			createPVC(ctx, resourceNamespace, fmt.Sprintf("%s-%s-0", vol, ssB))
		}
	})

	AfterAll(func() {
		resource := &garagev1alpha1.GarageCluster{}
		Expect(k8sClient.Get(ctx, key, resource)).To(Succeed())
		Expect(k8sClient.Delete(ctx, resource)).To(Succeed())
	})

	It("deletes the removed pool's StatefulSet and PVCs once approved", func() {
		var cluster garagev1alpha1.GarageCluster
		Expect(k8sClient.Get(ctx, key, &cluster)).To(Succeed())
		cluster.Spec.NodePools = cluster.Spec.NodePools[:1] // drop pool "b"
		cluster.Annotations = map[string]string{annotationApproveLayout: "2"}
		Expect(k8sClient.Update(ctx, &cluster)).To(Succeed())

		_, err := reconciler().Reconcile(ctx, reconcile.Request{NamespacedName: key})
		Expect(err).NotTo(HaveOccurred())
		Expect(layout.applyCalls).To(Equal(2))

		By("deleting pool b's StatefulSet")
		err = k8sClient.Get(ctx, types.NamespacedName{Name: ssB, Namespace: resourceNamespace}, &appsv1.StatefulSet{})
		Expect(apierrors.IsNotFound(err)).To(BeTrue())

		By("deleting pool b's PVCs and keeping pool a")
		for _, vol := range []string{volumeNameMeta, volumeNameData} {
			Expect(pvcExists(ctx, resourceNamespace, fmt.Sprintf("%s-%s-0", vol, ssB))).To(BeFalse())
		}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: ssA, Namespace: resourceNamespace}, &appsv1.StatefulSet{})).To(Succeed())

		By("reporting a single-node layout")
		Expect(k8sClient.Get(ctx, key, &cluster)).To(Succeed())
		Expect(cluster.Status.Layout.Nodes).To(HaveLen(1))
	})
})
