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

	// redundancy is the applied zone-redundancy parameter, defaulting to Garage's Maximum.
	// redundancyCalls counts how many times the controller applied a change to it.
	redundancy      garageadmin.ZoneRedundancyValue
	redundancyCalls int
}

func newFakeLayout() *fakeLayout {
	return &fakeLayout{
		applied:    map[string]struct{}{},
		previewMsg: []string{"fake preview"},
		redundancy: garageadmin.ZoneRedundancyValue{Maximum: true},
	}
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

	// planErr, when set, makes PlanLayout fail so a test can drive the reconcileLayout error
	// path (the first Admin call reconcileLayout makes).
	planErr error

	// healthErr, when set, makes Health fail so a test can drive the swallowed-health-error
	// path: the cached status.Health must be invalidated rather than left stale.
	healthErr error

	// unreachable, keyed by node id, makes NodeID fail for the matching pod so a test can
	// simulate a single down node while the rest of the cluster keeps answering. A shared map
	// across the per-pod clients one reconcile creates lets a test mark exactly one node down.
	unreachable map[string]bool
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

func (f *fakeClusterAdmin) NodeID(context.Context) (string, error) {
	if f.unreachable[f.nodeID] {
		return "", fmt.Errorf("admin API unreachable")
	}
	return f.nodeID, nil
}

func (f *fakeClusterAdmin) ConnectNodes(_ context.Context, peers []string) error {
	if f.recorder != nil {
		f.recorder.calls++
		f.recorder.peers = append(f.recorder.peers, peers)
	}
	return nil
}

func (f *fakeClusterAdmin) PlanLayout(_ context.Context, desired []garageadmin.DesiredRole) (*garageadmin.LayoutPlan, error) {
	if f.planErr != nil {
		return nil, f.planErr
	}
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

func (f *fakeClusterAdmin) CurrentZoneRedundancy(context.Context) (garageadmin.ZoneRedundancyValue, error) {
	return f.layout.redundancy, nil
}

func (f *fakeClusterAdmin) SetZoneRedundancy(_ context.Context, desired garageadmin.ZoneRedundancyValue) (int64, error) {
	f.layout.redundancy = desired
	f.layout.version++
	f.layout.redundancyCalls++
	return f.layout.version, nil
}

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
	if f.healthErr != nil {
		return nil, f.healthErr
	}
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

	It("persists Ready=False when a layout reconcile errors", func() {
		// Regression guard for REVIEW.md #9: an error from a reconcile* step must be written to
		// status before Reconcile returns, otherwise kubectl keeps showing a stale Ready=True
		// while the operator error-loops. PlanLayout is the first call reconcileLayout makes, so
		// failing it drives the LayoutError path without disturbing the shared fake layout.
		By("provisioning the workload and marking the StatefulSet ready so the reconcile reaches the layout step")
		_, err := reconcilerWithFakeAdmin().Reconcile(ctx, reconcile.Request{NamespacedName: key})
		Expect(err).NotTo(HaveOccurred())
		var ss appsv1.StatefulSet
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: defaultSSName, Namespace: resourceNamespace}, &ss)).To(Succeed())
		ss.Status.ObservedGeneration = ss.Generation
		ss.Status.Replicas = 1
		ss.Status.ReadyReplicas = 1
		Expect(k8sClient.Status().Update(ctx, &ss)).To(Succeed())

		failing := &GarageClusterReconciler{
			Client: k8sClient,
			Scheme: k8sClient.Scheme(),
			NewAdminClient: func(string, string) (clusterAdmin, error) {
				return &fakeClusterAdmin{nodeID: nodeSelf, recorder: mesh, layout: layout, planErr: fmt.Errorf("admin API unreachable")}, nil
			},
		}

		_, err = failing.Reconcile(ctx, reconcile.Request{NamespacedName: key})
		Expect(err).To(HaveOccurred())

		var cluster garagev1alpha1.GarageCluster
		Expect(k8sClient.Get(ctx, key, &cluster)).To(Succeed())
		ready := meta.FindStatusCondition(cluster.Status.Conditions, conditionReady)
		Expect(ready).NotTo(BeNil())
		Expect(ready.Status).To(Equal(metav1.ConditionFalse))
		Expect(ready.Reason).To(Equal("LayoutError"))
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
//
//nolint:unparam // namespace is a parameter for symmetry with pvcExists; tests use "default".
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

		By("not reclaiming the surplus node's PVCs while the drain is still in the layout (REVIEW.md #6 safety)")
		for _, vol := range []string{volumeNameMeta, volumeNameData} {
			Expect(pvcExists(ctx, resourceNamespace, fmt.Sprintf("%s-%s-2", vol, ssName))).To(BeTrue())
		}

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

	It("refuses an approved drain when cluster health cannot be confirmed", func() {
		// Regression guard for REVIEW.md #1: a transient Health() error must invalidate the
		// cached status.Health rather than leave the stale "healthy" value in place, otherwise
		// the destructive-drain guardrail acts on stale data and drains a node from a cluster
		// that may have degraded. The approval is already in place, so only the health guardrail
		// can hold the drain back.
		var cluster garagev1alpha1.GarageCluster
		Expect(k8sClient.Get(ctx, key, &cluster)).To(Succeed())
		cluster.Annotations = map[string]string{annotationApproveLayout: "2"}
		Expect(k8sClient.Update(ctx, &cluster)).To(Succeed())

		unhealthy := &GarageClusterReconciler{
			Client: k8sClient,
			Scheme: k8sClient.Scheme(),
			NewAdminClient: func(baseURL, _ string) (clusterAdmin, error) {
				return &fakeClusterAdmin{nodeID: nodeIDFromBaseURL(baseURL), recorder: mesh, layout: layout, healthErr: fmt.Errorf("health endpoint timeout")}, nil
			},
		}

		_, err := unhealthy.Reconcile(ctx, reconcile.Request{NamespacedName: key})
		Expect(err).NotTo(HaveOccurred())

		By("not applying the drain")
		Expect(layout.applyCalls).To(Equal(1))

		By("invalidating the cached health and blocking the change")
		Expect(k8sClient.Get(ctx, key, &cluster)).To(Succeed())
		Expect(cluster.Status.Health).To(BeNil())
		blocked := meta.FindStatusCondition(cluster.Status.Conditions, conditionLayoutChangePending)
		Expect(blocked).NotTo(BeNil())
		Expect(blocked.Status).To(Equal(metav1.ConditionFalse))
		Expect(blocked.Reason).To(Equal(reasonBlocked))
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

var _ = Describe("GarageCluster day-2 ops with one unreachable node", Ordered, func() {
	// Regression guard for REVIEW.md #3: a single unreachable node must not freeze every other
	// day-2 operation. discoverNodes used to bail on the first node whose admin API was down, so
	// Reconcile returned before the layout step and a *different* node the user had removed from
	// spec never got drained. The fix proceeds on the reachable subset while keeping the
	// unreachable-but-still-desired node in the layout set so it is never mistaken for a removal
	// and drained (which would lose redundancy on a node that is merely temporarily down).
	const (
		resourceName      = "unreach"
		resourceNamespace = "default"
		ssName            = "unreach-default"
	)

	ctx := context.Background()
	key := types.NamespacedName{Name: resourceName, Namespace: resourceNamespace}

	layout := newFakeLayout()
	mesh := &meshRecorder{}
	// down is shared across the per-pod clients each reconcile creates, so flipping one entry
	// takes exactly that node's admin API offline mid-suite.
	down := map[string]bool{}
	reconciler := func() *GarageClusterReconciler {
		return &GarageClusterReconciler{
			Client: k8sClient,
			Scheme: k8sClient.Scheme(),
			NewAdminClient: func(baseURL, _ string) (clusterAdmin, error) {
				return &fakeClusterAdmin{nodeID: nodeIDFromBaseURL(baseURL), recorder: mesh, layout: layout, unreachable: down}, nil
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

		By("converging the 3-node cluster to Ready while every node is reachable")
		_, err := reconciler().Reconcile(ctx, reconcile.Request{NamespacedName: key})
		Expect(err).NotTo(HaveOccurred())
		markReady(3)
		_, err = reconciler().Reconcile(ctx, reconcile.Request{NamespacedName: key})
		Expect(err).NotTo(HaveOccurred())
		Expect(layout.applyCalls).To(Equal(1))
		Expect(layout.applied).To(HaveLen(3))

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

	It("drains a removed node while a different node is down, without draining the down node", func() {
		By("taking node-1's admin API offline")
		down[nodeIDFromBaseURL("http://unreach-default-1.")] = true

		By("removing node-2 from spec and pre-approving its drain (target version 2)")
		var cluster garagev1alpha1.GarageCluster
		Expect(k8sClient.Get(ctx, key, &cluster)).To(Succeed())
		cluster.Spec.NodePools[0].Replicas = 2
		cluster.Annotations = map[string]string{annotationApproveLayout: "2"}
		Expect(k8sClient.Update(ctx, &cluster)).To(Succeed())

		_, err := reconciler().Reconcile(ctx, reconcile.Request{NamespacedName: key})
		Expect(err).NotTo(HaveOccurred())

		By("proceeding to the drain on the reachable subset instead of bailing on the down node")
		Expect(layout.applyCalls).To(Equal(2))
		var ss appsv1.StatefulSet
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: ssName, Namespace: resourceNamespace}, &ss)).To(Succeed())
		Expect(*ss.Spec.Replicas).To(Equal(int32(2)))

		By("draining only the removed node's PVCs, keeping the down node's and the survivor's")
		for _, vol := range []string{volumeNameMeta, volumeNameData} {
			Expect(pvcExists(ctx, resourceNamespace, fmt.Sprintf("%s-%s-2", vol, ssName))).To(BeFalse())
			Expect(pvcExists(ctx, resourceNamespace, fmt.Sprintf("%s-%s-1", vol, ssName))).To(BeTrue())
			Expect(pvcExists(ctx, resourceNamespace, fmt.Sprintf("%s-%s-0", vol, ssName))).To(BeTrue())
		}

		By("keeping the unreachable node in the applied layout so it is never treated as a removal")
		Expect(k8sClient.Get(ctx, key, &cluster)).To(Succeed())
		Expect(cluster.Status.Layout.Version).To(Equal(int64(2)))
		Expect(cluster.Status.Layout.Nodes).To(HaveLen(2))
		laidOut := map[string]struct{}{}
		for _, n := range cluster.Status.Layout.Nodes {
			laidOut[n.NodeID] = struct{}{}
		}
		Expect(laidOut).To(HaveKey("id-unreach-default-1"), "the down node must remain in the layout")
		Expect(laidOut).To(HaveKey("id-unreach-default-0"))
		Expect(laidOut).NotTo(HaveKey("id-unreach-default-2"), "only the spec-removed node should be drained")
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
		Expect(cond.Reason).To(Equal(reasonBlocked))
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

var _ = Describe("GarageCluster surplus-workload teardown retried in the additive branch", Ordered, func() {
	// Regression guard for REVIEW.md #6: drained-workload teardown (scaling a shrunk pool down and
	// reclaiming its orphaned PVCs) used to run ONLY right after an approved destructive
	// ApplyLayout. If that teardown failed partway, the next PlanLayout was non-destructive, so
	// reconcileLayout took the additive branch forever and never re-entered teardown — the surplus
	// pods/PVCs leaked permanently. The fix also runs reconcileRemovedWorkload at the end of the
	// additive branch, where it is safe (the applied layout already excludes every spec-surplus
	// node) and idempotent, so a half-finished teardown is retried on an ordinary later pass.
	const (
		resourceName      = "leak"
		resourceNamespace = "default"
		ssName            = "leak-default"
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
		Expect(layout.applied).To(HaveLen(3))

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

	It("scales a leaked surplus pool down and reclaims its PVCs on an ordinary non-destructive pass", func() {
		By("simulating a half-finished teardown: spec is shrunk and node-2 is already out of the layout, while the StatefulSet still runs 3 replicas and node-2's PVCs linger")
		var cluster garagev1alpha1.GarageCluster
		Expect(k8sClient.Get(ctx, key, &cluster)).To(Succeed())
		cluster.Spec.NodePools[0].Replicas = 2
		Expect(k8sClient.Update(ctx, &cluster)).To(Succeed())
		// ensureStatefulSet refuses to scale a live StatefulSet down, so it stays at 3 replicas and
		// still reads as ready; dropping node-2 from the applied layout models the drain that a
		// prior approved pass committed before its workload teardown failed and left the surplus.
		delete(layout.applied, "id-"+ssName+"-2")
		Expect(layout.applied).To(HaveLen(2))

		By("reconciling: PlanLayout sees applied == desired, so this is the non-destructive additive branch")
		applyCallsBefore := layout.applyCalls
		_, err := reconciler().Reconcile(ctx, reconcile.Request{NamespacedName: key})
		Expect(err).NotTo(HaveOccurred())

		By("not staging or applying any layout change (the teardown is workload-only)")
		Expect(layout.applyCalls).To(Equal(applyCallsBefore))

		By("scaling the StatefulSet down to the desired replica count in the additive branch")
		var ss appsv1.StatefulSet
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: ssName, Namespace: resourceNamespace}, &ss)).To(Succeed())
		Expect(*ss.Spec.Replicas).To(Equal(int32(2)))

		By("reclaiming the leaked node's PVCs while keeping the survivors'")
		for _, vol := range []string{volumeNameMeta, volumeNameData} {
			Expect(pvcExists(ctx, resourceNamespace, fmt.Sprintf("%s-%s-2", vol, ssName))).To(BeFalse())
			Expect(pvcExists(ctx, resourceNamespace, fmt.Sprintf("%s-%s-0", vol, ssName))).To(BeTrue())
			Expect(pvcExists(ctx, resourceNamespace, fmt.Sprintf("%s-%s-1", vol, ssName))).To(BeTrue())
		}

		By("reporting the converged 2-node layout and staying Ready")
		Expect(k8sClient.Get(ctx, key, &cluster)).To(Succeed())
		Expect(cluster.Status.Layout.Nodes).To(HaveLen(2))
		Expect(meta.IsStatusConditionTrue(cluster.Status.Conditions, conditionReady)).To(BeTrue())
	})
})

var _ = Describe("GarageCluster blocked storage migration does not freeze unrelated ops", Ordered, func() {
	// Regression guard for REVIEW.md #5: a storage migration refused by a guardrail used to return
	// active=true, so Reconcile reported Ready/StorageMigrating and returned BEFORE the generic
	// layout/zone/maintenance steps — freezing every unrelated cluster operation until the user
	// reverted the impossible storage edit. The fix pins the blocked pool's capacity to its current
	// applied value and lets the reconcile fall through, so only the affected pool's capacity change
	// is suppressed. The pin is the safety half: reconcileLayout must NOT advertise the pool's new
	// (unmigrated) size to Garage — a blocked grow would otherwise claim more disk than the node has.
	const (
		resourceName      = "smblock"
		resourceNamespace = "default"
		ssName            = "smblock-default"
		nodeID            = "id-smblock-default-0"
		podZeroName       = "smblock-default-0"
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

	markReady := func() {
		var ss appsv1.StatefulSet
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: ssName, Namespace: resourceNamespace}, &ss)).To(Succeed())
		ss.Status.ObservedGeneration = ss.Generation
		ss.Status.Replicas = 1
		ss.Status.ReadyReplicas = 1
		Expect(k8sClient.Status().Update(ctx, &ss)).To(Succeed())
	}

	BeforeAll(func() {
		// A single-node rf=1 cluster: rf<2 is exactly the guardrail that refuses a migration drain.
		resource := &garagev1alpha1.GarageCluster{
			ObjectMeta: metav1.ObjectMeta{Name: resourceName, Namespace: resourceNamespace},
			Spec:       newBasicClusterSpec(1, 1),
		}
		Expect(k8sClient.Create(ctx, resource)).To(Succeed())

		By("converging the cluster to Ready with a 1Gi data volume laid out")
		_, err := reconciler().Reconcile(ctx, reconcile.Request{NamespacedName: key})
		Expect(err).NotTo(HaveOccurred())
		markReady()
		_, err = reconciler().Reconcile(ctx, reconcile.Request{NamespacedName: key})
		Expect(err).NotTo(HaveOccurred())
		Expect(layout.applyCalls).To(Equal(1))
		var cluster garagev1alpha1.GarageCluster
		Expect(k8sClient.Get(ctx, key, &cluster)).To(Succeed())
		Expect(cluster.Status.Layout.Nodes).To(HaveLen(1))
		Expect(cluster.Status.Layout.Nodes[0].Capacity.Cmp(apiresource.MustParse("1Gi"))).To(Equal(0))

		By("standing up the live PVCs the StatefulSet controller would have created (at 1Gi)")
		for _, vol := range []string{volumeNameMeta, volumeNameData} {
			createPVC(ctx, resourceNamespace, fmt.Sprintf("%s-%s-0", vol, ssName))
		}
	})

	AfterAll(func() {
		resource := &garagev1alpha1.GarageCluster{}
		Expect(k8sClient.Get(ctx, key, resource)).To(Succeed())
		Expect(k8sClient.Delete(ctx, resource)).To(Succeed())
	})

	It("applies an unrelated zone-redundancy change while refusing the impossible shrink", func() {
		By("shrinking the data volume (selects a migration the rf=1 guardrail refuses) and changing zone redundancy in the same edit")
		var cluster garagev1alpha1.GarageCluster
		Expect(k8sClient.Get(ctx, key, &cluster)).To(Succeed())
		cluster.Spec.NodePools[0].Storage.Data.Size = apiresource.MustParse("512Mi")
		cluster.Spec.ZoneRedundancy = &garagev1alpha1.ZoneRedundancy{
			Mode: garagev1alpha1.ZoneRedundancyAtLeast, AtLeast: 1,
		}
		Expect(k8sClient.Update(ctx, &cluster)).To(Succeed())

		redundancyBefore := layout.redundancyCalls
		applyBefore := layout.applyCalls
		_, err := reconciler().Reconcile(ctx, reconcile.Request{NamespacedName: key})
		Expect(err).NotTo(HaveOccurred())

		By("refusing the shrink: StorageChangePending=False/Blocked")
		Expect(k8sClient.Get(ctx, key, &cluster)).To(Succeed())
		blocked := meta.FindStatusCondition(cluster.Status.Conditions, conditionStorageChangePending)
		Expect(blocked).NotTo(BeNil())
		Expect(blocked.Status).To(Equal(metav1.ConditionFalse))
		Expect(blocked.Reason).To(Equal(reasonBlocked))

		By("no longer freezing the reconcile: the unrelated zone-redundancy change is applied")
		Expect(layout.redundancyCalls).To(Equal(redundancyBefore + 1))

		By("not applying any layout change for the blocked pool")
		Expect(layout.applyCalls).To(Equal(applyBefore))

		By("pinning the blocked pool's layout capacity to its current 1Gi value, not advancing it to the new 512Mi")
		Expect(cluster.Status.Layout.Nodes).To(HaveLen(1))
		Expect(cluster.Status.Layout.Nodes[0].Pod).To(Equal(podZeroName))
		Expect(cluster.Status.Layout.Nodes[0].Capacity.Cmp(apiresource.MustParse("1Gi"))).To(Equal(0))

		By("advertising the pinned 1Gi capacity to Garage's PlanLayout, never the unmigrated 512Mi")
		var role *garageadmin.DesiredRole
		for i := range layout.lastDesired {
			if layout.lastDesired[i].NodeID == nodeID {
				role = &layout.lastDesired[i]
			}
		}
		Expect(role).NotTo(BeNil())
		oneGi := apiresource.MustParse("1Gi")
		Expect(role.Capacity).To(Equal(oneGi.Value()))

		By("keeping the cluster Ready: it is healthy, only the storage edit is refused")
		Expect(meta.IsStatusConditionTrue(cluster.Status.Conditions, conditionReady)).To(BeTrue())
	})
})
