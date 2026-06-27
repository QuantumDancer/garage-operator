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
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	garagev1alpha1 "github.com/QuantumDancer/garage-operator/api/v1alpha1"
	"github.com/QuantumDancer/garage-operator/internal/garageadmin"
)

// Condition types reported on GarageCluster status.
const (
	conditionReady               = "Ready"
	conditionWorkloadReady       = "WorkloadReady"
	conditionPeersConnected      = "PeersConnected"
	conditionLayoutApplied       = "LayoutApplied"
	conditionLayoutChangePending = "LayoutChangePending"
)

// annotationApproveLayout gates a destructive layout change (node drain/removal). Set its
// value to the pending target layout version reported in the LayoutChangePending condition
// to authorize the operator to apply the drain. Keying approval to the version means a stale
// approval is ignored once the desired state — and thus the target version — changes again.
const annotationApproveLayout = "garage.rottler.io/approve-layout"

// workloadRequeue is how long to wait before re-checking pods/admin API while the cluster
// converges. Garage convergence emits no Kubernetes events, so these steps poll.
const workloadRequeue = 10 * time.Second

// steadyStateRequeue re-reconciles a healthy, converged cluster periodically. Garage-internal
// state (partition quorum, node connectivity) changes without any Kubernetes event and is not
// covered by the Owns() watches, so without a steady-state requeue status.health would go
// stale until an unrelated change happened to trigger a reconcile.
const steadyStateRequeue = time.Minute

// clusterAdmin is the slice of the Garage Admin API the cluster controller needs. It is an
// interface so reconcile logic can be exercised against a fake in tests.
type clusterAdmin interface {
	NodeID(ctx context.Context) (string, error)
	ConnectNodes(ctx context.Context, peers []string) error
	PlanLayout(ctx context.Context, desired []garageadmin.DesiredRole) (*garageadmin.LayoutPlan, error)
	StageLayoutChanges(ctx context.Context, changes []garageadmin.NodeRoleChangeRequest) error
	PreviewStagedChanges(ctx context.Context) ([]string, error)
	ApplyLayout(ctx context.Context, version int64) error
	RevertStagedChanges(ctx context.Context) error
	Health(ctx context.Context) (*garageadmin.GetClusterHealthResponse, error)
}

// GarageClusterReconciler reconciles a GarageCluster object
type GarageClusterReconciler struct {
	client.Client
	Scheme *runtime.Scheme

	// NewAdminClient builds an admin client for a single node's endpoint. Defaulted to the
	// real Garage Admin API client; overridden in tests.
	NewAdminClient func(baseURL, token string) (clusterAdmin, error)

	// Recorder emits Events onto the CR (e.g. that a destructive layout change is awaiting
	// approval) so the reason is visible in `kubectl describe`, not just a status condition.
	Recorder record.EventRecorder
}

func defaultAdminClientFactory(baseURL, token string) (clusterAdmin, error) {
	return garageadmin.NewAdminClient(baseURL, token)
}

// +kubebuilder:rbac:groups=garage.rottler.io,resources=garageclusters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=garage.rottler.io,resources=garageclusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=garage.rottler.io,resources=garageclusters/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=services;configmaps;secrets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=pods,verbs=get;list;watch
// +kubebuilder:rbac:groups=core,resources=persistentvolumeclaims,verbs=get;list;watch;delete
// +kubebuilder:rbac:groups=core,resources=events,verbs=create;patch

// Reconcile drives the observed cluster toward the GarageCluster spec: it provisions the
// workload (Secrets, ConfigMap, Services, StatefulSets) and, once pods are reachable, forms
// the layout through the Admin API. It is level-triggered and converges each concern
// independently, so a partial failure on one run is repaired on the next.
func (r *GarageClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	if r.NewAdminClient == nil {
		r.NewAdminClient = defaultAdminClientFactory
	}

	var cluster garagev1alpha1.GarageCluster
	if err := r.Get(ctx, req.NamespacedName, &cluster); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	status := cluster.Status.DeepCopy()
	status.ObservedGeneration = cluster.Generation
	status.AdminTokenSecret = resolveAdminTokenSecret(&cluster).name
	status.RpcTokenSecret = resolveRpcSecret(&cluster).name
	status.Endpoints = &garagev1alpha1.EndpointsStatus{
		S3Api: endpointURL(s3ServiceName(&cluster), cluster.Namespace, portS3),
		Web:   endpointURL(webServiceName(&cluster), cluster.Namespace, portWeb),
	}

	if err := r.ensureWorkload(ctx, &cluster); err != nil {
		return ctrl.Result{}, err
	}

	ready, err := r.workloadReady(ctx, &cluster)
	if err != nil {
		return ctrl.Result{}, err
	}
	if !ready {
		log.Info("Waiting for Garage pods to become ready")
		setCondition(status, conditionWorkloadReady, metav1.ConditionFalse, "PodsNotReady", "Waiting for Garage pods to become ready")
		setCondition(status, conditionReady, metav1.ConditionFalse, "WorkloadNotReady", "Workload is not ready")
		return r.finish(ctx, &cluster, status, ctrl.Result{RequeueAfter: workloadRequeue})
	}
	setCondition(status, conditionWorkloadReady, metav1.ConditionTrue, "PodsReady", "All Garage pods are ready")

	adminToken, err := r.adminTokenValue(ctx, &cluster)
	if err != nil {
		return ctrl.Result{}, err
	}

	desired, layoutClient, err := r.discoverNodes(ctx, &cluster, adminToken)
	if err != nil {
		log.Info("Waiting for Garage admin API to become reachable", "reason", err.Error())
		setCondition(status, conditionPeersConnected, metav1.ConditionFalse, "NodesNotReachable", "Waiting for Garage admin API")
		setCondition(status, conditionReady, metav1.ConditionFalse, "NodesNotReachable", "Garage nodes are not reachable yet")
		return r.finish(ctx, &cluster, status, ctrl.Result{RequeueAfter: workloadRequeue})
	}
	// Form the RPC mesh before applying layout: UpdateClusterLayout can only assign roles to
	// node ids the serving node has connected to, so an unmeshed cluster cannot be laid out.
	if peers := peerConnectStrings(&cluster, desired); len(peers) > 0 {
		if err := layoutClient.ConnectNodes(ctx, peers); err != nil {
			log.Info("Waiting for Garage nodes to peer", "reason", err.Error())
			setCondition(status, conditionPeersConnected, metav1.ConditionFalse, "ConnectFailed", "Waiting for Garage nodes to peer")
			setCondition(status, conditionReady, metav1.ConditionFalse, "ConnectFailed", "Garage nodes are not peered yet")
			return r.finish(ctx, &cluster, status, ctrl.Result{RequeueAfter: workloadRequeue})
		}
		setCondition(status, conditionPeersConnected, metav1.ConditionTrue, "NodesConnected", "Garage nodes are connected into a mesh")
	} else {
		setCondition(status, conditionPeersConnected, metav1.ConditionTrue, "SingleNode", "Single-node cluster needs no peering")
	}

	// Refresh health before the layout step: the destructive-layout guardrail refuses to
	// drain a node while the cluster is unhealthy.
	if health, herr := layoutClient.Health(ctx); herr == nil {
		status.Health = buildHealthStatus(health)
	}

	if err := r.reconcileLayout(ctx, &cluster, status, layoutClient, desired); err != nil {
		setCondition(status, conditionReady, metav1.ConditionFalse, "LayoutError", err.Error())
		return ctrl.Result{}, err
	}

	setCondition(status, conditionReady, metav1.ConditionTrue, "ClusterReady", "Garage cluster is ready")
	return r.finish(ctx, &cluster, status, ctrl.Result{RequeueAfter: steadyStateRequeue})
}

// ensureWorkload converges every in-cluster child resource. Each concern is independent so a
// failure part-way leaves the rest to be reconciled on the next run.
func (r *GarageClusterReconciler) ensureWorkload(ctx context.Context, cluster *garagev1alpha1.GarageCluster) error {
	if err := r.ensureBootstrapSecret(ctx, cluster, cluster.Spec.RpcSecret, resolveRpcSecret(cluster).name); err != nil {
		return err
	}
	if err := r.ensureBootstrapSecret(ctx, cluster, cluster.Spec.AdminToken, resolveAdminTokenSecret(cluster).name); err != nil {
		return err
	}
	if err := r.ensureConfigMap(ctx, cluster); err != nil {
		return err
	}
	for _, svc := range []*corev1.Service{
		desiredHeadlessService(cluster),
		desiredS3Service(cluster),
		desiredWebService(cluster),
	} {
		if err := r.ensureService(ctx, cluster, svc); err != nil {
			return err
		}
	}
	for i := range cluster.Spec.NodePools {
		if err := r.ensureStatefulSet(ctx, cluster, &cluster.Spec.NodePools[i]); err != nil {
			return err
		}
	}
	return nil
}

// ensureBootstrapSecret creates the operator-managed Secret for a generated token when it is
// absent. It never overwrites an existing Secret, so the generated value persists across
// reconciles (Garage only reveals it once). Provided secrets are owned by the user and left
// untouched.
func (r *GarageClusterReconciler) ensureBootstrapSecret(ctx context.Context, cluster *garagev1alpha1.GarageCluster, bootstrap garagev1alpha1.SecretBootstrap, name string) error {
	if bootstrap.Mode == garagev1alpha1.SecretBootstrapProvided {
		return nil
	}
	var existing corev1.Secret
	err := r.Get(ctx, client.ObjectKey{Name: name, Namespace: cluster.Namespace}, &existing)
	if err == nil {
		return nil
	}
	if !apierrors.IsNotFound(err) {
		return err
	}
	token, err := generateToken()
	if err != nil {
		return err
	}
	secret := desiredGeneratedSecret(cluster, name, token)
	if err := ctrl.SetControllerReference(cluster, secret, r.Scheme); err != nil {
		return err
	}
	return client.IgnoreAlreadyExists(r.Create(ctx, secret))
}

func (r *GarageClusterReconciler) ensureConfigMap(ctx context.Context, cluster *garagev1alpha1.GarageCluster) error {
	desired := desiredConfigMap(cluster)
	if err := ctrl.SetControllerReference(cluster, desired, r.Scheme); err != nil {
		return err
	}
	var existing corev1.ConfigMap
	err := r.Get(ctx, client.ObjectKeyFromObject(desired), &existing)
	if apierrors.IsNotFound(err) {
		return r.Create(ctx, desired)
	}
	if err != nil {
		return err
	}
	if apiequality.Semantic.DeepEqual(existing.Data, desired.Data) {
		return nil
	}
	existing.Data = desired.Data
	existing.Labels = desired.Labels
	return r.Update(ctx, &existing)
}

// ensureService creates the Service if absent, otherwise reconciles only the mutable fields.
// ClusterIP and other API-server-assigned fields on the existing object are preserved.
func (r *GarageClusterReconciler) ensureService(ctx context.Context, cluster *garagev1alpha1.GarageCluster, desired *corev1.Service) error {
	if err := ctrl.SetControllerReference(cluster, desired, r.Scheme); err != nil {
		return err
	}
	var existing corev1.Service
	err := r.Get(ctx, client.ObjectKeyFromObject(desired), &existing)
	if apierrors.IsNotFound(err) {
		return r.Create(ctx, desired)
	}
	if err != nil {
		return err
	}
	if apiequality.Semantic.DeepEqual(existing.Spec.Ports, desired.Spec.Ports) &&
		apiequality.Semantic.DeepEqual(existing.Spec.Selector, desired.Spec.Selector) &&
		existing.Spec.Type == desired.Spec.Type {
		return nil
	}
	existing.Spec.Ports = desired.Spec.Ports
	existing.Spec.Selector = desired.Spec.Selector
	existing.Spec.Type = desired.Spec.Type
	return r.Update(ctx, &existing)
}

// ensureStatefulSet creates the StatefulSet if absent, otherwise updates only the mutable
// fields (replicas and pod template). The selector, volumeClaimTemplates, and serviceName are
// immutable on a live StatefulSet, so they are set once at creation and never patched.
func (r *GarageClusterReconciler) ensureStatefulSet(ctx context.Context, cluster *garagev1alpha1.GarageCluster, pool *garagev1alpha1.NodePool) error {
	desired := desiredStatefulSet(cluster, pool)
	if err := ctrl.SetControllerReference(cluster, desired, r.Scheme); err != nil {
		return err
	}
	var existing appsv1.StatefulSet
	err := r.Get(ctx, client.ObjectKeyFromObject(desired), &existing)
	if apierrors.IsNotFound(err) {
		return r.Create(ctx, desired)
	}
	if err != nil {
		return err
	}
	// Never scale a StatefulSet down here. A replica reduction is a destructive layout
	// change: the nodes must first be drained from the Garage layout so their data is
	// redistributed to replicas. The gated drain path (reconcileRemovedWorkload) owns
	// scale-down; here we only ever create, scale up, or roll the pod template.
	desiredReplicas := desired.Spec.Replicas
	if existing.Spec.Replicas != nil && desiredReplicas != nil && *desiredReplicas < *existing.Spec.Replicas {
		desiredReplicas = existing.Spec.Replicas
	}
	if equalInt32Ptr(existing.Spec.Replicas, desiredReplicas) &&
		apiequality.Semantic.DeepEqual(existing.Spec.Template, desired.Spec.Template) {
		return nil
	}
	existing.Spec.Replicas = desiredReplicas
	existing.Spec.Template = desired.Spec.Template
	return r.Update(ctx, &existing)
}

// workloadReady reports whether every pool's StatefulSet has all of its current replicas
// ready for its current generation. Readiness is measured against each StatefulSet's own
// replica count, not the pool's desired count: during a gated scale-down the StatefulSet
// deliberately runs more replicas than spec until the drain is applied, and those extra pods
// must still count as ready for the drain to proceed.
func (r *GarageClusterReconciler) workloadReady(ctx context.Context, cluster *garagev1alpha1.GarageCluster) (bool, error) {
	for i := range cluster.Spec.NodePools {
		pool := &cluster.Spec.NodePools[i]
		var ss appsv1.StatefulSet
		key := client.ObjectKey{Name: statefulSetName(cluster, pool), Namespace: cluster.Namespace}
		if err := r.Get(ctx, key, &ss); err != nil {
			if apierrors.IsNotFound(err) {
				return false, nil
			}
			return false, err
		}
		want := int32(0)
		if ss.Spec.Replicas != nil {
			want = *ss.Spec.Replicas
		}
		if ss.Status.ObservedGeneration != ss.Generation || ss.Status.ReadyReplicas != want {
			return false, nil
		}
	}
	return true, nil
}

// nodeEndpoint is a single Garage node and the layout role the operator wants for it.
type nodeEndpoint struct {
	pod      string
	nodeID   string
	zone     string
	capacity resource.Quantity
}

// discoverNodes resolves each pod's Garage node id over headless per-pod DNS and returns the
// desired layout plus a client to use for layout operations (the layout is cluster-wide, so
// any reachable node serves). An error means a node is not reachable yet — the caller treats
// that as "requeue", not a failure.
func (r *GarageClusterReconciler) discoverNodes(ctx context.Context, cluster *garagev1alpha1.GarageCluster, token string) ([]nodeEndpoint, clusterAdmin, error) {
	var nodes []nodeEndpoint
	var layoutClient clusterAdmin

	for i := range cluster.Spec.NodePools {
		pool := &cluster.Spec.NodePools[i]
		zone := pool.Zone
		if zone == "" {
			zone = pool.Name
		}
		for ordinal := int32(0); ordinal < pool.Replicas; ordinal++ {
			podName := fmt.Sprintf("%s-%d", statefulSetName(cluster, pool), ordinal)
			baseURL := fmt.Sprintf("http://%s.%s.%s.svc:%d", podName, headlessServiceName(cluster), cluster.Namespace, portAdmin)
			admin, err := r.NewAdminClient(baseURL, token)
			if err != nil {
				return nil, nil, err
			}
			nodeID, err := admin.NodeID(ctx)
			if err != nil {
				return nil, nil, fmt.Errorf("node %s: %w", podName, err)
			}
			if layoutClient == nil {
				layoutClient = admin
			}
			nodes = append(nodes, nodeEndpoint{
				pod:      podName,
				nodeID:   nodeID,
				zone:     zone,
				capacity: pool.Storage.Data.Size,
			})
		}
	}
	if layoutClient == nil {
		return nil, nil, fmt.Errorf("no nodes discovered")
	}
	return nodes, layoutClient, nil
}

func (r *GarageClusterReconciler) adminTokenValue(ctx context.Context, cluster *garagev1alpha1.GarageCluster) (string, error) {
	ref := resolveAdminTokenSecret(cluster)
	var secret corev1.Secret
	if err := r.Get(ctx, client.ObjectKey{Name: ref.name, Namespace: cluster.Namespace}, &secret); err != nil {
		return "", err
	}
	value, ok := secret.Data[ref.key]
	if !ok {
		return "", fmt.Errorf("admin token Secret %q is missing key %q", ref.name, ref.key)
	}
	return string(value), nil
}

// finish writes status only when it actually changed, avoiding a status hot-loop, then
// returns the supplied result.
func (r *GarageClusterReconciler) finish(ctx context.Context, cluster *garagev1alpha1.GarageCluster, status *garagev1alpha1.GarageClusterStatus, result ctrl.Result) (ctrl.Result, error) {
	if apiequality.Semantic.DeepEqual(&cluster.Status, status) {
		return result, nil
	}
	cluster.Status = *status
	if err := r.Status().Update(ctx, cluster); err != nil {
		return ctrl.Result{}, err
	}
	return result, nil
}

// peerConnectStrings builds the Garage connect strings ("<nodeID>@<host>:<rpcPort>") the
// operator hands to ConnectNodes. The first node backs the layout client and is the one we
// connect *from*, so it is excluded — connecting it to every other node is enough for gossip
// to propagate full membership. Returns nil for a single-node cluster (nothing to peer).
func peerConnectStrings(cluster *garagev1alpha1.GarageCluster, nodes []nodeEndpoint) []string {
	if len(nodes) < 2 {
		return nil
	}
	peers := make([]string, 0, len(nodes)-1)
	for _, n := range nodes[1:] {
		host := fmt.Sprintf("%s.%s.%s.svc", n.pod, headlessServiceName(cluster), cluster.Namespace)
		peers = append(peers, fmt.Sprintf("%s@%s:%d", n.nodeID, host, portRPC))
	}
	return peers
}

func desiredRoles(nodes []nodeEndpoint) []garageadmin.DesiredRole {
	roles := make([]garageadmin.DesiredRole, 0, len(nodes))
	for _, n := range nodes {
		roles = append(roles, garageadmin.DesiredRole{
			NodeID:   n.nodeID,
			Zone:     n.zone,
			Capacity: n.capacity.Value(),
		})
	}
	return roles
}

func buildLayoutStatus(nodes []nodeEndpoint, version int64) *garagev1alpha1.LayoutStatus {
	out := &garagev1alpha1.LayoutStatus{Version: version}
	for _, n := range nodes {
		out.Nodes = append(out.Nodes, garagev1alpha1.LayoutNodeStatus{
			NodeID:   n.nodeID,
			Pod:      n.pod,
			Zone:     n.zone,
			Capacity: n.capacity,
			Role:     "active",
		})
	}
	return out
}

func buildHealthStatus(h *garageadmin.GetClusterHealthResponse) *garagev1alpha1.HealthStatus {
	return &garagev1alpha1.HealthStatus{
		Status:           h.Status,
		ConnectedNodes:   h.ConnectedNodes,
		KnownNodes:       h.KnownNodes,
		PartitionsQuorum: fmt.Sprintf("%d/%d", h.PartitionsQuorum, h.Partitions),
	}
}

func setCondition(status *garagev1alpha1.GarageClusterStatus, condType string, s metav1.ConditionStatus, reason, message string) {
	meta.SetStatusCondition(&status.Conditions, metav1.Condition{
		Type:               condType,
		Status:             s,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: status.ObservedGeneration,
	})
}

func endpointURL(service, namespace string, port int) string {
	return fmt.Sprintf("http://%s.%s.svc:%d", service, namespace, port)
}

func equalInt32Ptr(a, b *int32) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

// generateToken returns 32 bytes of cryptographic randomness as a hex string, suitable for
// both the Garage RPC secret (which requires 32 bytes hex) and the admin token.
func generateToken() (string, error) {
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf), nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *GarageClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.NewAdminClient == nil {
		r.NewAdminClient = defaultAdminClientFactory
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&garagev1alpha1.GarageCluster{}).
		Owns(&appsv1.StatefulSet{}).
		Owns(&corev1.Service{}).
		Owns(&corev1.ConfigMap{}).
		Owns(&corev1.Secret{}).
		Named("garagecluster").
		Complete(r)
}
