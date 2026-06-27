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
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	garagev1alpha1 "github.com/QuantumDancer/garage-operator/api/v1alpha1"
	"github.com/QuantumDancer/garage-operator/internal/garageadmin"
)

// migrationWorld assembles the world the migration state machine reads: a cluster whose spec
// asks for desiredData (meta fixed at 1Gi), a StatefulSet whose volumeClaimTemplate provisions
// templateData, and per-pod PVCs at pvcData. Separating the three sizes lets a test stage any
// point of the recreate dance — a stale template, an un-swapped PVC, or a fully swapped node.
func migrationWorld(t *testing.T, desiredData, templateData, pvcData string, replicas, rf int32, expandable bool) (*GarageClusterReconciler, client.Client, *garagev1alpha1.GarageCluster, []nodeEndpoint) {
	t.Helper()
	cluster := &garagev1alpha1.GarageCluster{
		ObjectMeta: metav1.ObjectMeta{Name: testClusterName, Namespace: testClusterNS},
		Spec: garagev1alpha1.GarageClusterSpec{
			ReplicationFactor: rf,
			NodePools: []garagev1alpha1.NodePool{{
				Name:     testPoolName,
				Replicas: replicas,
				Storage: garagev1alpha1.NodePoolStorage{
					Data: garagev1alpha1.StorageSpec{Size: resource.MustParse(desiredData)},
					Meta: garagev1alpha1.StorageSpec{Size: resource.MustParse("1Gi")},
				},
			}},
		},
	}

	templatePool := cluster.Spec.NodePools[0]
	templatePool.Storage.Data = garagev1alpha1.StorageSpec{Size: resource.MustParse(templateData)}
	templatePool.Storage.Meta = garagev1alpha1.StorageSpec{Size: resource.MustParse("1Gi")}
	ss := desiredStatefulSet(cluster, &templatePool)

	objs := make([]client.Object, 0, 2+2*replicas)
	objs = append(objs, ss, &storagev1.StorageClass{
		ObjectMeta:           metav1.ObjectMeta{Name: expandableStorageClass},
		AllowVolumeExpansion: ptr.To(expandable),
	})
	for ordinal := range replicas {
		objs = append(objs,
			storageClaim(ss.Name, volumeNameData, ordinal, pvcData),
			storageClaim(ss.Name, volumeNameMeta, ordinal, "1Gi"),
		)
	}

	r, c := newStorageReconciler(t, objs...)
	return r, c, cluster, migrationNodes(replicas, desiredData)
}

// migrationNodes builds the discovered-node list for a pool, one endpoint per ordinal with a
// stable node id ("node-<ordinal>") and the post-migration capacity.
func migrationNodes(replicas int32, desiredData string) []nodeEndpoint {
	nodes := make([]nodeEndpoint, 0, replicas)
	for ordinal := range replicas {
		nodes = append(nodes, nodeEndpoint{
			pod:      podName(ssName(), ordinal),
			nodeID:   fmt.Sprintf("node-%d", ordinal),
			zone:     testPoolName,
			capacity: resource.MustParse(desiredData),
		})
	}
	return nodes
}

// seededLayout returns a fake layout whose applied set already holds every pool node, as a
// converged cluster would before a migration drains one.
func seededLayout(replicas int32) *fakeLayout {
	l := newFakeLayout()
	for ordinal := range replicas {
		l.applied[fmt.Sprintf("node-%d", ordinal)] = struct{}{}
	}
	return l
}

func healthyStatus() *garagev1alpha1.GarageClusterStatus {
	return &garagev1alpha1.GarageClusterStatus{
		Health: &garagev1alpha1.HealthStatus{Status: healthStatusHealthy},
	}
}

func health(status string, allOk int) *garageadmin.GetClusterHealthResponse {
	return &garageadmin.GetClusterHealthResponse{
		Status: status, Partitions: 256, PartitionsQuorum: 256, PartitionsAllOk: allOk,
	}
}

// TestMigrationDrainStartsShrink proves a shrink starts the migration: the first node is
// drained from the layout and the migration advances to wait for re-replication.
func TestMigrationDrainStartsShrink(t *testing.T) {
	r, _, cluster, desired := migrationWorld(t, "1Gi", "2Gi", "2Gi", 2, 2, true)
	layout := seededLayout(2)
	admin := &fakeClusterAdmin{layout: layout, health: health(healthStatusHealthy, 256)}
	status := healthyStatus()

	active, err := r.reconcileStorageMigration(context.Background(), cluster, status, admin, desired)
	if err != nil {
		t.Fatalf("reconcileStorageMigration: %v", err)
	}
	if !active {
		t.Fatal("expected active=true while a shrink migration is in progress")
	}
	if _, in := layout.applied["node-0"]; in {
		t.Error("expected node-0 to be drained from the layout")
	}
	m := status.StorageMigration
	if m == nil || m.Ordinal != 0 || m.Phase != garagev1alpha1.StorageMigrationAwaitingReplication {
		t.Fatalf("storageMigration = %+v, want ordinal 0 phase AwaitingReplication", m)
	}
}

// TestMigrationBlockedBelowReplicationFactor proves a migration is refused — not started — when
// the replication factor leaves no replica to recover a drained node's data.
func TestMigrationBlockedBelowReplicationFactor(t *testing.T) {
	r, _, cluster, desired := migrationWorld(t, "1Gi", "2Gi", "2Gi", 1, 1, true)
	layout := seededLayout(1)
	admin := &fakeClusterAdmin{layout: layout, health: health(healthStatusHealthy, 256)}
	status := healthyStatus()

	active, err := r.reconcileStorageMigration(context.Background(), cluster, status, admin, desired)
	if err != nil {
		t.Fatalf("reconcileStorageMigration: %v", err)
	}
	if !active {
		t.Fatal("expected active=true so the generic layout path stays suppressed while blocked")
	}
	if _, in := layout.applied["node-0"]; !in {
		t.Error("a blocked migration must not drain any node")
	}
	if status.StorageMigration != nil {
		t.Error("a blocked migration records no in-progress node")
	}
	cond := meta.FindStatusCondition(status.Conditions, conditionStorageChangePending)
	if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != "Blocked" {
		t.Fatalf("expected StorageChangePending=False/Blocked, got %+v", cond)
	}
}

// TestMigrationBlockedWhenUnhealthy proves a migration will not start while the cluster is
// already degraded: draining would reduce redundancy further.
func TestMigrationBlockedWhenUnhealthy(t *testing.T) {
	r, _, cluster, desired := migrationWorld(t, "1Gi", "2Gi", "2Gi", 2, 2, true)
	layout := seededLayout(2)
	admin := &fakeClusterAdmin{layout: layout, health: health("degraded", 200)}
	status := &garagev1alpha1.GarageClusterStatus{Health: &garagev1alpha1.HealthStatus{Status: "degraded"}}

	active, err := r.reconcileStorageMigration(context.Background(), cluster, status, admin, desired)
	if err != nil {
		t.Fatalf("reconcileStorageMigration: %v", err)
	}
	if !active {
		t.Fatal("expected active=true while a migration is pending")
	}
	if _, in := layout.applied["node-0"]; !in {
		t.Error("an unhealthy cluster must not be drained")
	}
	cond := meta.FindStatusCondition(status.Conditions, conditionStorageChangePending)
	if cond == nil || cond.Reason != "Blocked" {
		t.Fatalf("expected StorageChangePending Blocked, got %+v", cond)
	}
}

// TestMigrationAwaitReplication proves the wait holds until every partition is fully replicated
// (partitionsAllOk == partitions) before advancing to recreate the volume.
func TestMigrationAwaitReplication(t *testing.T) {
	r, _, cluster, desired := migrationWorld(t, "1Gi", "2Gi", "2Gi", 2, 2, true)
	admin := &fakeClusterAdmin{layout: seededLayout(2), health: health("degraded", 200)}
	status := healthyStatus()
	status.StorageMigration = &garagev1alpha1.StorageMigrationStatus{
		Pool: testPoolName, Ordinal: 0, Phase: garagev1alpha1.StorageMigrationAwaitingReplication,
	}

	active, err := r.reconcileStorageMigration(context.Background(), cluster, status, admin, desired)
	if err != nil {
		t.Fatalf("reconcileStorageMigration: %v", err)
	}
	if !active || status.StorageMigration.Phase != garagev1alpha1.StorageMigrationAwaitingReplication {
		t.Fatalf("expected to stay in AwaitingReplication while not fully replicated, got %+v", status.StorageMigration)
	}

	// Once fully replicated, it advances to recreate the volume.
	admin.health = health(healthStatusHealthy, 256)
	if _, err := r.reconcileStorageMigration(context.Background(), cluster, status, admin, desired); err != nil {
		t.Fatalf("reconcileStorageMigration: %v", err)
	}
	if status.StorageMigration.Phase != garagev1alpha1.StorageMigrationRecreatingVolume {
		t.Fatalf("phase = %s, want RecreatingVolume once fully replicated", status.StorageMigration.Phase)
	}
}

// TestMigrationRecreateOrphanDeletesStaleTemplate proves the recreate phase first rebuilds the
// StatefulSet when its template still provisions the old size.
func TestMigrationRecreateOrphanDeletesStaleTemplate(t *testing.T) {
	r, c, cluster, desired := migrationWorld(t, "1Gi", "2Gi", "2Gi", 2, 2, true)
	admin := &fakeClusterAdmin{layout: seededLayout(2), health: health(healthStatusHealthy, 256)}
	status := healthyStatus()
	status.StorageMigration = &garagev1alpha1.StorageMigrationStatus{
		Pool: testPoolName, Ordinal: 0, Phase: garagev1alpha1.StorageMigrationRecreatingVolume,
	}

	active, err := r.reconcileStorageMigration(context.Background(), cluster, status, admin, desired)
	if err != nil {
		t.Fatalf("reconcileStorageMigration: %v", err)
	}
	if !active {
		t.Fatal("expected active=true")
	}
	var ss appsv1.StatefulSet
	if err := c.Get(context.Background(), types.NamespacedName{Name: ssName(), Namespace: testClusterNS}, &ss); !apierrors.IsNotFound(err) {
		t.Errorf("expected the stale-template StatefulSet to be orphan-deleted, got err=%v", err)
	}
}

// TestMigrationRecreateSwapsVolume proves that, with the template already updated, the node's
// pod and PVCs are deleted so the StatefulSet recreates them at the new size.
func TestMigrationRecreateSwapsVolume(t *testing.T) {
	// template already 1Gi, but the per-pod PVCs are still the old 2Gi.
	r, c, cluster, desired := migrationWorld(t, "1Gi", "1Gi", "2Gi", 2, 2, true)
	admin := &fakeClusterAdmin{layout: seededLayout(2), health: health(healthStatusHealthy, 256)}
	status := healthyStatus()
	status.StorageMigration = &garagev1alpha1.StorageMigrationStatus{
		Pool: testPoolName, Ordinal: 0, Phase: garagev1alpha1.StorageMigrationRecreatingVolume,
	}

	if _, err := r.reconcileStorageMigration(context.Background(), cluster, status, admin, desired); err != nil {
		t.Fatalf("reconcileStorageMigration: %v", err)
	}

	// Ordinal 0's claims are deleted; ordinal 1's are left intact (one node at a time).
	for _, vol := range []string{volumeNameData, volumeNameMeta} {
		var pvc corev1.PersistentVolumeClaim
		err := c.Get(context.Background(), types.NamespacedName{Name: claimName(vol, ssName(), 0), Namespace: testClusterNS}, &pvc)
		if !apierrors.IsNotFound(err) {
			t.Errorf("expected ordinal-0 %s claim deleted, got err=%v", vol, err)
		}
	}
	if got := claimSize(t, c, ssName(), volumeNameData, 1); got.Cmp(resource.MustParse("2Gi")) != 0 {
		t.Errorf("ordinal-1 data claim = %s, want 2Gi (untouched)", got.String())
	}
}

// TestMigrationRecreateRetriesTeardownWhileClaimsTerminating proves the recreate phase is
// re-entrant: when the old claims are already Terminating (e.g. a prior pass deleted the claims
// but its pod delete failed), it re-deletes the holder pod rather than waiting — otherwise the
// pinned claims could never finalize and the migration would wedge.
func TestMigrationRecreateRetriesTeardownWhileClaimsTerminating(t *testing.T) {
	cluster := &garagev1alpha1.GarageCluster{
		ObjectMeta: metav1.ObjectMeta{Name: testClusterName, Namespace: testClusterNS},
		Spec: garagev1alpha1.GarageClusterSpec{
			ReplicationFactor: 2,
			NodePools: []garagev1alpha1.NodePool{{
				Name: testPoolName, Replicas: 2,
				Storage: garagev1alpha1.NodePoolStorage{
					Data: garagev1alpha1.StorageSpec{Size: resource.MustParse("1Gi")},
					Meta: garagev1alpha1.StorageSpec{Size: resource.MustParse("1Gi")},
				},
			}},
		},
	}
	ss := desiredStatefulSet(cluster, &cluster.Spec.NodePools[0]) // template already at 1Gi

	now := metav1.Now()
	terminating := func(volume string) *corev1.PersistentVolumeClaim {
		c := storageClaim(ss.Name, volume, 0, "2Gi")
		c.DeletionTimestamp = &now
		c.Finalizers = []string{"kubernetes.io/pvc-protection"} // fake client requires one with a timestamp
		return c
	}
	pod0 := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: podName(ssName(), 0), Namespace: testClusterNS}}
	objs := []client.Object{
		ss,
		&storagev1.StorageClass{ObjectMeta: metav1.ObjectMeta{Name: expandableStorageClass}, AllowVolumeExpansion: ptr.To(true)},
		terminating(volumeNameData), terminating(volumeNameMeta),
		storageClaim(ss.Name, volumeNameData, 1, "1Gi"), storageClaim(ss.Name, volumeNameMeta, 1, "1Gi"),
		pod0,
	}
	r, c := newStorageReconciler(t, objs...)
	admin := &fakeClusterAdmin{layout: seededLayout(2), health: health(healthStatusHealthy, 256)}
	status := healthyStatus()
	status.StorageMigration = &garagev1alpha1.StorageMigrationStatus{
		Pool: testPoolName, Ordinal: 0, Phase: garagev1alpha1.StorageMigrationRecreatingVolume,
	}

	if _, err := r.reconcileStorageMigration(context.Background(), cluster, status, admin, migrationNodes(2, "1Gi")); err != nil {
		t.Fatalf("reconcileStorageMigration: %v", err)
	}

	var pod corev1.Pod
	err := c.Get(context.Background(), types.NamespacedName{Name: podName(ssName(), 0), Namespace: testClusterNS}, &pod)
	if !apierrors.IsNotFound(err) {
		t.Errorf("expected the holder pod to be re-deleted during teardown, got err=%v", err)
	}
	if status.StorageMigration.Phase != garagev1alpha1.StorageMigrationRecreatingVolume {
		t.Errorf("phase = %s, want RecreatingVolume while still tearing down", status.StorageMigration.Phase)
	}
}

// TestMigrationRecreateAdvancesWhenSwapped proves that once the fresh PVCs exist at the new
// size the node advances to rejoin.
func TestMigrationRecreateAdvancesWhenSwapped(t *testing.T) {
	// template and PVCs both already 1Gi: the swap is complete.
	r, _, cluster, desired := migrationWorld(t, "1Gi", "1Gi", "1Gi", 2, 2, true)
	admin := &fakeClusterAdmin{layout: seededLayout(2), health: health(healthStatusHealthy, 256)}
	status := healthyStatus()
	status.StorageMigration = &garagev1alpha1.StorageMigrationStatus{
		Pool: testPoolName, Ordinal: 0, Phase: garagev1alpha1.StorageMigrationRecreatingVolume,
	}

	if _, err := r.reconcileStorageMigration(context.Background(), cluster, status, admin, desired); err != nil {
		t.Fatalf("reconcileStorageMigration: %v", err)
	}
	if status.StorageMigration.Phase != garagev1alpha1.StorageMigrationAwaitingRejoin {
		t.Fatalf("phase = %s, want AwaitingRejoin once swapped", status.StorageMigration.Phase)
	}
}

// TestMigrationAwaitRejoinAddsThenCompletes proves the recreated node is re-added to the layout
// and, once refilled, the migration of that node completes (progress cleared).
func TestMigrationAwaitRejoinAddsThenCompletes(t *testing.T) {
	r, _, cluster, desired := migrationWorld(t, "1Gi", "1Gi", "1Gi", 2, 2, true)
	// node-0 was drained, so the layout holds only node-1 when the rejoin begins.
	layout := newFakeLayout()
	layout.applied["node-1"] = struct{}{}
	admin := &fakeClusterAdmin{layout: layout, health: health(healthStatusHealthy, 256)}
	status := healthyStatus()
	status.StorageMigration = &garagev1alpha1.StorageMigrationStatus{
		Pool: testPoolName, Ordinal: 0, Phase: garagev1alpha1.StorageMigrationAwaitingRejoin,
	}

	// First pass: the node is absent from the layout, so it is re-added and we keep waiting.
	if _, err := r.reconcileStorageMigration(context.Background(), cluster, status, admin, desired); err != nil {
		t.Fatalf("reconcileStorageMigration: %v", err)
	}
	if _, in := layout.applied["node-0"]; !in {
		t.Error("expected node-0 to be re-added to the layout")
	}
	if status.StorageMigration == nil {
		t.Fatal("migration must not complete before the node refills")
	}

	// Second pass: in the layout and fully replicated, so the node's migration completes.
	active, err := r.reconcileStorageMigration(context.Background(), cluster, status, admin, desired)
	if err != nil {
		t.Fatalf("reconcileStorageMigration: %v", err)
	}
	if !active {
		t.Error("expected active=true on the completing pass so the next node is picked up")
	}
	if status.StorageMigration != nil {
		t.Errorf("expected progress cleared once the node refilled, got %+v", status.StorageMigration)
	}
}

// TestMigrationPicksNextUnmigratedOrdinal proves that after one node is done the scan picks the
// next node whose volumes still differ from the spec.
func TestMigrationPicksNextUnmigratedOrdinal(t *testing.T) {
	r, c, cluster, desired := migrationWorld(t, "1Gi", "1Gi", "1Gi", 2, 2, true)
	// Ordinal 0 is already migrated (1Gi); make ordinal 1 still hold the old 2Gi.
	pvc := storageClaim(ssName(), volumeNameData, 1, "2Gi")
	if err := c.Update(context.Background(), pvc); err != nil {
		t.Fatalf("seed ordinal-1 claim: %v", err)
	}
	admin := &fakeClusterAdmin{layout: seededLayout(2), health: health(healthStatusHealthy, 256)}
	status := healthyStatus()

	if _, err := r.reconcileStorageMigration(context.Background(), cluster, status, admin, desired); err != nil {
		t.Fatalf("reconcileStorageMigration: %v", err)
	}
	if status.StorageMigration == nil || status.StorageMigration.Ordinal != 1 {
		t.Fatalf("storageMigration = %+v, want ordinal 1 to start", status.StorageMigration)
	}
}

// TestMigrationNoneNeededClearsCondition proves a converged pool leaves the migration inactive
// and clears any stale blocking condition.
func TestMigrationNoneNeededClearsCondition(t *testing.T) {
	r, _, cluster, desired := migrationWorld(t, "1Gi", "1Gi", "1Gi", 2, 2, true)
	admin := &fakeClusterAdmin{layout: seededLayout(2), health: health(healthStatusHealthy, 256)}
	status := healthyStatus()
	setCondition(status, conditionStorageChangePending, metav1.ConditionFalse, "Blocked", "stale")

	active, err := r.reconcileStorageMigration(context.Background(), cluster, status, admin, desired)
	if err != nil {
		t.Fatalf("reconcileStorageMigration: %v", err)
	}
	if active {
		t.Fatal("expected active=false when nothing needs migrating")
	}
	if meta.FindStatusCondition(status.Conditions, conditionStorageChangePending) != nil {
		t.Error("expected the stale StorageChangePending condition to be cleared")
	}
}

// TestMigrationDefersWhileScaleDownPending proves a migration does not start while a gated
// scale-down is pending (the StatefulSet runs more replicas than the pool wants), so the
// generic layout path can drain the scale-down first.
func TestMigrationDefersWhileScaleDownPending(t *testing.T) {
	// StatefulSet runs 3 replicas; the pool wants 2 (a pending scale-down) and also shrinks.
	r, _, cluster, desired := migrationWorld(t, "1Gi", "2Gi", "2Gi", 3, 2, true)
	cluster.Spec.NodePools[0].Replicas = 2
	admin := &fakeClusterAdmin{layout: seededLayout(3), health: health(healthStatusHealthy, 256)}
	status := healthyStatus()

	active, err := r.reconcileStorageMigration(context.Background(), cluster, status, admin, desired)
	if err != nil {
		t.Fatalf("reconcileStorageMigration: %v", err)
	}
	if active {
		t.Fatal("expected active=false: the migration must defer to the pending scale-down")
	}
	if status.StorageMigration != nil {
		t.Error("no migration should have started")
	}
}

// TestMigrationPicksUpNonExpandableGrow proves the second supported trigger: a grow on a
// StorageClass that forbids expansion is migrated rather than left blocked.
func TestMigrationPicksUpNonExpandableGrow(t *testing.T) {
	r, _, cluster, desired := migrationWorld(t, "2Gi", "1Gi", "1Gi", 2, 2, false)
	layout := seededLayout(2)
	admin := &fakeClusterAdmin{layout: layout, health: health(healthStatusHealthy, 256)}
	status := healthyStatus()

	active, err := r.reconcileStorageMigration(context.Background(), cluster, status, admin, desired)
	if err != nil {
		t.Fatalf("reconcileStorageMigration: %v", err)
	}
	if !active {
		t.Fatal("expected a non-expandable grow to start a migration")
	}
	if _, in := layout.applied["node-0"]; in {
		t.Error("expected node-0 drained for the non-expandable grow migration")
	}
}
