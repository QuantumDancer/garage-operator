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
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"

	garagev1alpha1 "github.com/QuantumDancer/garage-operator/api/v1alpha1"
	"github.com/QuantumDancer/garage-operator/internal/garageadmin"
)

const (
	zoneLabel    = "topology.kubernetes.io/zone"
	zonePoolName = "main"
	zonePod0     = "main-0"
	zoneNodeName = "worker-a"
)

func zoneTestCluster(pool garagev1alpha1.NodePool) *garagev1alpha1.GarageCluster {
	return &garagev1alpha1.GarageCluster{
		ObjectMeta: metav1.ObjectMeta{Name: testClusterName, Namespace: testClusterNS},
		Spec:       garagev1alpha1.GarageClusterSpec{NodePools: []garagev1alpha1.NodePool{pool}},
	}
}

func TestResolveZoneStaticPrecedence(t *testing.T) {
	r, _ := newStorageReconciler(t)
	ctx := context.Background()

	// Explicit zone wins over the pool-name fallback.
	pool := garagev1alpha1.NodePool{Name: zonePoolName, Zone: "dc1"}
	cluster := zoneTestCluster(pool)
	got, err := r.resolveZone(ctx, cluster, &cluster.Spec.NodePools[0], zonePod0)
	if err != nil {
		t.Fatalf("resolveZone: %v", err)
	}
	if got != "dc1" {
		t.Errorf("zone = %q, want dc1 (explicit zone)", got)
	}

	// No zone and no zoneFrom falls back to the pool name.
	pool = garagev1alpha1.NodePool{Name: zonePoolName}
	cluster = zoneTestCluster(pool)
	got, err = r.resolveZone(ctx, cluster, &cluster.Spec.NodePools[0], zonePod0)
	if err != nil {
		t.Fatalf("resolveZone: %v", err)
	}
	if got != "main" {
		t.Errorf("zone = %q, want main (pool-name fallback)", got)
	}
}

func TestResolveZoneFromNodeLabel(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: zonePod0, Namespace: testClusterNS},
		Spec:       corev1.PodSpec{NodeName: zoneNodeName},
	}
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: zoneNodeName, Labels: map[string]string{zoneLabel: "za"}},
	}
	r, _ := newStorageReconciler(t, pod, node)

	pool := garagev1alpha1.NodePool{Name: zonePoolName, Zone: "ignored", ZoneFrom: zoneLabel}
	cluster := zoneTestCluster(pool)
	got, err := r.resolveZone(context.Background(), cluster, &cluster.Spec.NodePools[0], zonePod0)
	if err != nil {
		t.Fatalf("resolveZone: %v", err)
	}
	if got != "za" {
		t.Errorf("zone = %q, want za (derived from the Node label, overriding the static zone)", got)
	}
}

func TestResolveZoneFromDefersWhenUnscheduled(t *testing.T) {
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: zonePod0, Namespace: testClusterNS}}
	r, _ := newStorageReconciler(t, pod)

	pool := garagev1alpha1.NodePool{Name: zonePoolName, ZoneFrom: zoneLabel}
	cluster := zoneTestCluster(pool)
	_, err := r.resolveZone(context.Background(), cluster, &cluster.Spec.NodePools[0], zonePod0)
	if err == nil || !strings.Contains(err.Error(), "not scheduled") {
		t.Errorf("error = %v, want it to report the pod is not scheduled yet", err)
	}
}

func TestResolveZoneFromDefersWhenLabelMissing(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: zonePod0, Namespace: testClusterNS},
		Spec:       corev1.PodSpec{NodeName: zoneNodeName},
	}
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: zoneNodeName}}
	r, _ := newStorageReconciler(t, pod, node)

	pool := garagev1alpha1.NodePool{Name: zonePoolName, ZoneFrom: zoneLabel}
	cluster := zoneTestCluster(pool)
	_, err := r.resolveZone(context.Background(), cluster, &cluster.Spec.NodePools[0], zonePod0)
	if err == nil || !strings.Contains(err.Error(), "zoneFrom label") {
		t.Errorf("error = %v, want it to report the missing zoneFrom label", err)
	}
}

func TestDesiredZoneRedundancy(t *testing.T) {
	cases := []struct {
		name string
		spec *garagev1alpha1.ZoneRedundancy
		want garageadmin.ZoneRedundancyValue
	}{
		{"omitted defaults to maximum", nil, garageadmin.ZoneRedundancyValue{Maximum: true}},
		{"empty mode defaults to maximum", &garagev1alpha1.ZoneRedundancy{}, garageadmin.ZoneRedundancyValue{Maximum: true}},
		{"explicit maximum", &garagev1alpha1.ZoneRedundancy{Mode: garagev1alpha1.ZoneRedundancyMaximum}, garageadmin.ZoneRedundancyValue{Maximum: true}},
		{"atLeast", &garagev1alpha1.ZoneRedundancy{Mode: garagev1alpha1.ZoneRedundancyAtLeast, AtLeast: 2}, garageadmin.ZoneRedundancyValue{AtLeast: 2}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cluster := &garagev1alpha1.GarageCluster{Spec: garagev1alpha1.GarageClusterSpec{ZoneRedundancy: tc.spec}}
			if got := desiredZoneRedundancy(cluster); !got.Equal(tc.want) {
				t.Errorf("desiredZoneRedundancy = %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestDistinctZones(t *testing.T) {
	nodes := []nodeEndpoint{{zone: "a"}, {zone: "b"}, {zone: "a"}}
	if got := distinctZones(nodes); got != 2 {
		t.Errorf("distinctZones = %d, want 2", got)
	}
}

// zoneRedundancyReconciler builds a reconciler with a fake admin whose layout starts at the
// given applied redundancy, plus the shared layout it converges against.
func zoneRedundancyReconciler(applied garageadmin.ZoneRedundancyValue) (*GarageClusterReconciler, *fakeLayout) {
	layout := newFakeLayout()
	layout.redundancy = applied
	r := &GarageClusterReconciler{
		Recorder: record.NewFakeRecorder(100),
		NewAdminClient: func(string, string) (clusterAdmin, error) {
			return &fakeClusterAdmin{nodeID: "n", layout: layout}, nil
		},
	}
	return r, layout
}

func healthyZoneStatus() *garagev1alpha1.GarageClusterStatus {
	return &garagev1alpha1.GarageClusterStatus{
		Health: &garagev1alpha1.HealthStatus{Status: healthStatusHealthy},
		Layout: &garagev1alpha1.LayoutStatus{Version: 1},
	}
}

func twoZoneDesired() []nodeEndpoint {
	return []nodeEndpoint{{zone: "a"}, {zone: "b"}}
}

func reconcileZoneRedundancyWith(t *testing.T, r *GarageClusterReconciler, cluster *garagev1alpha1.GarageCluster, status *garagev1alpha1.GarageClusterStatus, desired []nodeEndpoint) error {
	t.Helper()
	admin, err := r.NewAdminClient("", "")
	if err != nil {
		t.Fatalf("NewAdminClient: %v", err)
	}
	return r.reconcileZoneRedundancy(context.Background(), cluster, status, admin, desired)
}

func TestReconcileZoneRedundancyApplies(t *testing.T) {
	r, layout := zoneRedundancyReconciler(garageadmin.ZoneRedundancyValue{Maximum: true})
	cluster := &garagev1alpha1.GarageCluster{
		Spec: garagev1alpha1.GarageClusterSpec{
			ZoneRedundancy: &garagev1alpha1.ZoneRedundancy{Mode: garagev1alpha1.ZoneRedundancyAtLeast, AtLeast: 2},
		},
	}
	status := healthyZoneStatus()

	if err := reconcileZoneRedundancyWith(t, r, cluster, status, twoZoneDesired()); err != nil {
		t.Fatalf("reconcileZoneRedundancy: %v", err)
	}
	if layout.redundancyCalls != 1 {
		t.Fatalf("SetZoneRedundancy calls = %d, want 1", layout.redundancyCalls)
	}
	if layout.redundancy.Maximum || layout.redundancy.AtLeast != 2 {
		t.Errorf("applied redundancy = %+v, want AtLeast 2", layout.redundancy)
	}
	// The applied layout version is echoed back into status.
	if status.Layout.Version != layout.version {
		t.Errorf("status layout version = %d, want %d", status.Layout.Version, layout.version)
	}
}

func TestReconcileZoneRedundancyNoOpWhenConverged(t *testing.T) {
	r, layout := zoneRedundancyReconciler(garageadmin.ZoneRedundancyValue{AtLeast: 2})
	cluster := &garagev1alpha1.GarageCluster{
		Spec: garagev1alpha1.GarageClusterSpec{
			ZoneRedundancy: &garagev1alpha1.ZoneRedundancy{Mode: garagev1alpha1.ZoneRedundancyAtLeast, AtLeast: 2},
		},
	}
	if err := reconcileZoneRedundancyWith(t, r, cluster, healthyZoneStatus(), twoZoneDesired()); err != nil {
		t.Fatalf("reconcileZoneRedundancy: %v", err)
	}
	if layout.redundancyCalls != 0 {
		t.Errorf("SetZoneRedundancy calls = %d, want 0 when already converged", layout.redundancyCalls)
	}
}

func TestReconcileZoneRedundancyBlocksWhenAtLeastExceedsZones(t *testing.T) {
	r, layout := zoneRedundancyReconciler(garageadmin.ZoneRedundancyValue{Maximum: true})
	cluster := &garagev1alpha1.GarageCluster{
		Spec: garagev1alpha1.GarageClusterSpec{
			ZoneRedundancy: &garagev1alpha1.ZoneRedundancy{Mode: garagev1alpha1.ZoneRedundancyAtLeast, AtLeast: 3},
		},
	}
	status := healthyZoneStatus()

	if err := reconcileZoneRedundancyWith(t, r, cluster, status, twoZoneDesired()); err != nil {
		t.Fatalf("reconcileZoneRedundancy: %v", err)
	}
	if layout.redundancyCalls != 0 {
		t.Errorf("SetZoneRedundancy calls = %d, want 0 (atLeast exceeds zones)", layout.redundancyCalls)
	}
	cond := meta.FindStatusCondition(status.Conditions, conditionLayoutApplied)
	if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != "ZoneRedundancyInvalid" {
		t.Errorf("LayoutApplied condition = %+v, want False/ZoneRedundancyInvalid", cond)
	}
}

func TestReconcileZoneRedundancyBlocksWhenAtLeastOmitted(t *testing.T) {
	// `{mode: AtLeast}` with atLeast omitted slips through admission as 0; the controller must
	// refuse it rather than ship atLeast:0 to Garage.
	r, layout := zoneRedundancyReconciler(garageadmin.ZoneRedundancyValue{Maximum: true})
	cluster := &garagev1alpha1.GarageCluster{
		Spec: garagev1alpha1.GarageClusterSpec{
			ZoneRedundancy: &garagev1alpha1.ZoneRedundancy{Mode: garagev1alpha1.ZoneRedundancyAtLeast},
		},
	}
	status := healthyZoneStatus()

	if err := reconcileZoneRedundancyWith(t, r, cluster, status, twoZoneDesired()); err != nil {
		t.Fatalf("reconcileZoneRedundancy: %v", err)
	}
	if layout.redundancyCalls != 0 {
		t.Errorf("SetZoneRedundancy calls = %d, want 0 (atLeast omitted is invalid)", layout.redundancyCalls)
	}
	cond := meta.FindStatusCondition(status.Conditions, conditionLayoutApplied)
	if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != "ZoneRedundancyInvalid" {
		t.Errorf("LayoutApplied condition = %+v, want False/ZoneRedundancyInvalid", cond)
	}
}

func TestReconcileZoneRedundancyDefersWhenUnhealthy(t *testing.T) {
	r, layout := zoneRedundancyReconciler(garageadmin.ZoneRedundancyValue{Maximum: true})
	cluster := &garagev1alpha1.GarageCluster{
		Spec: garagev1alpha1.GarageClusterSpec{
			ZoneRedundancy: &garagev1alpha1.ZoneRedundancy{Mode: garagev1alpha1.ZoneRedundancyAtLeast, AtLeast: 2},
		},
	}
	status := &garagev1alpha1.GarageClusterStatus{
		Health: &garagev1alpha1.HealthStatus{Status: "degraded"},
		Layout: &garagev1alpha1.LayoutStatus{Version: 1},
	}

	if err := reconcileZoneRedundancyWith(t, r, cluster, status, twoZoneDesired()); err != nil {
		t.Fatalf("reconcileZoneRedundancy: %v", err)
	}
	if layout.redundancyCalls != 0 {
		t.Errorf("SetZoneRedundancy calls = %d, want 0 (deferred while unhealthy)", layout.redundancyCalls)
	}
}
