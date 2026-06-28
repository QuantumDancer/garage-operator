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
	"errors"
	"reflect"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	garagev1alpha1 "github.com/QuantumDancer/garage-operator/api/v1alpha1"
)

// erroringRESTMapper fails every mapping with a fixed error, simulating a transient discovery
// failure (as opposed to a clean "no such kind", which a DefaultRESTMapper returns).
type erroringRESTMapper struct {
	meta.RESTMapper
	err error
}

func (m erroringRESTMapper) RESTMapping(schema.GroupKind, ...string) (*meta.RESTMapping, error) {
	return nil, m.err
}

// ownedBy stamps the cluster's controller reference onto a PodMonitor, matching what the
// reconciler asserts, so an "in sync" fixture is genuinely in sync (no ownership drift).
func ownedBy(t *testing.T, cluster *garagev1alpha1.GarageCluster, pm *unstructured.Unstructured) *unstructured.Unstructured {
	t.Helper()
	if err := ctrl.SetControllerReference(cluster, pm, bucketTestScheme(t)); err != nil {
		t.Fatalf("set owner ref: %v", err)
	}
	return pm
}

const (
	testScrapeInterval  = "20s"
	testPrometheusLabel = "kube-prometheus-stack"
)

// metricsCluster returns the single-pool test cluster with metrics scraping enabled.
func metricsCluster() *garagev1alpha1.GarageCluster {
	c := newTestCluster()
	c.Spec.Metrics = garagev1alpha1.MetricsConfig{
		Enabled:  true,
		Interval: testScrapeInterval,
		Labels:   map[string]string{"release": testPrometheusLabel},
	}
	return c
}

// metricsReconciler builds a reconciler whose fake client knows the PodMonitor GVK (so it can
// store the unstructured object) and whose RESTMapper reports the CRD as served only when
// supported is true — letting tests drive the feature-detection branch explicitly.
func metricsReconciler(t *testing.T, supported bool, objs ...client.Object) (*GarageClusterReconciler, client.Client) {
	t.Helper()
	scheme := bucketTestScheme(t)
	scheme.AddKnownTypeWithName(podMonitorGVK, &unstructured.Unstructured{})
	scheme.AddKnownTypeWithName(podMonitorGVK.GroupVersion().WithKind("PodMonitorList"), &unstructured.UnstructuredList{})

	builder := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...)
	if supported {
		mapper := meta.NewDefaultRESTMapper([]schema.GroupVersion{podMonitorGVK.GroupVersion()})
		mapper.Add(podMonitorGVK, meta.RESTScopeNamespace)
		builder = builder.WithRESTMapper(mapper)
	}
	c := builder.Build()
	return &GarageClusterReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(100)}, c
}

func getPodMonitor(t *testing.T, c client.Client, cluster *garagev1alpha1.GarageCluster) (*unstructured.Unstructured, error) {
	t.Helper()
	pm := newPodMonitor()
	err := c.Get(context.Background(), client.ObjectKey{Name: podMonitorName(cluster), Namespace: cluster.Namespace}, pm)
	return pm, err
}

func TestDesiredPodMonitor(t *testing.T) {
	cluster := metricsCluster()
	pm := desiredPodMonitor(cluster)

	if got := pm.GroupVersionKind(); got != podMonitorGVK {
		t.Fatalf("GVK = %v, want %v", got, podMonitorGVK)
	}
	if pm.GetName() != cluster.Name+"-metrics" || pm.GetNamespace() != cluster.Namespace {
		t.Fatalf("name/namespace = %s/%s, want %s-metrics/%s", pm.GetName(), pm.GetNamespace(), cluster.Name, cluster.Namespace)
	}

	labels := pm.GetLabels()
	if labels["release"] != testPrometheusLabel {
		t.Errorf("missing user label: %v", labels)
	}
	if labels[labelCluster] != cluster.Name {
		t.Errorf("missing cluster label: %v", labels)
	}

	sel, _, _ := unstructured.NestedStringMap(pm.Object, "spec", "selector", "matchLabels")
	if !reflect.DeepEqual(sel, clusterSelectorLabels(cluster)) {
		t.Errorf("selector matchLabels = %v, want %v", sel, clusterSelectorLabels(cluster))
	}

	eps, _, _ := unstructured.NestedSlice(pm.Object, "spec", "podMetricsEndpoints")
	if len(eps) != 1 {
		t.Fatalf("podMetricsEndpoints = %d, want 1", len(eps))
	}
	ep := eps[0].(map[string]any)
	if ep["port"] != portNameAdmin || ep["path"] != metricsPath || ep["scheme"] != metricsScheme || ep["interval"] != testScrapeInterval {
		t.Errorf("endpoint = %v", ep)
	}
}

func TestDesiredPodMonitorOmitsIntervalWhenUnset(t *testing.T) {
	cluster := metricsCluster()
	cluster.Spec.Metrics.Interval = ""

	eps, _, _ := unstructured.NestedSlice(desiredPodMonitor(cluster).Object, "spec", "podMetricsEndpoints")
	if _, ok := eps[0].(map[string]any)["interval"]; ok {
		t.Errorf("interval should be absent when unset: %v", eps[0])
	}
}

func TestReconcileMetricsCreatesPodMonitorWhenSupported(t *testing.T) {
	cluster := metricsCluster()
	r, c := metricsReconciler(t, true, cluster)
	status := &garagev1alpha1.GarageClusterStatus{}

	r.reconcileMetrics(context.Background(), cluster, status)

	pm, err := getPodMonitor(t, c, cluster)
	if err != nil {
		t.Fatalf("PodMonitor not created: %v", err)
	}
	owners := pm.GetOwnerReferences()
	if len(owners) != 1 || owners[0].Name != cluster.Name {
		t.Errorf("owner reference = %v, want controller ref to %s", owners, cluster.Name)
	}
	cond := meta.FindStatusCondition(status.Conditions, conditionMetricsReady)
	if cond == nil || cond.Status != metav1.ConditionTrue {
		t.Errorf("MetricsReady = %v, want True", cond)
	}
}

func TestReconcileMetricsUnsupportedSetsConditionAndCreatesNothing(t *testing.T) {
	cluster := metricsCluster()
	r, c := metricsReconciler(t, false, cluster)
	status := &garagev1alpha1.GarageClusterStatus{}

	r.reconcileMetrics(context.Background(), cluster, status)

	if _, err := getPodMonitor(t, c, cluster); !apierrors.IsNotFound(err) {
		t.Errorf("expected no PodMonitor, got err=%v", err)
	}
	cond := meta.FindStatusCondition(status.Conditions, conditionMetricsReady)
	if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != "PrometheusOperatorMissing" {
		t.Errorf("MetricsReady = %v, want False/PrometheusOperatorMissing", cond)
	}
}

func TestReconcileMetricsDisabledDeletesPodMonitorAndClearsCondition(t *testing.T) {
	cluster := metricsCluster()
	cluster.Spec.Metrics.Enabled = false
	existing := desiredPodMonitor(cluster)
	r, c := metricsReconciler(t, true, cluster, existing)
	status := &garagev1alpha1.GarageClusterStatus{}
	setCondition(status, conditionMetricsReady, metav1.ConditionTrue, "PodMonitorReady", "was on")

	r.reconcileMetrics(context.Background(), cluster, status)

	if _, err := getPodMonitor(t, c, cluster); !apierrors.IsNotFound(err) {
		t.Errorf("PodMonitor should be deleted, got err=%v", err)
	}
	if meta.FindStatusCondition(status.Conditions, conditionMetricsReady) != nil {
		t.Errorf("MetricsReady condition should be removed when metrics disabled")
	}
}

func TestReconcileMetricsUpdatesDriftedPodMonitor(t *testing.T) {
	cluster := metricsCluster()
	// Seed a PodMonitor whose endpoint drifted (wrong path, stale interval).
	stale := desiredPodMonitor(cluster)
	_ = unstructured.SetNestedSlice(stale.Object, []any{map[string]any{
		"port": portNameAdmin, "path": "/wrong", "scheme": metricsScheme, "interval": "99s",
	}}, "spec", "podMetricsEndpoints")
	r, c := metricsReconciler(t, true, cluster, stale)

	r.reconcileMetrics(context.Background(), cluster, &garagev1alpha1.GarageClusterStatus{})

	pm, err := getPodMonitor(t, c, cluster)
	if err != nil {
		t.Fatalf("get PodMonitor: %v", err)
	}
	eps, _, _ := unstructured.NestedSlice(pm.Object, "spec", "podMetricsEndpoints")
	ep := eps[0].(map[string]any)
	if ep["path"] != metricsPath || ep["interval"] != testScrapeInterval {
		t.Errorf("drift not reconciled: %v", ep)
	}
}

func TestReconcileMetricsNoOpWhenInSync(t *testing.T) {
	cluster := metricsCluster()
	existing := ownedBy(t, cluster, desiredPodMonitor(cluster))
	existing.SetResourceVersion("999")
	r, c := metricsReconciler(t, true, cluster, existing)

	r.reconcileMetrics(context.Background(), cluster, &garagev1alpha1.GarageClusterStatus{})

	pm, err := getPodMonitor(t, c, cluster)
	if err != nil {
		t.Fatalf("get PodMonitor: %v", err)
	}
	// An in-sync object is not updated, so its resourceVersion is unchanged.
	if pm.GetResourceVersion() != "999" {
		t.Errorf("PodMonitor was rewritten (rv=%s) despite being in sync", pm.GetResourceVersion())
	}
}

func TestReconcileMetricsAdoptsAndOwnsExistingPodMonitor(t *testing.T) {
	cluster := metricsCluster()
	// A same-named PodMonitor with no controller reference (e.g. pre-created): the reconciler
	// adopts it and must stamp ownership so it is garbage-collected with the cluster.
	orphan := desiredPodMonitor(cluster)
	r, c := metricsReconciler(t, true, cluster, orphan)

	r.reconcileMetrics(context.Background(), cluster, &garagev1alpha1.GarageClusterStatus{})

	pm, err := getPodMonitor(t, c, cluster)
	if err != nil {
		t.Fatalf("get PodMonitor: %v", err)
	}
	owners := pm.GetOwnerReferences()
	if len(owners) != 1 || owners[0].Name != cluster.Name || owners[0].Controller == nil || !*owners[0].Controller {
		t.Errorf("owner reference = %v, want controller ref to %s", owners, cluster.Name)
	}
}

func TestReconcileMetricsUnsupportedEmitsEventOnceOnTransition(t *testing.T) {
	cluster := metricsCluster()
	r, _ := metricsReconciler(t, false, cluster)
	rec := r.Recorder.(*record.FakeRecorder)
	status := &garagev1alpha1.GarageClusterStatus{}

	// First pass transitions into the missing-operator state and warns; the second is steady
	// state (the condition already says so) and must stay silent.
	r.reconcileMetrics(context.Background(), cluster, status)
	r.reconcileMetrics(context.Background(), cluster, status)

	if got := len(rec.Events); got != 1 {
		t.Fatalf("expected exactly 1 Warning event across two reconciles, got %d", got)
	}
}

func TestReconcileMetricsTransientMapperErrorReportsUnknown(t *testing.T) {
	cluster := metricsCluster()
	scheme := bucketTestScheme(t)
	rec := record.NewFakeRecorder(10)
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithRESTMapper(erroringRESTMapper{err: errors.New("discovery boom")}).Build()
	r := &GarageClusterReconciler{Client: c, Scheme: scheme, Recorder: rec}
	status := &garagev1alpha1.GarageClusterStatus{}

	r.reconcileMetrics(context.Background(), cluster, status)

	cond := meta.FindStatusCondition(status.Conditions, conditionMetricsReady)
	if cond == nil || cond.Status != metav1.ConditionUnknown || cond.Reason != "DiscoveryFailed" {
		t.Errorf("MetricsReady = %v, want Unknown/DiscoveryFailed", cond)
	}
	// A transient discovery hiccup is not a missing CRD: no Warning event.
	if len(rec.Events) != 0 {
		t.Errorf("transient error should emit no Warning event, got %d", len(rec.Events))
	}
}
