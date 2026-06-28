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

	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	garagev1alpha1 "github.com/QuantumDancer/garage-operator/api/v1alpha1"
)

// reconcileMetrics converges the PodMonitor that scrapes the cluster's Garage nodes. It is
// best-effort and never gates Ready: a Prometheus scrape config is auxiliary to the cluster's
// operation, so a failure here is surfaced on the MetricsReady condition and logged, but never
// returned to block the core reconcile. Drift is corrected on the steady-state requeue rather
// than via an Owns watch — watching a CRD that may be absent would fail the manager at startup.
func (r *GarageClusterReconciler) reconcileMetrics(
	ctx context.Context,
	cluster *garagev1alpha1.GarageCluster,
	status *garagev1alpha1.GarageClusterStatus,
) {
	log := logf.FromContext(ctx)
	supported, mapErr := r.podMonitorSupported()

	if !cluster.Spec.Metrics.Enabled {
		// Tear down a PodMonitor left over from when metrics was enabled, and drop the
		// condition so a disabled cluster reports nothing about metrics. Nothing to clean up
		// if the CRD is not even installed (or its presence could not be determined).
		if supported {
			if err := r.deletePodMonitor(ctx, cluster); err != nil {
				log.Error(err, "Failed to delete PodMonitor")
			}
		}
		meta.RemoveStatusCondition(&status.Conditions, conditionMetricsReady)
		return
	}

	// A transient discovery error is not the same as "the CRD is absent": report it as Unknown
	// rather than claiming the operator is missing, and self-correct on the next requeue.
	if mapErr != nil {
		log.V(1).Info("Could not determine PodMonitor support", "error", mapErr.Error())
		setCondition(status, conditionMetricsReady, metav1.ConditionUnknown, "DiscoveryFailed",
			"Could not determine whether the Prometheus Operator CRDs are installed: "+mapErr.Error())
		return
	}

	if !supported {
		const reason = "PrometheusOperatorMissing"
		const msg = "Prometheus Operator CRDs are not installed; install them to scrape Garage metrics"
		// Emit the Warning only on the transition into this state, not on every requeue: the
		// MetricsReady condition already carries the signal idempotently, so re-firing the event
		// each pass would be pure churn. status still holds the previous reconcile's condition.
		if r.Recorder != nil && !conditionMatches(status, conditionMetricsReady, metav1.ConditionFalse, reason) {
			r.Recorder.Event(cluster, "Warning", reason, msg)
		}
		setCondition(status, conditionMetricsReady, metav1.ConditionFalse, reason, msg)
		return
	}

	if err := r.ensurePodMonitor(ctx, cluster); err != nil {
		log.Error(err, "Failed to reconcile PodMonitor")
		setCondition(status, conditionMetricsReady, metav1.ConditionFalse, "PodMonitorError", err.Error())
		return
	}
	setCondition(status, conditionMetricsReady, metav1.ConditionTrue, "PodMonitorReady", "Garage metrics are exposed via a PodMonitor")
}

// conditionMatches reports whether the named condition is already in the given status/reason —
// used to fire a one-shot Event only when the state actually transitions.
func conditionMatches(status *garagev1alpha1.GarageClusterStatus, condType string, s metav1.ConditionStatus, reason string) bool {
	cond := meta.FindStatusCondition(status.Conditions, condType)
	return cond != nil && cond.Status == s && cond.Reason == reason
}

// podMonitorSupported reports whether the PodMonitor CRD is served by the API server. A genuine
// "no such kind" answer returns (false, nil); any other (transient) mapper failure returns the
// error so the caller can distinguish it from a missing CRD. The controller-runtime RESTMapper
// reloads on a cache miss, so a CRD installed after the manager started is discovered later.
func (r *GarageClusterReconciler) podMonitorSupported() (bool, error) {
	_, err := r.RESTMapper().RESTMapping(podMonitorGVK.GroupKind(), podMonitorGVK.Version)
	switch {
	case err == nil:
		return true, nil
	case meta.IsNoMatchError(err):
		return false, nil
	default:
		return false, err
	}
}

// ensurePodMonitor creates the PodMonitor if absent, otherwise reconciles its spec, labels, and
// controller ownership. The owner reference is re-asserted on the update path too, so a
// same-named object the operator adopts is still garbage-collected with the cluster.
// SetControllerReference is a no-op when the cluster already owns it and errors (rather than
// hijacking) if a different controller does.
func (r *GarageClusterReconciler) ensurePodMonitor(ctx context.Context, cluster *garagev1alpha1.GarageCluster) error {
	desired := desiredPodMonitor(cluster)

	existing := newPodMonitor()
	err := r.Get(ctx, client.ObjectKeyFromObject(desired), existing)
	if apierrors.IsNotFound(err) {
		if err := ctrl.SetControllerReference(cluster, desired, r.Scheme); err != nil {
			return err
		}
		return r.Create(ctx, desired)
	}
	if err != nil {
		return err
	}

	ownerBefore := existing.GetOwnerReferences()
	if err := ctrl.SetControllerReference(cluster, existing, r.Scheme); err != nil {
		return err
	}
	ownerChanged := !apiequality.Semantic.DeepEqual(ownerBefore, existing.GetOwnerReferences())

	if !ownerChanged &&
		apiequality.Semantic.DeepEqual(existing.Object["spec"], desired.Object["spec"]) &&
		apiequality.Semantic.DeepEqual(existing.GetLabels(), desired.GetLabels()) {
		return nil
	}
	existing.Object["spec"] = desired.Object["spec"]
	existing.SetLabels(desired.GetLabels())
	return r.Update(ctx, existing)
}

// deletePodMonitor removes the PodMonitor, ignoring a missing object so a disabled cluster that
// never had one is a no-op.
func (r *GarageClusterReconciler) deletePodMonitor(ctx context.Context, cluster *garagev1alpha1.GarageCluster) error {
	pm := newPodMonitor()
	pm.SetNamespace(cluster.Namespace)
	pm.SetName(podMonitorName(cluster))
	return client.IgnoreNotFound(r.Delete(ctx, pm))
}

// newPodMonitor returns an empty unstructured PodMonitor with its GVK set, for Get/Delete calls.
func newPodMonitor() *unstructured.Unstructured {
	pm := &unstructured.Unstructured{}
	pm.SetGroupVersionKind(podMonitorGVK)
	return pm
}
