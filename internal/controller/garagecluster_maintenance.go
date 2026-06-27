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
	"fmt"
	"slices"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	garagev1alpha1 "github.com/QuantumDancer/garage-operator/api/v1alpha1"
	"github.com/QuantumDancer/garage-operator/internal/garageadmin"
)

// Annotations that trigger one-shot maintenance actions. Each holds a free-form trigger value;
// the operator acts once whenever the value differs from the one recorded in status, leaving
// the annotation in place so a GitOps controller never sees drift (the value is the trigger,
// not the action). To re-run the same action, change the value — for repair, append "@<nonce>"
// (e.g. "blocks@2"), which the operator strips back to the repair type.
const (
	annotationSnapshot = "garage.rottler.io/snapshot"
	annotationRepair   = "garage.rottler.io/repair"
)

// maintenanceNode targets every node. Snapshots and repairs are node-local; "*" fans the
// single Admin API call out across the whole cluster server-side.
const maintenanceNode = "*"

// Snapshot result and repair state values reported in status.maintenance.
const (
	resultSucceeded = "Succeeded"
	resultPartial   = "Partial"
	resultFailed    = "Failed"

	stateLaunched = "Launched"
	stateRunning  = "Running"
	stateDone     = "Done"
	stateFailed   = "Failed"
)

// reconcileMaintenance runs the operator-triggered, one-shot maintenance actions requested via
// annotations and refreshes the progress of a repair still in flight. It never fails the
// reconcile: the cluster being Ready does not depend on maintenance, so a maintenance error is
// recorded in status and surfaced as an Event, then retried on the next steady-state requeue.
//
// Semantics are at-least-once: an action is issued before its observedTrigger is persisted (the
// status write happens later, in finish), so a failed status write re-issues the action next
// pass. That is safe by design — snapshots are repeatable and repairs re-runnable — so we accept
// it rather than complicating the flow with a pre-write of the trigger.
func (r *GarageClusterReconciler) reconcileMaintenance(ctx context.Context, cluster *garagev1alpha1.GarageCluster, status *garagev1alpha1.GarageClusterStatus, admin clusterAdmin) {
	r.reconcileSnapshot(ctx, cluster, status, admin)
	r.reconcileRepair(ctx, cluster, status, admin)
}

// reconcileSnapshot takes a metadata snapshot when the snapshot annotation carries a new
// trigger value. A transport error leaves observedTrigger untouched so the next requeue
// retries; a completed call (even one some nodes rejected) advances it, since re-running is the
// user's call (bump the value).
func (r *GarageClusterReconciler) reconcileSnapshot(ctx context.Context, cluster *garagev1alpha1.GarageCluster, status *garagev1alpha1.GarageClusterStatus, admin clusterAdmin) {
	log := logf.FromContext(ctx)

	trigger, ok := cluster.Annotations[annotationSnapshot]
	if snapshot := snapshotStatus(status); !ok || (snapshot != nil && snapshot.ObservedTrigger == trigger) {
		return
	}

	result, err := admin.CreateMetadataSnapshot(ctx, maintenanceNode)
	if err != nil {
		log.Error(err, "Failed to create metadata snapshot")
		r.eventf(cluster, corev1.EventTypeWarning, "SnapshotFailed", err.Error())
		return
	}

	snapshot := &garagev1alpha1.SnapshotStatus{ObservedTrigger: trigger, Time: metav1.Now()}
	snapshot.Result, snapshot.Message = summarizeMultiResult(result)
	ensureMaintenance(status).Snapshot = snapshot

	if snapshot.Result == resultSucceeded {
		r.eventf(cluster, corev1.EventTypeNormal, "SnapshotCreated", snapshot.Message)
	} else {
		r.eventf(cluster, corev1.EventTypeWarning, "SnapshotFailed", snapshot.Message)
	}
}

// reconcileRepair launches a repair when the repair annotation carries a new trigger value,
// and otherwise refreshes the progress of a repair that has not yet finished. An unknown repair
// type is terminal: observedTrigger advances so the operator does not retry a value that can
// never succeed.
func (r *GarageClusterReconciler) reconcileRepair(ctx context.Context, cluster *garagev1alpha1.GarageCluster, status *garagev1alpha1.GarageClusterStatus, admin clusterAdmin) {
	log := logf.FromContext(ctx)
	repair := repairStatus(status)

	if trigger, ok := cluster.Annotations[annotationRepair]; ok && (repair == nil || repair.ObservedTrigger != trigger) {
		// Launch, then return: Garage spawns the repair's background worker asynchronously, so
		// polling progress in the same pass could see no active worker and wrongly mark it Done.
		// The next steady-state requeue refreshes progress once the worker is running.
		r.launchRepair(ctx, cluster, status, admin, trigger)
		return
	}

	if repair == nil || repair.State == stateDone || repair.State == stateFailed {
		return
	}

	workers, err := admin.ListActiveWorkers(ctx, maintenanceNode)
	if err != nil {
		log.Error(err, "Failed to list Garage workers")
		return
	}
	// finish() diffs status before writing, so an unchanged state/progress is already a no-op.
	repair.State = stateDone
	if len(workers) > 0 {
		repair.State = stateRunning
	}
	repair.Progress = summarizeWorkers(workers)
}

// launchRepair issues a single repair operation and records its initial state.
func (r *GarageClusterReconciler) launchRepair(ctx context.Context, cluster *garagev1alpha1.GarageCluster, status *garagev1alpha1.GarageClusterStatus, admin clusterAdmin, trigger string) {
	log := logf.FromContext(ctx)
	repairType := parseRepairType(trigger)

	result, err := admin.LaunchRepair(ctx, maintenanceNode, repairType)
	switch {
	case errors.Is(err, garageadmin.ErrUnknownRepairType):
		ensureMaintenance(status).Repair = &garagev1alpha1.RepairStatus{
			ObservedTrigger: trigger,
			Type:            repairType,
			Time:            metav1.Now(),
			State:           stateFailed,
			Progress:        "unknown repair type",
		}
		r.eventf(cluster, corev1.EventTypeWarning, "RepairFailed", fmt.Sprintf("Unknown repair type %q", repairType))
		return
	case err != nil:
		log.Error(err, "Failed to launch repair", "type", repairType)
		r.eventf(cluster, corev1.EventTypeWarning, "RepairFailed", err.Error())
		return
	}

	repair := &garagev1alpha1.RepairStatus{ObservedTrigger: trigger, Type: repairType, Time: metav1.Now()}
	if outcome, message := summarizeMultiResult(result); outcome == resultFailed {
		repair.State = stateFailed
		repair.Progress = message
		ensureMaintenance(status).Repair = repair
		r.eventf(cluster, corev1.EventTypeWarning, "RepairFailed", message)
		return
	}
	repair.State = stateLaunched
	ensureMaintenance(status).Repair = repair
	r.eventf(cluster, corev1.EventTypeNormal, "RepairLaunched", fmt.Sprintf("Launched %s repair", repairType))
}

// parseRepairType strips the optional "@<nonce>" re-run suffix from a repair trigger, leaving
// the repair type. The nonce lets a user relaunch the same repair by changing only the suffix.
func parseRepairType(trigger string) string {
	if before, _, ok := strings.Cut(trigger, "@"); ok {
		return before
	}
	return trigger
}

// summarizeMultiResult turns a fan-out result into a status result label and a human message.
func summarizeMultiResult(result garageadmin.MultiNodeResult) (string, string) {
	switch {
	case len(result.Succeeded) > 0 && len(result.Failed) == 0:
		return resultSucceeded, fmt.Sprintf("%d node(s) succeeded", len(result.Succeeded))
	case len(result.Succeeded) == 0 && len(result.Failed) == 0:
		return resultFailed, "no nodes responded"
	case len(result.Succeeded) == 0:
		return resultFailed, formatFailures(result.Failed)
	default:
		return resultPartial, fmt.Sprintf("%d node(s) succeeded, %d failed: %s",
			len(result.Succeeded), len(result.Failed), formatFailures(result.Failed))
	}
}

// formatFailures renders a node-id -> message map as a stable, sorted "node: message" list.
func formatFailures(failures map[string]string) string {
	nodes := make([]string, 0, len(failures))
	for node := range failures {
		nodes = append(nodes, node)
	}
	slices.Sort(nodes)
	parts := make([]string, 0, len(nodes))
	for _, node := range nodes {
		parts = append(parts, fmt.Sprintf("%s: %s", node, failures[node]))
	}
	return strings.Join(parts, "; ")
}

// summarizeWorkers renders active workers into a single progress string.
func summarizeWorkers(workers []garageadmin.WorkerSummary) string {
	if len(workers) == 0 {
		return ""
	}
	parts := make([]string, 0, len(workers))
	for _, w := range workers {
		if w.Progress != "" {
			parts = append(parts, fmt.Sprintf("%s: %s", w.Name, w.Progress))
		} else {
			parts = append(parts, w.Name)
		}
	}
	return strings.Join(parts, "; ")
}

func snapshotStatus(status *garagev1alpha1.GarageClusterStatus) *garagev1alpha1.SnapshotStatus {
	if status.Maintenance == nil {
		return nil
	}
	return status.Maintenance.Snapshot
}

func repairStatus(status *garagev1alpha1.GarageClusterStatus) *garagev1alpha1.RepairStatus {
	if status.Maintenance == nil {
		return nil
	}
	return status.Maintenance.Repair
}

// ensureMaintenance returns the status maintenance block, creating it on first use.
func ensureMaintenance(status *garagev1alpha1.GarageClusterStatus) *garagev1alpha1.MaintenanceStatus {
	if status.Maintenance == nil {
		status.Maintenance = &garagev1alpha1.MaintenanceStatus{}
	}
	return status.Maintenance
}
