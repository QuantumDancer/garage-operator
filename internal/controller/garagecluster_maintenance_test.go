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

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"

	garagev1alpha1 "github.com/QuantumDancer/garage-operator/api/v1alpha1"
	"github.com/QuantumDancer/garage-operator/internal/garageadmin"
)

const (
	testNodeID   = "node-a"
	repairBlocks = "blocks"
	nodeSelf     = "node-self"
)

// reconcileMaintenance mutates only the passed-in status (and emits Events) — it never touches
// the API server — so these branch tests drive it directly with a fake admin, no envtest.
var _ = Describe("GarageCluster maintenance", func() {
	ctx := context.Background()

	newCluster := func(annotations map[string]string) *garagev1alpha1.GarageCluster {
		return &garagev1alpha1.GarageCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "c", Annotations: annotations},
		}
	}
	newReconciler := func() (*GarageClusterReconciler, *record.FakeRecorder) {
		rec := record.NewFakeRecorder(10)
		return &GarageClusterReconciler{Recorder: rec}, rec
	}

	Describe("snapshots", func() {
		It("snapshots on a new trigger, skips a repeat, and re-runs on a changed value", func() {
			maint := &fakeMaintenance{}
			admin := &fakeClusterAdmin{nodeID: testNodeID, maint: maint}
			r, _ := newReconciler()
			cluster := newCluster(map[string]string{annotationSnapshot: "t1"})
			status := &garagev1alpha1.GarageClusterStatus{}

			r.reconcileMaintenance(ctx, cluster, status, admin)
			Expect(maint.snapshotCalls).To(Equal(1))
			Expect(status.Maintenance.Snapshot.ObservedTrigger).To(Equal("t1"))
			Expect(status.Maintenance.Snapshot.Result).To(Equal(resultSucceeded))

			By("not re-running while the trigger is unchanged")
			r.reconcileMaintenance(ctx, cluster, status, admin)
			Expect(maint.snapshotCalls).To(Equal(1))

			By("re-running when the trigger value changes")
			cluster.Annotations[annotationSnapshot] = "t2"
			r.reconcileMaintenance(ctx, cluster, status, admin)
			Expect(maint.snapshotCalls).To(Equal(2))
			Expect(status.Maintenance.Snapshot.ObservedTrigger).To(Equal("t2"))
		})

		It("reports Partial when some nodes fail", func() {
			maint := &fakeMaintenance{snapshotResult: &garageadmin.MultiNodeResult{
				Succeeded: []string{testNodeID},
				Failed:    map[string]string{"node-b": "disk full"},
			}}
			admin := &fakeClusterAdmin{nodeID: testNodeID, maint: maint}
			r, _ := newReconciler()
			status := &garagev1alpha1.GarageClusterStatus{}

			r.reconcileMaintenance(ctx, newCluster(map[string]string{annotationSnapshot: "t1"}), status, admin)
			Expect(status.Maintenance.Snapshot.Result).To(Equal(resultPartial))
			Expect(status.Maintenance.Snapshot.Message).To(ContainSubstring("node-b: disk full"))
		})

		It("does nothing without a snapshot annotation", func() {
			maint := &fakeMaintenance{}
			admin := &fakeClusterAdmin{nodeID: testNodeID, maint: maint}
			r, _ := newReconciler()
			status := &garagev1alpha1.GarageClusterStatus{}

			r.reconcileMaintenance(ctx, newCluster(nil), status, admin)
			Expect(maint.snapshotCalls).To(BeZero())
			Expect(status.Maintenance).To(BeNil())
		})
	})

	Describe("repairs", func() {
		It("launches on a new trigger and then tracks progress to Done", func() {
			maint := &fakeMaintenance{}
			admin := &fakeClusterAdmin{nodeID: testNodeID, maint: maint}
			r, _ := newReconciler()
			cluster := newCluster(map[string]string{annotationRepair: repairBlocks})
			status := &garagev1alpha1.GarageClusterStatus{}

			By("launching without polling workers in the same pass")
			r.reconcileMaintenance(ctx, cluster, status, admin)
			Expect(maint.repairCalls).To(Equal(1))
			Expect(maint.repairTypes).To(Equal([]string{repairBlocks}))
			Expect(status.Maintenance.Repair.Type).To(Equal("blocks"))
			Expect(status.Maintenance.Repair.State).To(Equal(stateLaunched))

			By("reporting Running while a worker is active")
			maint.workers = []garageadmin.WorkerSummary{{Name: "block resync worker", State: "busy", Progress: "42 left"}}
			r.reconcileMaintenance(ctx, cluster, status, admin)
			Expect(maint.repairCalls).To(Equal(1)) // unchanged trigger does not relaunch
			Expect(status.Maintenance.Repair.State).To(Equal(stateRunning))
			Expect(status.Maintenance.Repair.Progress).To(Equal("block resync worker: 42 left"))

			By("reporting Done once no workers are active, then stopping polling")
			maint.workers = nil
			r.reconcileMaintenance(ctx, cluster, status, admin)
			Expect(status.Maintenance.Repair.State).To(Equal(stateDone))
			Expect(status.Maintenance.Repair.Progress).To(BeEmpty())
		})

		It("re-runs the same repair type when the @nonce suffix changes", func() {
			maint := &fakeMaintenance{}
			admin := &fakeClusterAdmin{nodeID: testNodeID, maint: maint}
			r, _ := newReconciler()
			cluster := newCluster(map[string]string{annotationRepair: repairBlocks})
			status := &garagev1alpha1.GarageClusterStatus{}

			r.reconcileMaintenance(ctx, cluster, status, admin)
			Expect(maint.repairCalls).To(Equal(1))

			cluster.Annotations[annotationRepair] = "blocks@2"
			r.reconcileMaintenance(ctx, cluster, status, admin)
			Expect(maint.repairCalls).To(Equal(2))
			Expect(maint.repairTypes).To(Equal([]string{repairBlocks, repairBlocks})) // type stripped of nonce
			Expect(status.Maintenance.Repair.ObservedTrigger).To(Equal("blocks@2"))
		})

		It("maps a scrub command and fails terminally on an unknown type", func() {
			maint := &fakeMaintenance{}
			admin := &fakeClusterAdmin{nodeID: testNodeID, maint: maint}
			r, rec := newReconciler()
			status := &garagev1alpha1.GarageClusterStatus{}

			r.reconcileMaintenance(ctx, newCluster(map[string]string{annotationRepair: "scrubStart"}), status, admin)
			Expect(maint.repairTypes).To(Equal([]string{"scrubStart"}))
			Expect(status.Maintenance.Repair.State).To(Equal(stateLaunched))

			By("recording Failed for an unknown type and not retrying it")
			cluster := newCluster(map[string]string{annotationRepair: "bogus"})
			r.reconcileMaintenance(ctx, cluster, status, admin)
			Expect(status.Maintenance.Repair.State).To(Equal(stateFailed))
			Expect(status.Maintenance.Repair.ObservedTrigger).To(Equal("bogus"))
			Expect(maint.repairCalls).To(Equal(1)) // scrubStart only; bogus never reached Garage
			Expect(rec.Events).To(Receive(ContainSubstring("RepairLaunched")))
			Expect(rec.Events).To(Receive(ContainSubstring("RepairFailed")))
		})
	})
})
