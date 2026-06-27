package garageadmin

import (
	"context"
	"errors"
	"testing"
)

const (
	testNodeA         = "node-a"
	testNodeB         = "node-b"
	blockResyncWorker = "block resync worker"
)

// workerStateResp builds a string-valued WorkerStateResp ("busy"/"idle"/"done") for tests.
func workerStateResp(t *testing.T, state string) WorkerStateResp {
	t.Helper()
	var ws WorkerStateResp
	if err := ws.FromWorkerStateResp0(WorkerStateResp0(state)); err != nil {
		t.Fatalf("build worker state: %v", err)
	}
	return ws
}

// throttledWorkerState builds the object-valued "throttled" WorkerStateResp.
func throttledWorkerState(t *testing.T) WorkerStateResp {
	t.Helper()
	var ws WorkerStateResp
	v := WorkerStateResp1{}
	v.Throttled.DurationSecs = 1.5
	if err := ws.FromWorkerStateResp1(v); err != nil {
		t.Fatalf("build throttled state: %v", err)
	}
	return ws
}

func TestCreateMetadataSnapshot(t *testing.T) {
	fake := &fakeAdmin{
		t:               t,
		snapshotSuccess: map[string]any{testNodeA: nil, testNodeB: nil},
		snapshotError:   map[string]string{"node-c": "disk full"},
	}
	client := newTestClient(t, fake)

	result, err := client.CreateMetadataSnapshot(context.Background(), "*")
	if err != nil {
		t.Fatalf("CreateMetadataSnapshot: %v", err)
	}
	if fake.snapshotNode != "*" {
		t.Errorf("node param = %q, want %q", fake.snapshotNode, "*")
	}
	if got := result.Succeeded; len(got) != 2 || got[0] != testNodeA || got[1] != testNodeB {
		t.Errorf("Succeeded = %v, want sorted [node-a node-b]", got)
	}
	if msg, ok := result.Failed["node-c"]; !ok || msg != "disk full" {
		t.Errorf("Failed[node-c] = %q, ok=%v, want %q", msg, ok, "disk full")
	}
}

func TestLaunchRepairBareType(t *testing.T) {
	fake := &fakeAdmin{t: t, repairSuccess: map[string]any{testNodeA: nil}}
	client := newTestClient(t, fake)

	result, err := client.LaunchRepair(context.Background(), "*", "blocks")
	if err != nil {
		t.Fatalf("LaunchRepair: %v", err)
	}
	if len(result.Succeeded) != 1 || result.Succeeded[0] != testNodeA {
		t.Errorf("Succeeded = %v, want [node-a]", result.Succeeded)
	}
	if len(fake.repairBodies) != 1 {
		t.Fatalf("repair bodies = %d, want 1", len(fake.repairBodies))
	}
	got, err := fake.repairBodies[0].RepairType.AsRepairType0()
	if err != nil {
		t.Fatalf("decode sent repair type: %v", err)
	}
	if string(got) != "blocks" {
		t.Errorf("sent repair type = %q, want %q", got, "blocks")
	}
}

func TestLaunchRepairScrubCommand(t *testing.T) {
	fake := &fakeAdmin{t: t, repairSuccess: map[string]any{testNodeA: nil}}
	client := newTestClient(t, fake)

	if _, err := client.LaunchRepair(context.Background(), "self", "scrubStart"); err != nil {
		t.Fatalf("LaunchRepair: %v", err)
	}
	if fake.repairNode != "self" {
		t.Errorf("node param = %q, want %q", fake.repairNode, "self")
	}
	scrub, err := fake.repairBodies[0].RepairType.AsRepairType7()
	if err != nil {
		t.Fatalf("decode scrub repair type: %v", err)
	}
	if scrub.Scrub != "start" {
		t.Errorf("scrub command = %q, want %q", scrub.Scrub, "start")
	}
}

func TestLaunchRepairUnknownType(t *testing.T) {
	fake := &fakeAdmin{t: t}
	client := newTestClient(t, fake)

	_, err := client.LaunchRepair(context.Background(), "*", "bogus")
	if !errors.Is(err, ErrUnknownRepairType) {
		t.Fatalf("error = %v, want ErrUnknownRepairType", err)
	}
	// An unknown type must not reach Garage.
	if len(fake.repairBodies) != 0 {
		t.Errorf("repair bodies = %d, want 0 (no request sent)", len(fake.repairBodies))
	}
}

func TestListActiveWorkersFiltersIdle(t *testing.T) {
	fake := &fakeAdmin{
		t: t,
		workers: map[string][]WorkerInfoResp{
			testNodeA: {
				{Name: blockResyncWorker, State: workerStateResp(t, "busy"), Progress: ptrString("12 left")},
				{Name: "scrub worker", State: workerStateResp(t, "idle")},
				{Name: "rebalance worker", State: throttledWorkerState(t)},
			},
			testNodeB: {
				{Name: blockResyncWorker, State: workerStateResp(t, "done")},
			},
		},
	}
	client := newTestClient(t, fake)

	workers, err := client.ListActiveWorkers(context.Background(), "*")
	if err != nil {
		t.Fatalf("ListActiveWorkers: %v", err)
	}
	if len(workers) != 2 {
		t.Fatalf("active workers = %d, want 2 (busy + throttled only)", len(workers))
	}
	// node-a sorts first; within a node, source order is preserved.
	if workers[0].Name != blockResyncWorker || workers[0].State != "busy" || workers[0].Progress != "12 left" {
		t.Errorf("workers[0] = %+v, want busy block resync with progress", workers[0])
	}
	if workers[1].Name != "rebalance worker" || workers[1].State != "throttled" {
		t.Errorf("workers[1] = %+v, want throttled rebalance", workers[1])
	}
}

func TestIsValidRepairType(t *testing.T) {
	for _, valid := range []string{"tables", "blocks", "rebalance", "clearResyncQueue", "scrubStart", "scrubCancel"} {
		if !IsValidRepairType(valid) {
			t.Errorf("IsValidRepairType(%q) = false, want true", valid)
		}
	}
	for _, invalid := range []string{"", "scrub", "Blocks", "bogus"} {
		if IsValidRepairType(invalid) {
			t.Errorf("IsValidRepairType(%q) = true, want false", invalid)
		}
	}
}

func ptrString(s string) *string { return &s }
