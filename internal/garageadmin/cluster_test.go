package garageadmin

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

const (
	zoneDefault = "default"
	nodeID1     = "node-1"
	nodeStale   = "node-stale"
)

// fakeAdmin is a minimal stand-in for the Garage Admin API. Each field is the JSON the
// server returns for the corresponding endpoint; recorded slices capture request bodies so
// tests can assert what the layout helper sent.
type fakeAdmin struct {
	t *testing.T

	nodeID string

	layout          GetClusterLayoutResponse
	updateBodies    []UpdateClusterLayoutRequest
	appliedVersions []int64
	updateCalls     int
	applyCalls      int
	revertCalls     int
	previewCalls    int

	// previewMessage is returned from PreviewClusterLayoutChanges. previewError, when set,
	// is returned as the error variant instead.
	previewMessage []string
	previewError   string

	// connectResults, when set, is returned from ConnectClusterNodes (one entry per peer).
	// When nil the server reports success for every requested peer.
	connectResults []ConnectNodeResponse
	connectBodies  []ConnectClusterNodesRequest

	// Maintenance endpoints. The *Success/*Error maps are returned verbatim as the fan-out
	// response; repairBodies records the RepairType the client sent; workers is the per-node
	// worker list ListWorkers returns. snapshotNode/repairNode/workersNode capture the node
	// query parameter.
	snapshotSuccess map[string]any
	snapshotError   map[string]string
	snapshotNode    string
	repairSuccess   map[string]any
	repairError     map[string]string
	repairBodies    []LocalLaunchRepairOperationRequest
	repairNode      string
	workers         map[string][]WorkerInfoResp
	workersNode     string
}

func (f *fakeAdmin) server() *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/v2/GetNodeInfo":
			resp := MultiResponseLocalGetNodeInfoResponse{
				Error: map[string]string{},
			}
			resp.Success = map[string]struct {
				DbEngine       string    `json:"dbEngine"`
				GarageFeatures *[]string `json:"garageFeatures,omitempty"`
				GarageVersion  string    `json:"garageVersion"`
				Hostname       *string   `json:"hostname,omitempty"`
				NodeId         string    `json:"nodeId"`
				RustVersion    string    `json:"rustVersion"`
			}{
				f.nodeID: {NodeId: f.nodeID, DbEngine: "lmdb", GarageVersion: "v2.3.0", RustVersion: "1.0"},
			}
			f.encode(w, resp)
		case "/v2/ConnectClusterNodes":
			var body ConnectClusterNodesRequest
			f.decode(r, &body)
			f.connectBodies = append(f.connectBodies, body)
			results := f.connectResults
			if results == nil {
				results = make([]ConnectNodeResponse, len(body))
				for i := range results {
					results[i] = ConnectNodeResponse{Success: true}
				}
			}
			f.encode(w, results)
		case "/v2/GetClusterLayout":
			f.encode(w, f.layout)
		case "/v2/UpdateClusterLayout":
			f.updateCalls++
			var body UpdateClusterLayoutRequest
			f.decode(r, &body)
			f.updateBodies = append(f.updateBodies, body)
			f.encode(w, UpdateClusterLayoutResponse{})
		case "/v2/ApplyClusterLayout":
			f.applyCalls++
			var body ApplyClusterLayoutRequest
			f.decode(r, &body)
			f.appliedVersions = append(f.appliedVersions, body.Version)
			// Reflect the apply into the layout so a follow-up GET is converged.
			f.layout.Version = body.Version
			f.encode(w, ApplyClusterLayoutResponse{})
		case "/v2/PreviewClusterLayoutChanges":
			f.previewCalls++
			var resp PreviewClusterLayoutChangesResponse
			if f.previewError != "" {
				if err := resp.FromPreviewClusterLayoutChangesResponse0(
					PreviewClusterLayoutChangesResponse0{Error: f.previewError}); err != nil {
					f.t.Fatalf("build preview error: %v", err)
				}
			} else {
				if err := resp.FromPreviewClusterLayoutChangesResponse1(
					PreviewClusterLayoutChangesResponse1{Message: f.previewMessage}); err != nil {
					f.t.Fatalf("build preview success: %v", err)
				}
			}
			f.encode(w, resp)
		case "/v2/RevertClusterLayout":
			f.revertCalls++
			f.encode(w, f.layout)
		case "/v2/CreateMetadataSnapshot":
			f.snapshotNode = r.URL.Query().Get("node")
			f.encode(w, MultiResponseLocalCreateMetadataSnapshotResponse{
				Success: f.snapshotSuccess,
				Error:   f.snapshotError,
			})
		case "/v2/LaunchRepairOperation":
			f.repairNode = r.URL.Query().Get("node")
			var body LocalLaunchRepairOperationRequest
			f.decode(r, &body)
			f.repairBodies = append(f.repairBodies, body)
			f.encode(w, MultiResponseLocalLaunchRepairOperationResponse{
				Success: f.repairSuccess,
				Error:   f.repairError,
			})
		case "/v2/ListWorkers":
			f.workersNode = r.URL.Query().Get("node")
			f.encode(w, MultiResponseLocalListWorkersResponse{
				Success: f.workers,
				Error:   map[string]string{},
			})
		default:
			f.t.Errorf("unexpected request path %q", r.URL.Path)
			http.Error(w, "unexpected", http.StatusNotFound)
		}
	}))
}

func (f *fakeAdmin) encode(w http.ResponseWriter, v any) {
	if err := json.NewEncoder(w).Encode(v); err != nil {
		f.t.Fatalf("encode response: %v", err)
	}
}

func (f *fakeAdmin) decode(r *http.Request, v any) {
	if err := json.NewDecoder(r.Body).Decode(v); err != nil {
		f.t.Fatalf("decode request: %v", err)
	}
}

func newTestClient(t *testing.T, fake *fakeAdmin) *AdminClient {
	t.Helper()
	srv := fake.server()
	t.Cleanup(srv.Close)
	client, err := NewAdminClient(srv.URL, "test-token")
	if err != nil {
		t.Fatalf("NewAdminClient: %v", err)
	}
	return client
}

func ptrInt64(v int64) *int64 { return &v }

func TestNodeID(t *testing.T) {
	fake := &fakeAdmin{t: t, nodeID: "abcd1234"}
	client := newTestClient(t, fake)

	got, err := client.NodeID(context.Background())
	if err != nil {
		t.Fatalf("NodeID: %v", err)
	}
	if got != "abcd1234" {
		t.Errorf("NodeID = %q, want %q", got, "abcd1234")
	}
}

func TestConnectNodes(t *testing.T) {
	fake := &fakeAdmin{t: t}
	client := newTestClient(t, fake)

	peers := []string{"node-2@pod-1.headless.ns.svc:3901", "node-3@pod-2.headless.ns.svc:3901"}
	if err := client.ConnectNodes(context.Background(), peers); err != nil {
		t.Fatalf("ConnectNodes: %v", err)
	}
	if len(fake.connectBodies) != 1 {
		t.Fatalf("ConnectClusterNodes calls = %d, want 1", len(fake.connectBodies))
	}
	if got := fake.connectBodies[0]; len(got) != 2 || got[0] != peers[0] || got[1] != peers[1] {
		t.Errorf("sent peers = %v, want %v", got, peers)
	}
}

func TestConnectNodesSurfacesPerPeerFailure(t *testing.T) {
	failure := "connection refused"
	fake := &fakeAdmin{
		t: t,
		connectResults: []ConnectNodeResponse{
			{Success: true},
			{Success: false, Error: &failure},
		},
	}
	client := newTestClient(t, fake)

	err := client.ConnectNodes(context.Background(), []string{"ok@a:3901", "bad@b:3901"})
	if err == nil {
		t.Fatal("ConnectNodes error = nil, want a per-peer failure")
	}
	if !strings.Contains(err.Error(), "bad@b:3901") || !strings.Contains(err.Error(), failure) {
		t.Errorf("error = %q, want it to name the failed peer and its message", err)
	}
}

func TestEnsureLayoutAssignsRoleOnFreshCluster(t *testing.T) {
	fake := &fakeAdmin{
		t:      t,
		layout: GetClusterLayoutResponse{Version: 0, Roles: []LayoutNodeRole{}},
	}
	client := newTestClient(t, fake)

	applied, version, err := client.EnsureLayout(context.Background(), []DesiredRole{
		{NodeID: nodeID1, Zone: zoneDefault, Capacity: 1 << 40},
	})
	if err != nil {
		t.Fatalf("EnsureLayout: %v", err)
	}
	if !applied {
		t.Error("applied = false, want true on a fresh cluster")
	}
	if version != 1 {
		t.Errorf("version = %d, want 1", version)
	}
	if fake.updateCalls != 1 || fake.applyCalls != 1 {
		t.Fatalf("update/apply calls = %d/%d, want 1/1", fake.updateCalls, fake.applyCalls)
	}
	if got := fake.appliedVersions[0]; got != 1 {
		t.Errorf("applied version = %d, want 1 (current+1)", got)
	}

	// Verify the staged change carried the derived capacity and zone.
	role, err := (*fake.updateBodies[0].Roles)[0].AsNodeRoleChangeRequest1()
	if err != nil {
		t.Fatalf("decode staged role: %v", err)
	}
	if role.Id != nodeID1 || role.Zone != "default" || role.Capacity == nil || *role.Capacity != 1<<40 {
		t.Errorf("staged role = %+v, want id=node-1 zone=default capacity=2^40", role)
	}
}

func TestEnsureLayoutIdempotentWhenConverged(t *testing.T) {
	fake := &fakeAdmin{
		t: t,
		layout: GetClusterLayoutResponse{
			Version: 5,
			Roles: []LayoutNodeRole{
				{Id: nodeID1, Zone: zoneDefault, Capacity: ptrInt64(1 << 40)},
			},
		},
	}
	client := newTestClient(t, fake)

	applied, version, err := client.EnsureLayout(context.Background(), []DesiredRole{
		{NodeID: nodeID1, Zone: zoneDefault, Capacity: 1 << 40},
	})
	if err != nil {
		t.Fatalf("EnsureLayout: %v", err)
	}
	if applied {
		t.Error("applied = true, want false when layout already matches")
	}
	if version != 5 {
		t.Errorf("version = %d, want 5 (unchanged)", version)
	}
	if fake.updateCalls != 0 || fake.applyCalls != 0 {
		t.Errorf("update/apply calls = %d/%d, want 0/0 on a converged cluster", fake.updateCalls, fake.applyCalls)
	}
}

func TestPlanLayoutDetectsRemovalAndAdditive(t *testing.T) {
	// node-1 is already in the layout and stays; node-stale is in the layout but no longer
	// desired (a removal); node-new is desired but absent (an additive change).
	fake := &fakeAdmin{
		t: t,
		layout: GetClusterLayoutResponse{
			Version: 7,
			Roles: []LayoutNodeRole{
				{Id: nodeID1, Zone: zoneDefault, Capacity: ptrInt64(1 << 40)},
				{Id: nodeStale, Zone: zoneDefault, Capacity: ptrInt64(1 << 40)},
			},
		},
	}
	client := newTestClient(t, fake)

	plan, err := client.PlanLayout(context.Background(), []DesiredRole{
		{NodeID: nodeID1, Zone: zoneDefault, Capacity: 1 << 40},
		{NodeID: "node-new", Zone: zoneDefault, Capacity: 1 << 40},
	})
	if err != nil {
		t.Fatalf("PlanLayout: %v", err)
	}
	if plan.CurrentVersion != 7 || plan.TargetVersion != 8 {
		t.Errorf("versions = %d/%d, want current 7 target 8", plan.CurrentVersion, plan.TargetVersion)
	}
	if !plan.IsDestructive() {
		t.Error("IsDestructive = false, want true (node-stale is removed)")
	}
	if len(plan.Removals) != 1 || plan.Removals[0] != nodeStale {
		t.Errorf("removals = %v, want [node-stale]", plan.Removals)
	}
	if len(plan.AdditiveChanges) != 1 {
		t.Fatalf("additive changes = %d, want 1 (node-new)", len(plan.AdditiveChanges))
	}
	add, err := plan.AdditiveChanges[0].AsNodeRoleChangeRequest1()
	if err != nil {
		t.Fatalf("decode additive change: %v", err)
	}
	if add.Id != "node-new" {
		t.Errorf("additive change id = %q, want node-new", add.Id)
	}

	// StagedChanges carries the additive assignment followed by the removal.
	staged, err := plan.StagedChanges()
	if err != nil {
		t.Fatalf("StagedChanges: %v", err)
	}
	if len(staged) != 2 {
		t.Fatalf("staged changes = %d, want 2", len(staged))
	}
	removal, err := staged[1].AsNodeRoleChangeRequest0()
	if err != nil {
		t.Fatalf("decode removal change: %v", err)
	}
	if removal.Id != nodeStale || !removal.Remove {
		t.Errorf("removal change = %+v, want id=node-stale remove=true", removal)
	}
}

func TestPlanLayoutCleanWhenConverged(t *testing.T) {
	fake := &fakeAdmin{
		t: t,
		layout: GetClusterLayoutResponse{
			Version: 3,
			Roles: []LayoutNodeRole{
				{Id: nodeID1, Zone: zoneDefault, Capacity: ptrInt64(1 << 40)},
			},
		},
	}
	client := newTestClient(t, fake)

	plan, err := client.PlanLayout(context.Background(), []DesiredRole{
		{NodeID: nodeID1, Zone: zoneDefault, Capacity: 1 << 40},
	})
	if err != nil {
		t.Fatalf("PlanLayout: %v", err)
	}
	if plan.HasChanges() || plan.IsDestructive() {
		t.Errorf("plan = %+v, want no changes on a converged cluster", plan)
	}
	if plan.TargetVersion != 0 {
		t.Errorf("target version = %d, want 0 for an empty plan", plan.TargetVersion)
	}
}

func TestPreviewStagedChangesReturnsMessage(t *testing.T) {
	fake := &fakeAdmin{t: t, previewMessage: []string{"line 1", "line 2"}}
	client := newTestClient(t, fake)

	msg, err := client.PreviewStagedChanges(context.Background())
	if err != nil {
		t.Fatalf("PreviewStagedChanges: %v", err)
	}
	if len(msg) != 2 || msg[0] != "line 1" || msg[1] != "line 2" {
		t.Errorf("message = %v, want [line 1, line 2]", msg)
	}
}

func TestPreviewStagedChangesSurfacesError(t *testing.T) {
	fake := &fakeAdmin{t: t, previewError: "layout cannot be computed"}
	client := newTestClient(t, fake)

	_, err := client.PreviewStagedChanges(context.Background())
	if err == nil || !strings.Contains(err.Error(), "layout cannot be computed") {
		t.Errorf("error = %v, want it to surface the computation failure", err)
	}
}

func TestStageApplyRevertHitEndpoints(t *testing.T) {
	fake := &fakeAdmin{
		t:      t,
		layout: GetClusterLayoutResponse{Version: 4, Roles: []LayoutNodeRole{}},
	}
	client := newTestClient(t, fake)
	ctx := context.Background()

	change, err := removeRoleChange(nodeStale)
	if err != nil {
		t.Fatalf("removeRoleChange: %v", err)
	}
	if err := client.StageLayoutChanges(ctx, []NodeRoleChangeRequest{change}); err != nil {
		t.Fatalf("StageLayoutChanges: %v", err)
	}
	if fake.updateCalls != 1 {
		t.Errorf("update calls = %d, want 1", fake.updateCalls)
	}

	if err := client.ApplyLayout(ctx, 5); err != nil {
		t.Fatalf("ApplyLayout: %v", err)
	}
	if fake.applyCalls != 1 || fake.appliedVersions[0] != 5 {
		t.Errorf("apply calls/version = %d/%v, want 1/[5]", fake.applyCalls, fake.appliedVersions)
	}

	if err := client.RevertStagedChanges(ctx); err != nil {
		t.Fatalf("RevertStagedChanges: %v", err)
	}
	if fake.revertCalls != 1 {
		t.Errorf("revert calls = %d, want 1", fake.revertCalls)
	}
}

func TestStageLayoutChangesNoOpOnEmpty(t *testing.T) {
	fake := &fakeAdmin{t: t}
	client := newTestClient(t, fake)

	if err := client.StageLayoutChanges(context.Background(), nil); err != nil {
		t.Fatalf("StageLayoutChanges: %v", err)
	}
	if fake.updateCalls != 0 {
		t.Errorf("update calls = %d, want 0 for empty changes", fake.updateCalls)
	}
}

func TestRemoveNodeDrainsOneNode(t *testing.T) {
	fake := &fakeAdmin{
		t: t,
		layout: GetClusterLayoutResponse{
			Version: 9,
			Roles: []LayoutNodeRole{
				{Id: nodeID1, Zone: zoneDefault, Capacity: ptrInt64(1 << 40)},
				{Id: nodeStale, Zone: zoneDefault, Capacity: ptrInt64(1 << 40)},
			},
		},
	}
	client := newTestClient(t, fake)

	if err := client.RemoveNode(context.Background(), nodeStale); err != nil {
		t.Fatalf("RemoveNode: %v", err)
	}

	// Leftover staged changes are discarded before staging exactly this removal, which is
	// then applied as the next version.
	if fake.revertCalls != 1 {
		t.Errorf("revert calls = %d, want 1", fake.revertCalls)
	}
	if fake.updateCalls != 1 || fake.applyCalls != 1 {
		t.Fatalf("update/apply calls = %d/%d, want 1/1", fake.updateCalls, fake.applyCalls)
	}
	if got := fake.appliedVersions[0]; got != 10 {
		t.Errorf("applied version = %d, want 10 (current+1)", got)
	}
	staged := *fake.updateBodies[0].Roles
	if len(staged) != 1 {
		t.Fatalf("staged changes = %d, want 1", len(staged))
	}
	removal, err := staged[0].AsNodeRoleChangeRequest0()
	if err != nil {
		t.Fatalf("decode removal: %v", err)
	}
	if removal.Id != nodeStale || !removal.Remove {
		t.Errorf("staged removal = %+v, want id=node-stale remove=true", removal)
	}
}

func TestAddNodeAssignsOneRole(t *testing.T) {
	fake := &fakeAdmin{
		t:      t,
		layout: GetClusterLayoutResponse{Version: 2, Roles: []LayoutNodeRole{}},
	}
	client := newTestClient(t, fake)

	err := client.AddNode(context.Background(), DesiredRole{NodeID: nodeID1, Zone: zoneDefault, Capacity: 1 << 39})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	if fake.revertCalls != 1 || fake.updateCalls != 1 || fake.applyCalls != 1 {
		t.Fatalf("revert/update/apply = %d/%d/%d, want 1/1/1", fake.revertCalls, fake.updateCalls, fake.applyCalls)
	}
	if got := fake.appliedVersions[0]; got != 3 {
		t.Errorf("applied version = %d, want 3 (current+1)", got)
	}
	staged := *fake.updateBodies[0].Roles
	assign, err := staged[0].AsNodeRoleChangeRequest1()
	if err != nil {
		t.Fatalf("decode assignment: %v", err)
	}
	if assign.Id != nodeID1 || assign.Capacity == nil || *assign.Capacity != 1<<39 {
		t.Errorf("staged assignment = %+v, want id=node-1 capacity=2^39", assign)
	}
}

func zoneRedundancyMaximumUnion(t *testing.T) ZoneRedundancy {
	t.Helper()
	var zr ZoneRedundancy
	if err := zr.FromZoneRedundancy1(Maximum); err != nil {
		t.Fatalf("build maximum: %v", err)
	}
	return zr
}

func zoneRedundancyAtLeastUnion(t *testing.T, n int) ZoneRedundancy {
	t.Helper()
	var zr ZoneRedundancy
	if err := zr.FromZoneRedundancy0(ZoneRedundancy0{AtLeast: n}); err != nil {
		t.Fatalf("build atLeast: %v", err)
	}
	return zr
}

func TestCurrentZoneRedundancyMaximum(t *testing.T) {
	fake := &fakeAdmin{
		t:      t,
		layout: GetClusterLayoutResponse{Version: 1, Parameters: LayoutParameters{ZoneRedundancy: zoneRedundancyMaximumUnion(t)}},
	}
	client := newTestClient(t, fake)

	got, err := client.CurrentZoneRedundancy(context.Background())
	if err != nil {
		t.Fatalf("CurrentZoneRedundancy: %v", err)
	}
	if !got.Maximum {
		t.Errorf("got %+v, want Maximum", got)
	}
}

func TestCurrentZoneRedundancyAtLeast(t *testing.T) {
	fake := &fakeAdmin{
		t:      t,
		layout: GetClusterLayoutResponse{Version: 1, Parameters: LayoutParameters{ZoneRedundancy: zoneRedundancyAtLeastUnion(t, 2)}},
	}
	client := newTestClient(t, fake)

	got, err := client.CurrentZoneRedundancy(context.Background())
	if err != nil {
		t.Fatalf("CurrentZoneRedundancy: %v", err)
	}
	if got.Maximum || got.AtLeast != 2 {
		t.Errorf("got %+v, want AtLeast 2", got)
	}
}

func TestSetZoneRedundancyStagesParametersAndApplies(t *testing.T) {
	fake := &fakeAdmin{
		t:      t,
		layout: GetClusterLayoutResponse{Version: 4, Parameters: LayoutParameters{ZoneRedundancy: zoneRedundancyMaximumUnion(t)}},
	}
	client := newTestClient(t, fake)

	version, err := client.SetZoneRedundancy(context.Background(), ZoneRedundancyValue{AtLeast: 2})
	if err != nil {
		t.Fatalf("SetZoneRedundancy: %v", err)
	}
	if version != 5 {
		t.Errorf("version = %d, want 5 (current+1)", version)
	}
	// Leftover staged changes are discarded first, then exactly the parameters are staged and
	// applied as the next version.
	if fake.revertCalls != 1 || fake.updateCalls != 1 || fake.applyCalls != 1 {
		t.Fatalf("revert/update/apply = %d/%d/%d, want 1/1/1", fake.revertCalls, fake.updateCalls, fake.applyCalls)
	}
	if got := fake.appliedVersions[0]; got != 5 {
		t.Errorf("applied version = %d, want 5", got)
	}

	body := fake.updateBodies[0]
	if body.Roles != nil {
		t.Errorf("roles = %v, want nil for a parameters-only update", body.Roles)
	}
	if body.Parameters == nil {
		t.Fatal("parameters = nil, want the staged zone redundancy")
	}
	lp, err := body.Parameters.AsLayoutParameters()
	if err != nil {
		t.Fatalf("decode staged parameters: %v", err)
	}
	val, err := zoneRedundancyFromAPI(lp.ZoneRedundancy)
	if err != nil {
		t.Fatalf("decode staged redundancy: %v", err)
	}
	if val.Maximum || val.AtLeast != 2 {
		t.Errorf("staged redundancy = %+v, want AtLeast 2", val)
	}
}

func TestZoneRedundancyValueEqual(t *testing.T) {
	cases := []struct {
		name string
		a, b ZoneRedundancyValue
		want bool
	}{
		{"both maximum", ZoneRedundancyValue{Maximum: true}, ZoneRedundancyValue{Maximum: true}, true},
		{"maximum vs atLeast", ZoneRedundancyValue{Maximum: true}, ZoneRedundancyValue{AtLeast: 2}, false},
		{"same atLeast", ZoneRedundancyValue{AtLeast: 3}, ZoneRedundancyValue{AtLeast: 3}, true},
		{"different atLeast", ZoneRedundancyValue{AtLeast: 2}, ZoneRedundancyValue{AtLeast: 3}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.a.Equal(tc.b); got != tc.want {
				t.Errorf("%+v.Equal(%+v) = %v, want %v", tc.a, tc.b, got, tc.want)
			}
		})
	}
}

func TestEnsureLayoutRestagesOnCapacityChange(t *testing.T) {
	fake := &fakeAdmin{
		t: t,
		layout: GetClusterLayoutResponse{
			Version: 2,
			Roles: []LayoutNodeRole{
				{Id: nodeID1, Zone: zoneDefault, Capacity: ptrInt64(1 << 39)},
			},
		},
	}
	client := newTestClient(t, fake)

	applied, version, err := client.EnsureLayout(context.Background(), []DesiredRole{
		{NodeID: nodeID1, Zone: zoneDefault, Capacity: 1 << 40},
	})
	if err != nil {
		t.Fatalf("EnsureLayout: %v", err)
	}
	if !applied || version != 3 {
		t.Errorf("applied/version = %v/%d, want true/3 after capacity change", applied, version)
	}
}
