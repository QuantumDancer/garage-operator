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

	// connectResults, when set, is returned from ConnectClusterNodes (one entry per peer).
	// When nil the server reports success for every requested peer.
	connectResults []ConnectNodeResponse
	connectBodies  []ConnectClusterNodesRequest
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
				f.nodeID: {NodeId: f.nodeID, DbEngine: "lmdb", GarageVersion: "v2.0.0", RustVersion: "1.0"},
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
