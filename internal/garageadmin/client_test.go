package garageadmin

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestClientGetClusterHealth exercises the full generated stack against a fake
// Admin API: it confirms the wrapper injects bearer auth, targets the right path,
// and that a real response body round-trips into the generated typed struct.
func TestClientGetClusterHealth(t *testing.T) {
	const token = "test-admin-token"

	want := GetClusterHealthResponse{
		Status:           "healthy",
		ConnectedNodes:   3,
		KnownNodes:       3,
		StorageNodes:     3,
		Partitions:       256,
		PartitionsQuorum: 256,
		PartitionsAllOk:  256,
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer "+token {
			t.Errorf("Authorization header = %q, want %q", got, "Bearer "+token)
		}
		if r.URL.Path != "/v2/GetClusterHealth" {
			t.Errorf("request path = %q, want %q", r.URL.Path, "/v2/GetClusterHealth")
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(want); err != nil {
			t.Fatalf("encode response: %v", err)
		}
	}))
	defer srv.Close()

	client, err := NewAdminClient(srv.URL, token)
	if err != nil {
		t.Fatalf("NewAdminClient: %v", err)
	}

	resp, err := client.GetClusterHealthWithResponse(context.Background())
	if err != nil {
		t.Fatalf("GetClusterHealthWithResponse: %v", err)
	}
	if resp.StatusCode() != http.StatusOK {
		t.Fatalf("status code = %d, want 200", resp.StatusCode())
	}
	if resp.JSON200 == nil {
		t.Fatal("JSON200 is nil; response did not decode")
	}
	if got := *resp.JSON200; got != want {
		t.Errorf("decoded response = %+v, want %+v", got, want)
	}
}
