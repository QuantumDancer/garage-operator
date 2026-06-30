package garageadmin

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// Canonical fixtures shared across the bucket-wrapper tests.
const (
	testBucketID = "abc"
	testAlias    = "photos"
)

// fakeBucketServer is a minimal Garage Admin API stand-in for the bucket endpoints. Each
// handler records what it received so tests can assert request shape, and returns canned
// responses keyed off the fields below.
type fakeBucketServer struct {
	t *testing.T

	// bucket, when non-nil, is returned from GetBucketInfo; nil yields a 404.
	bucket *GetBucketInfoResponse

	createdAlias string
	updateBodies []UpdateBucketRequestBody
	addedAliases []BucketAliasEnum0
	removedAlias []BucketAliasEnum0
	deletedIDs   []string
	deleteStatus int // status code DeleteBucket returns; defaults to 200
}

func (f *fakeBucketServer) client() *AdminClient {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/v2/GetBucketInfo":
			if f.bucket == nil {
				http.Error(w, `{"error":"not found"}`, http.StatusNotFound)
				return
			}
			f.encode(w, f.bucket)
		case "/v2/CreateBucket":
			var body CreateBucketRequest
			f.decode(r, &body)
			if body.GlobalAlias != nil {
				f.createdAlias = *body.GlobalAlias
			}
			f.encode(w, GetBucketInfoResponse{Id: "new-bucket-id", GlobalAliases: []string{}, Created: time.Now()})
		case "/v2/UpdateBucket":
			var body UpdateBucketRequestBody
			f.decode(r, &body)
			f.updateBodies = append(f.updateBodies, body)
			f.encode(w, GetBucketInfoResponse{Id: r.URL.Query().Get("id")})
		case "/v2/AddBucketAlias":
			var body BucketAliasEnum
			f.decode(r, &body)
			g, err := body.AsBucketAliasEnum0()
			if err != nil {
				f.t.Fatalf("decode alias union: %v", err)
			}
			f.addedAliases = append(f.addedAliases, g)
			f.encode(w, GetBucketInfoResponse{Id: g.BucketId})
		case "/v2/RemoveBucketAlias":
			var body BucketAliasEnum
			f.decode(r, &body)
			g, err := body.AsBucketAliasEnum0()
			if err != nil {
				f.t.Fatalf("decode alias union: %v", err)
			}
			f.removedAlias = append(f.removedAlias, g)
			f.encode(w, GetBucketInfoResponse{Id: g.BucketId})
		case "/v2/DeleteBucket":
			f.deletedIDs = append(f.deletedIDs, r.URL.Query().Get("id"))
			status := f.deleteStatus
			if status == 0 {
				status = http.StatusOK
			}
			w.WriteHeader(status)
		default:
			f.t.Errorf("unexpected request path %q", r.URL.Path)
			http.Error(w, "unexpected", http.StatusNotFound)
		}
	}))
	f.t.Cleanup(srv.Close)
	client, err := NewAdminClient(srv.URL, "test-token")
	if err != nil {
		f.t.Fatalf("NewAdminClient: %v", err)
	}
	return client
}

func (f *fakeBucketServer) encode(w http.ResponseWriter, v any) {
	if err := json.NewEncoder(w).Encode(v); err != nil {
		f.t.Fatalf("encode response: %v", err)
	}
}

func (f *fakeBucketServer) decode(r *http.Request, v any) {
	if err := json.NewDecoder(r.Body).Decode(v); err != nil {
		f.t.Fatalf("decode request: %v", err)
	}
}

func TestGetBucketByIDFound(t *testing.T) {
	fake := &fakeBucketServer{t: t, bucket: &GetBucketInfoResponse{Id: testBucketID, GlobalAliases: []string{testAlias}}}
	info, found, err := fake.client().GetBucketByID(context.Background(), testBucketID)
	if err != nil {
		t.Fatalf("GetBucketByID: %v", err)
	}
	if !found || info == nil || info.Id != testBucketID {
		t.Fatalf("got found=%v info=%+v, want found bucket abc", found, info)
	}
}

func TestGetBucketByIDNotFound(t *testing.T) {
	fake := &fakeBucketServer{t: t, bucket: nil}
	info, found, err := fake.client().GetBucketByID(context.Background(), "missing")
	if err != nil {
		t.Fatalf("GetBucketByID: %v", err)
	}
	if found || info != nil {
		t.Fatalf("got found=%v info=%+v, want not found", found, info)
	}
}

func TestCreateBucketWithAlias(t *testing.T) {
	fake := &fakeBucketServer{t: t}
	id, err := fake.client().CreateBucket(context.Background(), testAlias)
	if err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	if id != "new-bucket-id" {
		t.Errorf("id = %q, want new-bucket-id", id)
	}
	if fake.createdAlias != testAlias {
		t.Errorf("created alias = %q, want photos", fake.createdAlias)
	}
}

func TestCreateBucketWithoutAlias(t *testing.T) {
	fake := &fakeBucketServer{t: t}
	if _, err := fake.client().CreateBucket(context.Background(), ""); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	if fake.createdAlias != "" {
		t.Errorf("created alias = %q, want empty (anonymous bucket)", fake.createdAlias)
	}
}

func TestUpdateBucketSendsBody(t *testing.T) {
	fake := &fakeBucketServer{t: t}
	quota := int64(100)
	body := UpdateBucketRequestBody{Quotas: &ApiBucketQuotas{MaxObjects: &quota}}
	if err := fake.client().UpdateBucket(context.Background(), testBucketID, body); err != nil {
		t.Fatalf("UpdateBucket: %v", err)
	}
	if len(fake.updateBodies) != 1 {
		t.Fatalf("UpdateBucket calls = %d, want 1", len(fake.updateBodies))
	}
	got := fake.updateBodies[0]
	if got.Quotas == nil || got.Quotas.MaxObjects == nil || *got.Quotas.MaxObjects != 100 {
		t.Errorf("sent quotas = %+v, want maxObjects=100", got.Quotas)
	}
}

func TestAddAndRemoveGlobalAlias(t *testing.T) {
	fake := &fakeBucketServer{t: t}
	c := fake.client()
	if err := c.AddBucketGlobalAlias(context.Background(), testBucketID, testAlias); err != nil {
		t.Fatalf("AddBucketGlobalAlias: %v", err)
	}
	if err := c.RemoveBucketGlobalAlias(context.Background(), testBucketID, "old"); err != nil {
		t.Fatalf("RemoveBucketGlobalAlias: %v", err)
	}
	if len(fake.addedAliases) != 1 || fake.addedAliases[0].BucketId != testBucketID || fake.addedAliases[0].GlobalAlias != testAlias {
		t.Errorf("added = %+v, want {abc, photos}", fake.addedAliases)
	}
	if len(fake.removedAlias) != 1 || fake.removedAlias[0].GlobalAlias != "old" {
		t.Errorf("removed = %+v, want alias old", fake.removedAlias)
	}
}

func TestDeleteBucket(t *testing.T) {
	fake := &fakeBucketServer{t: t}
	if err := fake.client().DeleteBucket(context.Background(), testBucketID); err != nil {
		t.Fatalf("DeleteBucket: %v", err)
	}
	if len(fake.deletedIDs) != 1 || fake.deletedIDs[0] != testBucketID {
		t.Errorf("deleted ids = %v, want [abc]", fake.deletedIDs)
	}
}

func TestDeleteBucketSurfacesFailure(t *testing.T) {
	fake := &fakeBucketServer{t: t, deleteStatus: http.StatusBadRequest}
	if err := fake.client().DeleteBucket(context.Background(), "nonempty"); err == nil {
		t.Fatal("DeleteBucket error = nil, want failure on non-200 (e.g. non-empty bucket)")
	}
}

func TestDeleteBucketIdempotentOn404(t *testing.T) {
	fake := &fakeBucketServer{t: t, deleteStatus: http.StatusNotFound}
	if err := fake.client().DeleteBucket(context.Background(), testBucketID); err != nil {
		t.Fatalf("DeleteBucket should treat 404 as success, got %v", err)
	}
}
