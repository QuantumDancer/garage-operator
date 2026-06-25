package garageadmin

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

const (
	testKeyID     = "GK0123"
	testKeySecret = "s3cr3t"
)

// fakeKeyServer is a minimal Garage Admin API stand-in for the key + bucket-key endpoints.
// Each handler records what it received so tests can assert request shape.
type fakeKeyServer struct {
	t *testing.T

	// key, when non-nil, is returned from GetKeyInfo; nil yields a 404.
	key *GetKeyInfoResponse

	createBodies []UpdateKeyRequestBody
	importBodies []ImportKeyRequest
	updateBodies []UpdateKeyRequestBody
	deletedIDs   []string
	deleteStatus int // status DeleteKey returns; defaults to 200

	allowed      []BucketKeyPermChangeRequest
	denied       []BucketKeyPermChangeRequest
	addedAlias   []BucketAliasEnum1
	removedAlias []BucketAliasEnum1
}

func (f *fakeKeyServer) client() *AdminClient {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/v2/GetKeyInfo":
			if f.key == nil {
				http.Error(w, `{"error":"not found"}`, http.StatusNotFound)
				return
			}
			f.encode(w, f.key)
		case "/v2/CreateKey":
			var body UpdateKeyRequestBody
			f.decode(r, &body)
			f.createBodies = append(f.createBodies, body)
			name := ""
			if body.Name != nil {
				name = *body.Name
			}
			secret := testKeySecret
			f.encode(w, GetKeyInfoResponse{AccessKeyId: testKeyID, Name: name, SecretAccessKey: &secret})
		case "/v2/ImportKey":
			var body ImportKeyRequest
			f.decode(r, &body)
			f.importBodies = append(f.importBodies, body)
			f.encode(w, GetKeyInfoResponse{AccessKeyId: body.AccessKeyId})
		case "/v2/UpdateKey":
			var body UpdateKeyRequestBody
			f.decode(r, &body)
			f.updateBodies = append(f.updateBodies, body)
			f.encode(w, GetKeyInfoResponse{AccessKeyId: r.URL.Query().Get("id"), Expired: false})
		case "/v2/DeleteKey":
			f.deletedIDs = append(f.deletedIDs, r.URL.Query().Get("id"))
			status := f.deleteStatus
			if status == 0 {
				status = http.StatusOK
			}
			w.WriteHeader(status)
		case "/v2/AllowBucketKey":
			var body BucketKeyPermChangeRequest
			f.decode(r, &body)
			f.allowed = append(f.allowed, body)
			f.encode(w, GetBucketInfoResponse{Id: body.BucketId})
		case "/v2/DenyBucketKey":
			var body BucketKeyPermChangeRequest
			f.decode(r, &body)
			f.denied = append(f.denied, body)
			f.encode(w, GetBucketInfoResponse{Id: body.BucketId})
		case "/v2/AddBucketAlias":
			f.addedAlias = append(f.addedAlias, f.decodeLocalAlias(r))
			f.encode(w, GetBucketInfoResponse{Id: testBucketID})
		case "/v2/RemoveBucketAlias":
			f.removedAlias = append(f.removedAlias, f.decodeLocalAlias(r))
			f.encode(w, GetBucketInfoResponse{Id: testBucketID})
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

func (f *fakeKeyServer) decodeLocalAlias(r *http.Request) BucketAliasEnum1 {
	var body BucketAliasEnum
	f.decode(r, &body)
	local, err := body.AsBucketAliasEnum1()
	if err != nil {
		f.t.Fatalf("decode local alias union: %v", err)
	}
	return local
}

func (f *fakeKeyServer) encode(w http.ResponseWriter, v any) {
	if err := json.NewEncoder(w).Encode(v); err != nil {
		f.t.Fatalf("encode response: %v", err)
	}
}

func (f *fakeKeyServer) decode(r *http.Request, v any) {
	if err := json.NewDecoder(r.Body).Decode(v); err != nil {
		f.t.Fatalf("decode request: %v", err)
	}
}

func TestGetKeyByIDFound(t *testing.T) {
	fake := &fakeKeyServer{t: t, key: &GetKeyInfoResponse{AccessKeyId: testKeyID, Name: "photos-rw"}}
	info, found, err := fake.client().GetKeyByID(context.Background(), testKeyID)
	if err != nil {
		t.Fatalf("GetKeyByID: %v", err)
	}
	if !found || info == nil || info.AccessKeyId != testKeyID {
		t.Fatalf("got found=%v info=%+v, want found key %s", found, info, testKeyID)
	}
}

func TestGetKeyByIDNotFound(t *testing.T) {
	fake := &fakeKeyServer{t: t, key: nil}
	_, found, err := fake.client().GetKeyByID(context.Background(), "missing")
	if err != nil {
		t.Fatalf("GetKeyByID: %v", err)
	}
	if found {
		t.Fatal("found = true, want not found")
	}
}

func TestCreateKeyReturnsSecret(t *testing.T) {
	fake := &fakeKeyServer{t: t}
	body, err := NewKeyUpdateBody("photos-rw", true, nil)
	if err != nil {
		t.Fatalf("NewKeyUpdateBody: %v", err)
	}
	info, err := fake.client().CreateKey(context.Background(), body)
	if err != nil {
		t.Fatalf("CreateKey: %v", err)
	}
	if info.SecretAccessKey == nil || *info.SecretAccessKey != testKeySecret {
		t.Fatalf("secret = %v, want %s returned once on create", info.SecretAccessKey, testKeySecret)
	}
	if len(fake.createBodies) != 1 || fake.createBodies[0].Allow == nil {
		t.Fatalf("create body = %+v, want createBucket granted via Allow", fake.createBodies)
	}
}

func TestImportKeySendsCredentials(t *testing.T) {
	fake := &fakeKeyServer{t: t}
	info, err := fake.client().ImportKey(context.Background(), ImportKeyRequest{
		AccessKeyId: testKeyID, SecretAccessKey: testKeySecret,
	})
	if err != nil {
		t.Fatalf("ImportKey: %v", err)
	}
	if info.AccessKeyId != testKeyID {
		t.Errorf("accessKeyId = %q, want %s", info.AccessKeyId, testKeyID)
	}
	if len(fake.importBodies) != 1 || fake.importBodies[0].SecretAccessKey != testKeySecret {
		t.Errorf("import body = %+v, want secret forwarded", fake.importBodies)
	}
}

func TestUpdateKeyNeverExpires(t *testing.T) {
	fake := &fakeKeyServer{t: t}
	body, err := NewKeyUpdateBody("photos-rw", false, nil)
	if err != nil {
		t.Fatalf("NewKeyUpdateBody: %v", err)
	}
	if _, err := fake.client().UpdateKey(context.Background(), testKeyID, body); err != nil {
		t.Fatalf("UpdateKey: %v", err)
	}
	got := fake.updateBodies[0]
	if got.NeverExpires == nil || !*got.NeverExpires {
		t.Errorf("neverExpires = %v, want true when expiration omitted", got.NeverExpires)
	}
	if got.Deny == nil {
		t.Errorf("want createBucket revoked via Deny when permission is false")
	}
}

func TestUpdateKeyWithExpiration(t *testing.T) {
	fake := &fakeKeyServer{t: t}
	exp := time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC)
	body, err := NewKeyUpdateBody("k", false, &exp)
	if err != nil {
		t.Fatalf("NewKeyUpdateBody: %v", err)
	}
	if _, err := fake.client().UpdateKey(context.Background(), testKeyID, body); err != nil {
		t.Fatalf("UpdateKey: %v", err)
	}
	got := fake.updateBodies[0]
	if got.Expiration == nil || !got.Expiration.Equal(exp) {
		t.Errorf("expiration = %v, want %v", got.Expiration, exp)
	}
	if got.NeverExpires != nil {
		t.Errorf("neverExpires = %v, want nil when expiration is set", got.NeverExpires)
	}
}

func TestDeleteKeyIdempotentOn404(t *testing.T) {
	fake := &fakeKeyServer{t: t, deleteStatus: http.StatusNotFound}
	if err := fake.client().DeleteKey(context.Background(), testKeyID); err != nil {
		t.Fatalf("DeleteKey should treat 404 as success, got %v", err)
	}
}

func TestAllowAndDenyBucketKey(t *testing.T) {
	fake := &fakeKeyServer{t: t}
	c := fake.client()
	yes := true
	if err := c.AllowBucketKey(context.Background(), testBucketID, testKeyID, ApiBucketKeyPerm{Read: &yes, Write: &yes}); err != nil {
		t.Fatalf("AllowBucketKey: %v", err)
	}
	if err := c.DenyBucketKey(context.Background(), testBucketID, testKeyID, ApiBucketKeyPerm{Owner: &yes}); err != nil {
		t.Fatalf("DenyBucketKey: %v", err)
	}
	if len(fake.allowed) != 1 || fake.allowed[0].AccessKeyId != testKeyID || fake.allowed[0].Permissions.Read == nil {
		t.Errorf("allowed = %+v, want read/write granted to %s", fake.allowed, testKeyID)
	}
	if len(fake.denied) != 1 || fake.denied[0].Permissions.Owner == nil {
		t.Errorf("denied = %+v, want owner revoked", fake.denied)
	}
}

func TestAddAndRemoveLocalAlias(t *testing.T) {
	fake := &fakeKeyServer{t: t}
	c := fake.client()
	if err := c.AddBucketLocalAlias(context.Background(), testBucketID, testKeyID, "photos"); err != nil {
		t.Fatalf("AddBucketLocalAlias: %v", err)
	}
	if err := c.RemoveBucketLocalAlias(context.Background(), testBucketID, testKeyID, "old"); err != nil {
		t.Fatalf("RemoveBucketLocalAlias: %v", err)
	}
	if len(fake.addedAlias) != 1 || fake.addedAlias[0].LocalAlias != "photos" || fake.addedAlias[0].AccessKeyId != testKeyID {
		t.Errorf("added = %+v, want local alias photos for %s", fake.addedAlias, testKeyID)
	}
	if len(fake.removedAlias) != 1 || fake.removedAlias[0].LocalAlias != "old" {
		t.Errorf("removed = %+v, want local alias old", fake.removedAlias)
	}
}
