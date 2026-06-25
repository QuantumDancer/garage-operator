package garageadmin

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"k8s.io/utils/ptr"
)

// GetKeyByID looks an access key up by its id. found is false (with a nil error) when no such
// key exists, so callers can distinguish "absent" from a transport/API failure. The secret
// access key is never returned here — Garage only reveals it once, at creation.
func (c *AdminClient) GetKeyByID(ctx context.Context, id string) (info *GetKeyInfoResponse, found bool, err error) {
	resp, err := c.GetKeyInfoWithResponse(ctx, &GetKeyInfoParams{Id: &id})
	if err != nil {
		return nil, false, err
	}
	if resp.JSON200 != nil {
		return resp.JSON200, true, nil
	}
	if resp.StatusCode() == http.StatusNotFound {
		return nil, false, nil
	}
	return nil, false, fmt.Errorf("GetKeyInfo: unexpected status %s", resp.Status())
}

// CreateKey creates a new access key and returns its info, including the freshly generated
// secret access key (populated only on this create response).
func (c *AdminClient) CreateKey(ctx context.Context, body CreateKeyRequest) (*GetKeyInfoResponse, error) {
	resp, err := c.CreateKeyWithResponse(ctx, body)
	if err != nil {
		return nil, err
	}
	if resp.JSON200 == nil {
		return nil, fmt.Errorf("CreateKey: unexpected status %s", resp.Status())
	}
	return resp.JSON200, nil
}

// ImportKey adopts an existing key from caller-supplied credentials and returns its info.
func (c *AdminClient) ImportKey(ctx context.Context, body ImportKeyRequest) (*GetKeyInfoResponse, error) {
	resp, err := c.ImportKeyWithResponse(ctx, body)
	if err != nil {
		return nil, err
	}
	if resp.JSON200 == nil {
		return nil, fmt.Errorf("ImportKey: unexpected status %s", resp.Status())
	}
	return resp.JSON200, nil
}

// UpdateKey converges a key's name, permissions and expiration, returning the updated info.
func (c *AdminClient) UpdateKey(ctx context.Context, id string, body UpdateKeyRequestBody) (*GetKeyInfoResponse, error) {
	resp, err := c.UpdateKeyWithResponse(ctx, &UpdateKeyParams{Id: id}, body)
	if err != nil {
		return nil, err
	}
	if resp.JSON200 == nil {
		return nil, fmt.Errorf("UpdateKey: unexpected status %s", resp.Status())
	}
	return resp.JSON200, nil
}

// DeleteKey deletes a key by id. A 404 is treated as success so deletion is idempotent when
// the key is already gone (e.g. a retried finalizer pass).
func (c *AdminClient) DeleteKey(ctx context.Context, id string) error {
	resp, err := c.DeleteKeyWithResponse(ctx, &DeleteKeyParams{Id: id})
	if err != nil {
		return err
	}
	if resp.StatusCode() != http.StatusOK && resp.StatusCode() != http.StatusNoContent &&
		resp.StatusCode() != http.StatusNotFound {
		return fmt.Errorf("DeleteKey: unexpected status %s", resp.Status())
	}
	return nil
}

// AllowBucketKey grants the named permissions to a key on a bucket. Permissions left nil are
// untouched; Garage merges the granted permissions in.
func (c *AdminClient) AllowBucketKey(ctx context.Context, bucketID, accessKeyID string, perm ApiBucketKeyPerm) error {
	resp, err := c.AllowBucketKeyWithResponse(ctx, bucketKeyPermBody(bucketID, accessKeyID, perm))
	if err != nil {
		return err
	}
	if resp.JSON200 == nil {
		return fmt.Errorf("AllowBucketKey: unexpected status %s", resp.Status())
	}
	return nil
}

// DenyBucketKey revokes the named permissions from a key on a bucket.
func (c *AdminClient) DenyBucketKey(ctx context.Context, bucketID, accessKeyID string, perm ApiBucketKeyPerm) error {
	resp, err := c.DenyBucketKeyWithResponse(ctx, bucketKeyPermBody(bucketID, accessKeyID, perm))
	if err != nil {
		return err
	}
	if resp.JSON200 == nil {
		return fmt.Errorf("DenyBucketKey: unexpected status %s", resp.Status())
	}
	return nil
}

func bucketKeyPermBody(bucketID, accessKeyID string, perm ApiBucketKeyPerm) BucketKeyPermChangeRequest {
	return BucketKeyPermChangeRequest{BucketId: bucketID, AccessKeyId: accessKeyID, Permissions: perm}
}

// AddBucketLocalAlias adds a per-key (local) alias for a bucket, visible only to that key.
func (c *AdminClient) AddBucketLocalAlias(ctx context.Context, bucketID, accessKeyID, alias string) error {
	body, err := localAliasBody(bucketID, accessKeyID, alias)
	if err != nil {
		return err
	}
	resp, err := c.AddBucketAliasWithResponse(ctx, body)
	if err != nil {
		return err
	}
	if resp.JSON200 == nil {
		return fmt.Errorf("AddBucketAlias (local): unexpected status %s", resp.Status())
	}
	return nil
}

// RemoveBucketLocalAlias drops a per-key (local) alias from a bucket.
func (c *AdminClient) RemoveBucketLocalAlias(ctx context.Context, bucketID, accessKeyID, alias string) error {
	body, err := localAliasBody(bucketID, accessKeyID, alias)
	if err != nil {
		return err
	}
	resp, err := c.RemoveBucketAliasWithResponse(ctx, body)
	if err != nil {
		return err
	}
	if resp.JSON200 == nil {
		return fmt.Errorf("RemoveBucketAlias (local): unexpected status %s", resp.Status())
	}
	return nil
}

// localAliasBody builds the alias-union body for the local-alias variant (the second member
// of the shared Add/Remove alias union), which carries the owning access key id.
func localAliasBody(bucketID, accessKeyID, alias string) (BucketAliasEnum, error) {
	var body BucketAliasEnum
	err := body.FromBucketAliasEnum1(BucketAliasEnum1{BucketId: bucketID, AccessKeyId: accessKeyID, LocalAlias: alias})
	return body, err
}

// NewKeyUpdateBody assembles the request body shared by CreateKey and UpdateKey from the
// fields the operator manages: display name, the createBucket capability, and expiration.
// createBucket is expressed as a Garage allow/deny set so an unset capability is actively
// revoked (deny) rather than left to drift. A nil expiration reconciles to neverExpires.
func NewKeyUpdateBody(name string, createBucket bool, expiration *time.Time) (UpdateKeyRequestBody, error) {
	body := UpdateKeyRequestBody{Name: ptr.To(name)}

	perm := KeyPerm{CreateBucket: ptr.To(true)}
	if createBucket {
		var allow UpdateKeyRequestBody_Allow
		if err := allow.FromKeyPerm(perm); err != nil {
			return body, err
		}
		body.Allow = &allow
	} else {
		var deny UpdateKeyRequestBody_Deny
		if err := deny.FromKeyPerm(perm); err != nil {
			return body, err
		}
		body.Deny = &deny
	}

	if expiration != nil {
		body.Expiration = expiration
	} else {
		body.NeverExpires = ptr.To(true)
	}
	return body, nil
}
