package garageadmin

import (
	"context"
	"fmt"
	"net/http"
)

// GetBucketByID looks a bucket up by its Garage id. found is false (with a nil error) when
// no such bucket exists, so callers can distinguish "absent" from a transport/API failure.
func (c *AdminClient) GetBucketByID(ctx context.Context, id string) (info *GetBucketInfoResponse, found bool, err error) {
	return c.getBucket(ctx, &GetBucketInfoParams{Id: &id})
}

// GetBucketByGlobalAlias looks a bucket up by one of its cluster-wide aliases. found is
// false (with a nil error) when no bucket carries the alias.
func (c *AdminClient) GetBucketByGlobalAlias(ctx context.Context, alias string) (info *GetBucketInfoResponse, found bool, err error) {
	return c.getBucket(ctx, &GetBucketInfoParams{GlobalAlias: &alias})
}

func (c *AdminClient) getBucket(ctx context.Context, params *GetBucketInfoParams) (*GetBucketInfoResponse, bool, error) {
	resp, err := c.GetBucketInfoWithResponse(ctx, params)
	if err != nil {
		return nil, false, err
	}
	if resp.JSON200 != nil {
		return resp.JSON200, true, nil
	}
	if resp.StatusCode() == http.StatusNotFound {
		return nil, false, nil
	}
	return nil, false, fmt.Errorf("GetBucketInfo: unexpected status %s", resp.Status())
}

// CreateBucket creates a bucket, optionally with an initial global alias (pass "" for none),
// and returns the new bucket's id.
func (c *AdminClient) CreateBucket(ctx context.Context, globalAlias string) (string, error) {
	var req CreateBucketRequest
	if globalAlias != "" {
		req.GlobalAlias = &globalAlias
	}
	resp, err := c.CreateBucketWithResponse(ctx, req)
	if err != nil {
		return "", err
	}
	if resp.JSON200 == nil {
		return "", fmt.Errorf("CreateBucket: unexpected status %s", resp.Status())
	}
	return resp.JSON200.Id, nil
}

// UpdateBucket applies website, quotas, CORS and lifecycle settings to a bucket. Nil fields
// on body leave the corresponding setting unchanged; non-nil fields replace it wholesale.
func (c *AdminClient) UpdateBucket(ctx context.Context, id string, body UpdateBucketRequestBody) error {
	resp, err := c.UpdateBucketWithResponse(ctx, &UpdateBucketParams{Id: id}, body)
	if err != nil {
		return err
	}
	if resp.JSON200 == nil {
		return fmt.Errorf("UpdateBucket: unexpected status %s", resp.Status())
	}
	return nil
}

// AddBucketGlobalAlias adds a cluster-wide alias to a bucket. Idempotent: re-adding an
// existing alias is a no-op on the Garage side.
func (c *AdminClient) AddBucketGlobalAlias(ctx context.Context, bucketID, alias string) error {
	body, err := globalAliasBody(bucketID, alias)
	if err != nil {
		return err
	}
	resp, err := c.AddBucketAliasWithResponse(ctx, body)
	if err != nil {
		return err
	}
	if resp.JSON200 == nil {
		return fmt.Errorf("AddBucketAlias: unexpected status %s", resp.Status())
	}
	return nil
}

// RemoveBucketGlobalAlias drops a cluster-wide alias from a bucket.
func (c *AdminClient) RemoveBucketGlobalAlias(ctx context.Context, bucketID, alias string) error {
	body, err := globalAliasBody(bucketID, alias)
	if err != nil {
		return err
	}
	resp, err := c.RemoveBucketAliasWithResponse(ctx, body)
	if err != nil {
		return err
	}
	if resp.JSON200 == nil {
		return fmt.Errorf("RemoveBucketAlias: unexpected status %s", resp.Status())
	}
	return nil
}

// globalAliasBody builds the alias-union body for the global-alias variant. The Add/Remove
// alias endpoints share one union request type whose first member is the global alias.
func globalAliasBody(bucketID, alias string) (BucketAliasEnum, error) {
	var body BucketAliasEnum
	err := body.FromBucketAliasEnum0(BucketAliasEnum0{BucketId: bucketID, GlobalAlias: alias})
	return body, err
}

// DeleteBucket deletes a bucket by id. Garage refuses to delete a non-empty bucket, which
// surfaces here as a non-200 status; callers gate deletion on emptiness beforehand. A 404 is
// treated as success so deletion is idempotent when the bucket is already gone (e.g. a
// retried finalizer pass).
func (c *AdminClient) DeleteBucket(ctx context.Context, id string) error {
	resp, err := c.DeleteBucketWithResponse(ctx, &DeleteBucketParams{Id: id})
	if err != nil {
		return err
	}
	if resp.StatusCode() != http.StatusOK && resp.StatusCode() != http.StatusNoContent &&
		resp.StatusCode() != http.StatusNotFound {
		return fmt.Errorf("DeleteBucket: unexpected status %s", resp.Status())
	}
	return nil
}
