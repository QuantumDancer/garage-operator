package garageadmin

import (
	"context"
	"fmt"
)

// DesiredRole is the layout role the operator wants assigned to a single Garage node.
type DesiredRole struct {
	// NodeID is the Garage node identifier (its public key).
	NodeID string
	// Zone is the layout zone the node belongs to.
	Zone string
	// Capacity is the node's storage capacity in bytes, derived from storage.data.size.
	Capacity int64
	// Tags are optional administrator tags on the node.
	Tags []string
}

// NodeID returns the identifier of the Garage node answering at this client's endpoint.
// The client must target a single node's admin API (per-pod headless DNS), so the "self"
// query resolves to exactly that node.
func (c *AdminClient) NodeID(ctx context.Context) (string, error) {
	resp, err := c.GetNodeInfoWithResponse(ctx, &GetNodeInfoParams{Node: "self"})
	if err != nil {
		return "", err
	}
	if resp.JSON200 == nil {
		return "", fmt.Errorf("GetNodeInfo: unexpected status %s", resp.Status())
	}
	for id, info := range resp.JSON200.Success {
		if info.NodeId != "" {
			return info.NodeId, nil
		}
		return id, nil
	}
	for id, msg := range resp.JSON200.Error {
		return "", fmt.Errorf("GetNodeInfo: node %s reported error: %s", id, msg)
	}
	return "", fmt.Errorf("GetNodeInfo: empty response")
}

// Health returns the current cluster health (mirror of Garage's GetClusterHealth).
func (c *AdminClient) Health(ctx context.Context) (*GetClusterHealthResponse, error) {
	resp, err := c.GetClusterHealthWithResponse(ctx)
	if err != nil {
		return nil, err
	}
	if resp.JSON200 == nil {
		return nil, fmt.Errorf("GetClusterHealth: unexpected status %s", resp.Status())
	}
	return resp.JSON200, nil
}

// EnsureLayout converges the cluster layout so every node in desired holds its target role.
// It is additive-only: roles are assigned or updated, never removed, so a node missing from
// desired keeps whatever role it has (destructive changes are gated elsewhere). The call is
// idempotent — when the live layout already matches, no update is staged and applied stays
// false. It returns the layout version in effect afterwards.
func (c *AdminClient) EnsureLayout(ctx context.Context, desired []DesiredRole) (applied bool, version int64, err error) {
	layoutResp, err := c.GetClusterLayoutWithResponse(ctx)
	if err != nil {
		return false, 0, err
	}
	if layoutResp.JSON200 == nil {
		return false, 0, fmt.Errorf("GetClusterLayout: unexpected status %s", layoutResp.Status())
	}
	current := layoutResp.JSON200

	existing := make(map[string]LayoutNodeRole, len(current.Roles))
	for _, role := range current.Roles {
		existing[role.Id] = role
	}

	var changes []NodeRoleChangeRequest
	for _, want := range desired {
		if roleMatches(existing[want.NodeID], want) {
			continue
		}
		change, buildErr := assignRoleChange(want)
		if buildErr != nil {
			return false, current.Version, buildErr
		}
		changes = append(changes, change)
	}

	if len(changes) == 0 {
		return false, current.Version, nil
	}

	if _, err = c.UpdateClusterLayoutWithResponse(ctx, UpdateClusterLayoutRequest{Roles: &changes}); err != nil {
		return false, current.Version, err
	}

	// The new version is one past the version we observed; Garage requires it as a guard.
	newVersion := current.Version + 1
	applyResp, err := c.ApplyClusterLayoutWithResponse(ctx, ApplyClusterLayoutRequest{Version: newVersion})
	if err != nil {
		return false, current.Version, err
	}
	if applyResp.JSON200 == nil {
		return false, current.Version, fmt.Errorf("ApplyClusterLayout: unexpected status %s", applyResp.Status())
	}
	return true, newVersion, nil
}

// roleMatches reports whether an already-assigned role equals the desired one. A zero-value
// existing role (node not yet in the layout) never matches.
func roleMatches(existing LayoutNodeRole, want DesiredRole) bool {
	if existing.Id == "" {
		return false
	}
	if existing.Zone != want.Zone {
		return false
	}
	return existing.Capacity != nil && *existing.Capacity == want.Capacity
}

func assignRoleChange(want DesiredRole) (NodeRoleChangeRequest, error) {
	tags := want.Tags
	if tags == nil {
		tags = []string{}
	}
	capacity := want.Capacity
	var change NodeRoleChangeRequest
	err := change.FromNodeRoleChangeRequest1(NodeRoleChangeRequest1{
		Id:       want.NodeID,
		Zone:     want.Zone,
		Capacity: &capacity,
		Tags:     tags,
	})
	return change, err
}
