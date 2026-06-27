package garageadmin

import (
	"context"
	"fmt"
	"strings"
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

// ConnectNodes asks the node at this client's endpoint to open RPC connections to peers.
// Each peer is a Garage connect string, "<nodeID>@<host>:<rpcPort>". The call is idempotent:
// peers that are already connected report success. Garage gossip then propagates membership
// to the rest of the mesh, so connecting one node to all others is sufficient to form it.
func (c *AdminClient) ConnectNodes(ctx context.Context, peers []string) error {
	resp, err := c.ConnectClusterNodesWithResponse(ctx, peers)
	if err != nil {
		return err
	}
	if resp.JSON200 == nil {
		return fmt.Errorf("ConnectClusterNodes: unexpected status %s", resp.Status())
	}

	// Garage returns 200 even when an individual peer could not be reached, reporting the
	// failure per-peer. Surface those as an error so the caller retries instead of treating
	// an unformed mesh as success.
	var failures []string
	for i, node := range *resp.JSON200 {
		if node.Success {
			continue
		}
		msg := "unknown error"
		if node.Error != nil {
			msg = *node.Error
		}
		peer := "?"
		if i < len(peers) {
			peer = peers[i]
		}
		failures = append(failures, fmt.Sprintf("%s: %s", peer, msg))
	}
	if len(failures) > 0 {
		return fmt.Errorf("ConnectClusterNodes: %s", strings.Join(failures, "; "))
	}
	return nil
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

// LayoutPlan is the difference between the applied Garage layout and a desired set of
// roles. AdditiveChanges (new nodes, capacity/zone refinements) are safe to apply
// directly; Removals — node ids in the applied layout no longer desired — are
// destructive (they drain a node and trigger a rebalance) and are gated by the caller.
type LayoutPlan struct {
	// CurrentVersion is the applied layout version observed when the plan was computed.
	CurrentVersion int64
	// AdditiveChanges assign or refine roles without dropping any existing node.
	AdditiveChanges []NodeRoleChangeRequest
	// Removals are node ids present in the applied layout but absent from desired.
	Removals []string
	// TargetVersion is the layout version applying this plan would produce
	// (CurrentVersion + 1), or 0 when the plan changes nothing.
	TargetVersion int64
}

// HasChanges reports whether applying the plan would alter the layout.
func (p *LayoutPlan) HasChanges() bool {
	return len(p.AdditiveChanges) > 0 || len(p.Removals) > 0
}

// IsDestructive reports whether the plan removes any node from the layout.
func (p *LayoutPlan) IsDestructive() bool {
	return len(p.Removals) > 0
}

// StagedChanges returns every change the plan implies — additive assignments followed by
// removals — ready to hand to StageLayoutChanges.
func (p *LayoutPlan) StagedChanges() ([]NodeRoleChangeRequest, error) {
	changes := append([]NodeRoleChangeRequest(nil), p.AdditiveChanges...)
	for _, id := range p.Removals {
		change, err := removeRoleChange(id)
		if err != nil {
			return nil, err
		}
		changes = append(changes, change)
	}
	return changes, nil
}

// PlanLayout computes the difference between the applied layout and desired. It reads the
// applied roles (GetClusterLayout.Roles, which excludes any staged-but-unapplied changes),
// so the plan is stable even if a prior reconcile left changes staged.
func (c *AdminClient) PlanLayout(ctx context.Context, desired []DesiredRole) (*LayoutPlan, error) {
	layoutResp, err := c.GetClusterLayoutWithResponse(ctx)
	if err != nil {
		return nil, err
	}
	if layoutResp.JSON200 == nil {
		return nil, fmt.Errorf("GetClusterLayout: unexpected status %s", layoutResp.Status())
	}
	current := layoutResp.JSON200

	existing := make(map[string]LayoutNodeRole, len(current.Roles))
	for _, role := range current.Roles {
		existing[role.Id] = role
	}
	wanted := make(map[string]struct{}, len(desired))

	plan := &LayoutPlan{CurrentVersion: current.Version}
	for _, want := range desired {
		wanted[want.NodeID] = struct{}{}
		if roleMatches(existing[want.NodeID], want) {
			continue
		}
		change, buildErr := assignRoleChange(want)
		if buildErr != nil {
			return nil, buildErr
		}
		plan.AdditiveChanges = append(plan.AdditiveChanges, change)
	}
	for _, role := range current.Roles {
		if _, ok := wanted[role.Id]; !ok {
			plan.Removals = append(plan.Removals, role.Id)
		}
	}
	if plan.HasChanges() {
		plan.TargetVersion = current.Version + 1
	}
	return plan, nil
}

// StageLayoutChanges stages role changes into the layout's pending area (Garage's
// UpdateClusterLayout). Staged changes take effect only once applied, and can be inspected
// with PreviewStagedChanges or discarded with RevertStagedChanges. A no-op for empty input.
func (c *AdminClient) StageLayoutChanges(ctx context.Context, changes []NodeRoleChangeRequest) error {
	if len(changes) == 0 {
		return nil
	}
	resp, err := c.UpdateClusterLayoutWithResponse(ctx, UpdateClusterLayoutRequest{Roles: &changes})
	if err != nil {
		return err
	}
	if resp.JSON200 == nil {
		return fmt.Errorf("UpdateClusterLayout: unexpected status %s", resp.Status())
	}
	return nil
}

// PreviewStagedChanges computes the outcome of the currently staged changes and returns
// Garage's plain-text description of the resulting layout and rebalance. It mutates
// nothing. An empty stage or an uncomputable layout is reported as an error.
func (c *AdminClient) PreviewStagedChanges(ctx context.Context) ([]string, error) {
	resp, err := c.PreviewClusterLayoutChangesWithResponse(ctx)
	if err != nil {
		return nil, err
	}
	if resp.JSON200 == nil {
		return nil, fmt.Errorf("PreviewClusterLayoutChanges: unexpected status %s", resp.Status())
	}
	// The response is a union: an error variant (layout could not be computed) or the
	// success variant carrying the human-readable message lines.
	if failure, ferr := resp.JSON200.AsPreviewClusterLayoutChangesResponse0(); ferr == nil && failure.Error != "" {
		return nil, fmt.Errorf("PreviewClusterLayoutChanges: %s", failure.Error)
	}
	success, err := resp.JSON200.AsPreviewClusterLayoutChangesResponse1()
	if err != nil {
		return nil, err
	}
	return success.Message, nil
}

// ApplyLayout commits the staged changes as the given layout version. version must be one
// past the applied version observed in the plan; Garage requires it as a concurrency guard.
func (c *AdminClient) ApplyLayout(ctx context.Context, version int64) error {
	resp, err := c.ApplyClusterLayoutWithResponse(ctx, ApplyClusterLayoutRequest{Version: version})
	if err != nil {
		return err
	}
	if resp.JSON200 == nil {
		return fmt.Errorf("ApplyClusterLayout: unexpected status %s", resp.Status())
	}
	return nil
}

// RevertStagedChanges discards all staged-but-unapplied layout changes, returning the
// layout to its applied state. It is safe to call when nothing is staged.
func (c *AdminClient) RevertStagedChanges(ctx context.Context) error {
	resp, err := c.RevertClusterLayoutWithResponse(ctx)
	if err != nil {
		return err
	}
	if resp.JSON200 == nil {
		return fmt.Errorf("RevertClusterLayout: unexpected status %s", resp.Status())
	}
	return nil
}

// EnsureLayout converges the cluster layout so every node in desired holds its target role.
// It is additive-only: roles are assigned or updated, never removed, so a node missing from
// desired keeps whatever role it has (destructive removals are planned and gated by the
// caller via PlanLayout). The call is idempotent — when the applied layout already matches,
// nothing is staged and applied stays false. It returns the layout version in effect after.
func (c *AdminClient) EnsureLayout(ctx context.Context, desired []DesiredRole) (applied bool, version int64, err error) {
	plan, err := c.PlanLayout(ctx, desired)
	if err != nil {
		return false, 0, err
	}
	if len(plan.AdditiveChanges) == 0 {
		return false, plan.CurrentVersion, nil
	}
	if err = c.StageLayoutChanges(ctx, plan.AdditiveChanges); err != nil {
		return false, plan.CurrentVersion, err
	}
	if err = c.ApplyLayout(ctx, plan.TargetVersion); err != nil {
		return false, plan.CurrentVersion, err
	}
	return true, plan.TargetVersion, nil
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

// removeRoleChange builds the staged change that drops a node from the layout. Garage
// drains the node and redistributes its data to the remaining replicas on apply.
func removeRoleChange(nodeID string) (NodeRoleChangeRequest, error) {
	var change NodeRoleChangeRequest
	err := change.FromNodeRoleChangeRequest0(NodeRoleChangeRequest0{
		Id:     nodeID,
		Remove: true,
	})
	return change, err
}
