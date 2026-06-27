package garageadmin

import (
	"context"
	"errors"
	"fmt"
	"slices"
)

// ErrUnknownRepairType is returned by LaunchRepair when the repair-type token is not one the
// operator recognizes. It is distinct from a transport error so callers can treat an invalid
// type as terminal (re-running the same value will never succeed) rather than retrying it.
var ErrUnknownRepairType = errors.New("unknown repair type")

// repairScrubCommands maps the operator's scrub repair tokens onto the Garage Admin API's
// RepairType {scrub: <command>} variant. The bare-string repairs (repairBareTypes) share
// their wire value with their token and need no translation.
var repairScrubCommands = map[string]ScrubCommand{
	"scrubStart":  Start,
	"scrubPause":  Pause,
	"scrubResume": Resume,
	"scrubCancel": Cancel,
}

// repairBareTypes are the repair tokens whose value is passed verbatim to the Admin API. They
// are keyed by the generated RepairType enum values so this set stays in lockstep with the spec.
var repairBareTypes = map[string]struct{}{
	string(Tables):           {},
	string(Blocks):           {},
	string(Versions):         {},
	string(MultipartUploads): {},
	string(BlockRefs):        {},
	string(BlockRc):          {},
	string(Rebalance):        {},
	string(Aliases):          {},
	string(ClearResyncQueue): {},
}

// IsValidRepairType reports whether token names a repair the operator can launch.
func IsValidRepairType(token string) bool {
	if _, ok := repairBareTypes[token]; ok {
		return true
	}
	_, ok := repairScrubCommands[token]
	return ok
}

// buildRepairType translates an operator repair token into the Admin API RepairType union.
// All bare-string variants share an identical wire form (a plain JSON string), so the choice
// of FromRepairType0 over the other string members is immaterial — only scrub needs the
// object variant.
func buildRepairType(token string) (RepairType, error) {
	var rt RepairType
	if cmd, ok := repairScrubCommands[token]; ok {
		err := rt.FromRepairType7(RepairType7{Scrub: cmd})
		return rt, err
	}
	if _, ok := repairBareTypes[token]; ok {
		err := rt.FromRepairType0(RepairType0(token))
		return rt, err
	}
	return rt, fmt.Errorf("%w: %q", ErrUnknownRepairType, token)
}

// MultiNodeResult aggregates the per-node outcome of a fan-out Admin API call (node="*").
// Succeeded holds the node ids that completed the call; Failed maps a node id to its error
// message. Garage returns HTTP 200 even when individual nodes fail, reporting the failure
// per-node, so callers inspect both maps rather than relying on the status code.
type MultiNodeResult struct {
	Succeeded []string
	Failed    map[string]string
}

// multiResult collapses the Success/Error maps of a Garage MultiResponse into a MultiNodeResult.
// The success payload is ignored — these operations return no meaningful body — so only the
// node ids matter.
func multiResult[V any](success map[string]V, failed map[string]string) MultiNodeResult {
	r := MultiNodeResult{Failed: failed}
	for id := range success {
		r.Succeeded = append(r.Succeeded, id)
	}
	slices.Sort(r.Succeeded)
	return r
}

// CreateMetadataSnapshot instructs the given node(s) to snapshot their metadata database.
// node is a Garage node id, "*" for every node, or "self" for the node serving the request.
// The snapshot is written to each node's local metadata volume; the Admin API exposes no way
// to enumerate, retrieve, or delete it, so only the per-node success/failure is returned.
func (c *AdminClient) CreateMetadataSnapshot(ctx context.Context, node string) (MultiNodeResult, error) {
	resp, err := c.CreateMetadataSnapshotWithResponse(ctx, &CreateMetadataSnapshotParams{Node: node})
	if err != nil {
		return MultiNodeResult{}, err
	}
	if resp.JSON200 == nil {
		return MultiNodeResult{}, fmt.Errorf("CreateMetadataSnapshot: unexpected status %s", resp.Status())
	}
	return multiResult(resp.JSON200.Success, resp.JSON200.Error), nil
}

// LaunchRepair launches a repair operation of the given token type on the given node(s).
// node follows the same convention as CreateMetadataSnapshot. The repair runs asynchronously
// in Garage's background workers; this call only enqueues it, so the returned result reflects
// whether each node accepted the request, not the repair's completion. An unrecognized token
// returns ErrUnknownRepairType without contacting Garage.
func (c *AdminClient) LaunchRepair(ctx context.Context, node, repairType string) (MultiNodeResult, error) {
	rt, err := buildRepairType(repairType)
	if err != nil {
		return MultiNodeResult{}, err
	}
	resp, err := c.LaunchRepairOperationWithResponse(ctx,
		&LaunchRepairOperationParams{Node: node},
		LocalLaunchRepairOperationRequest{RepairType: rt})
	if err != nil {
		return MultiNodeResult{}, err
	}
	if resp.JSON200 == nil {
		return MultiNodeResult{}, fmt.Errorf("LaunchRepairOperation: unexpected status %s", resp.Status())
	}
	return multiResult(resp.JSON200.Success, resp.JSON200.Error), nil
}

// WorkerSummary describes one Garage background worker on one node. It is the operator-facing
// projection of the Admin API's richer WorkerInfoResp, carrying only what the cluster status
// needs to report repair progress.
type WorkerSummary struct {
	// Node is the id of the node running the worker.
	Node string
	// Name is the worker's name (e.g. "block resync worker").
	Name string
	// State is a human-readable worker state ("busy", "throttled", "idle", "done").
	State string
	// Progress is the worker's free-form progress string, empty when it reports none.
	Progress string
}

// ListActiveWorkers returns the background workers across the given node(s) that are actively
// doing work — those in the "busy" or "throttled" state. Idle and done workers are omitted, so
// an empty result means no work is in flight. node follows the CreateMetadataSnapshot
// convention. Garage's worker list is the only progress signal for a launched repair, since
// LaunchRepair itself is fire-and-forget.
func (c *AdminClient) ListActiveWorkers(ctx context.Context, node string) ([]WorkerSummary, error) {
	resp, err := c.ListWorkersWithResponse(ctx, &ListWorkersParams{Node: node}, LocalListWorkersRequest{})
	if err != nil {
		return nil, err
	}
	if resp.JSON200 == nil {
		return nil, fmt.Errorf("ListWorkers: unexpected status %s", resp.Status())
	}

	nodes := make([]string, 0, len(resp.JSON200.Success))
	for n := range resp.JSON200.Success {
		nodes = append(nodes, n)
	}
	slices.Sort(nodes)

	var out []WorkerSummary
	for _, n := range nodes {
		for _, w := range resp.JSON200.Success[n] {
			label, active := workerState(w.State)
			if !active {
				continue
			}
			summary := WorkerSummary{Node: n, Name: w.Name, State: label}
			if w.Progress != nil {
				summary.Progress = *w.Progress
			}
			out = append(out, summary)
		}
	}
	return out, nil
}

// workerState renders a WorkerStateResp union into a label and reports whether the worker is
// actively doing work. The string members ("busy", "idle", "done") all decode through the
// first string variant; "throttled" is the lone object variant, so a failed string decode
// identifies it.
func workerState(s WorkerStateResp) (label string, active bool) {
	if str, err := s.AsWorkerStateResp0(); err == nil && str != "" {
		return string(str), str == "busy"
	}
	if _, err := s.AsWorkerStateResp1(); err == nil {
		return "throttled", true
	}
	return "unknown", false
}
