package memgraph

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// PutSubgraph bulk-write limits. Exported so adapter packages (the MCP server,
// downstream tenant-scoped wrappers like memorysvc) share one source of truth.
const (
	// PutSubgraphMaxNodes caps the number of nodes that may be written in a
	// single PutSubgraph call.
	PutSubgraphMaxNodes = 50
	// PutSubgraphMaxEdges caps the number of edges that may be written in a
	// single PutSubgraph call.
	PutSubgraphMaxEdges = 100
)

// NodeSpec is one node in a PutSubgraph batch. A node is an UPDATE (new
// version on an existing lineage) when LineageID is non-empty; otherwise it
// is a CREATE (new lineage). The optional Ref is a per-call symbol other
// nodes' edges can target before any lineage_id is known.
type NodeSpec struct {
	// Ref is an opaque, per-call identifier. Edges in the same batch can
	// target this node via from_ref / to_ref before its lineage_id has been
	// assigned. Optional; only meaningful inside a single PutSubgraph call.
	Ref string `json:"ref,omitempty"`

	// LineageID, if set, makes this an UPDATE on an existing lineage.
	// If empty, a new lineage is created.
	LineageID string `json:"lineage_id,omitempty"`

	// BasedOnVersion is the optimistic-concurrency hint passed to PutNode
	// when LineageID is set. Ignored on creates.
	BasedOnVersion *int `json:"based_on_version,omitempty"`

	// Standard node fields (mirror NodeInput).
	Kind        string         `json:"kind"`
	Content     string         `json:"content"`
	Summary     string         `json:"summary,omitempty"`
	Tags        []string       `json:"tags,omitempty"`
	Metadata    map[string]any `json:"metadata,omitempty"`
	FreshnessAt *time.Time     `json:"freshness_at,omitempty"`
	CreatedBy   string         `json:"created_by,omitempty"`
}

// EdgeSpec is one edge in a PutSubgraph batch. Each endpoint is identified by
// EITHER a per-call ref (resolved against nodes earlier in this batch) OR an
// existing lineage_id. Providing both on the same side is an error and that
// edge will fail with a per-item error message.
type EdgeSpec struct {
	// FromRef refers to a node earlier in this batch by Ref.
	FromRef string `json:"from_ref,omitempty"`
	// FromLineage refers to an existing lineage by id.
	FromLineage string `json:"from_lineage,omitempty"`

	// ToRef refers to a node earlier in this batch by Ref.
	ToRef string `json:"to_ref,omitempty"`
	// ToLineage refers to an existing lineage by id.
	ToLineage string `json:"to_lineage,omitempty"`
	// ToGraph optionally pins a cross-graph symlink target; defaults to the
	// batch's GraphID.
	ToGraph string `json:"to_graph,omitempty"`

	// Standard edge fields (mirror EdgeInput).
	Kind      string         `json:"kind"`
	Metadata  map[string]any `json:"metadata,omitempty"`
	Ordinal   *int           `json:"ordinal,omitempty"`
	CreatedBy string         `json:"created_by,omitempty"`
}

// PutSubgraphInput is the payload accepted by PutSubgraph. At least one of
// Nodes or Edges must be non-empty. Limits: PutSubgraphMaxNodes nodes and
// PutSubgraphMaxEdges edges per call.
type PutSubgraphInput struct {
	GraphID GraphID    `json:"graph_id"`
	Nodes   []NodeSpec `json:"nodes,omitempty"`
	Edges   []EdgeSpec `json:"edges,omitempty"`
}

// NodeResult is the per-item outcome for one NodeSpec. Ref echoes the input
// ref (if any). LineageID and Version are populated on success. Created is
// true when a new lineage was started; false when an existing lineage gained
// a new version. Error, when non-empty, describes why this specific item
// failed; subsequent edges referencing this Ref will also fail.
type NodeResult struct {
	Ref       string `json:"ref,omitempty"`
	LineageID string `json:"lineage_id,omitempty"`
	Version   int    `json:"version,omitempty"`
	Created   bool   `json:"created"`
	Error     string `json:"error,omitempty"`
}

// EdgeResult is the per-item outcome for one EdgeSpec.
type EdgeResult struct {
	EdgeID string `json:"edge_id,omitempty"`
	Error  string `json:"error,omitempty"`
}

// PutSubgraphOutput is the best-effort response: one NodeResult per input
// node (in input order) and one EdgeResult per input edge (in input order).
type PutSubgraphOutput struct {
	Nodes []NodeResult `json:"nodes"`
	Edges []EdgeResult `json:"edges"`
}

// PutSubgraph writes a batch of nodes and edges in two passes against an
// existing Store, exposing per-item results so callers can retry only the
// failures. Semantics:
//
//   - Pass 1 walks Nodes in order. Each NodeSpec is dispatched to
//     Store.PutNode. Successful results with a Ref are recorded so edges in
//     pass 2 can resolve from_ref/to_ref. Failures populate NodeResult.Error
//     and the ref (if any) is NOT recorded.
//
//   - Pass 2 walks Edges in order. Each EdgeSpec is validated locally —
//     exactly one of from_ref / from_lineage and exactly one of to_ref /
//     to_lineage must be set; refs must resolve against pass-1 results.
//     Valid edges are dispatched to Store.PutEdge; failures populate
//     EdgeResult.Error.
//
// The batch is best-effort: no transaction wraps the underlying primitives,
// so partial success is the norm. Validation of the overall shape (graph_id
// present, nodes ≤ PutSubgraphMaxNodes, edges ≤ PutSubgraphMaxEdges, at
// least one of either) is performed up front and returned as a single error
// before any writes occur.
//
// PutSubgraph returns (PutSubgraphOutput, nil) once any writes are attempted;
// per-item errors are inside the result, never the outer error. The only
// returned error case is shape validation.
func PutSubgraph(ctx context.Context, store Store, graphID GraphID, in PutSubgraphInput) (PutSubgraphOutput, error) {
	if graphID == "" {
		return PutSubgraphOutput{}, fmt.Errorf("%w: graph_id required", ErrInvalidInput)
	}
	if len(in.Nodes) == 0 && len(in.Edges) == 0 {
		return PutSubgraphOutput{}, fmt.Errorf("%w: at least one of nodes or edges is required", ErrInvalidInput)
	}
	if len(in.Nodes) > PutSubgraphMaxNodes {
		return PutSubgraphOutput{}, fmt.Errorf("%w: nodes must contain at most %d entries", ErrInvalidInput, PutSubgraphMaxNodes)
	}
	if len(in.Edges) > PutSubgraphMaxEdges {
		return PutSubgraphOutput{}, fmt.Errorf("%w: edges must contain at most %d entries", ErrInvalidInput, PutSubgraphMaxEdges)
	}

	out := PutSubgraphOutput{
		Nodes: make([]NodeResult, len(in.Nodes)),
		Edges: make([]EdgeResult, len(in.Edges)),
	}

	// Pass 1 — nodes. Build ref -> resolved lineage id for use in pass 2.
	// A node's ref is only recorded on success; an edge referencing the ref
	// of a failed node therefore fails with "ref not found: <ref>" exactly
	// as if the ref had never been declared.
	refToLineage := make(map[string]LineageID, len(in.Nodes))

	for i, ns := range in.Nodes {
		res := NodeResult{Ref: ns.Ref}
		// A node with LineageID is an update; without is a create.
		isCreate := ns.LineageID == ""

		n, err := store.PutNode(ctx, NodeInput{
			GraphID:        graphID,
			LineageID:      LineageID(ns.LineageID),
			Kind:           ns.Kind,
			Content:        ns.Content,
			Summary:        ns.Summary,
			Tags:           ns.Tags,
			Metadata:       ns.Metadata,
			FreshnessAt:    ns.FreshnessAt,
			CreatedBy:      ns.CreatedBy,
			BasedOnVersion: ns.BasedOnVersion,
		})
		switch {
		case err == nil:
			res.LineageID = string(n.LineageID)
			res.Version = n.Version
			res.Created = isCreate
			if ns.Ref != "" {
				refToLineage[ns.Ref] = n.LineageID
			}
		case errors.Is(err, ErrConflictManual):
			// Manual conflict is a successful write that records a sibling.
			// Surface the sibling so callers can resolve it, AND mark the
			// item as errored so edges referencing the ref also fail (the
			// agent likely doesn't want to link into an unresolved fork).
			res.LineageID = string(n.LineageID)
			res.Version = n.Version
			res.Created = false
			res.Error = err.Error()
		default:
			res.Error = err.Error()
		}
		out.Nodes[i] = res
	}

	// Pass 2 — edges.
	for i, es := range in.Edges {
		var res EdgeResult

		// Endpoint validation: exactly one side identifier per side.
		if (es.FromRef != "") == (es.FromLineage != "") {
			if es.FromRef != "" && es.FromLineage != "" {
				res.Error = "only one of from_ref/from_lineage allowed"
			} else {
				res.Error = "one of from_ref/from_lineage required"
			}
			out.Edges[i] = res
			continue
		}
		if (es.ToRef != "") == (es.ToLineage != "") {
			if es.ToRef != "" && es.ToLineage != "" {
				res.Error = "only one of to_ref/to_lineage allowed"
			} else {
				res.Error = "one of to_ref/to_lineage required"
			}
			out.Edges[i] = res
			continue
		}

		// Resolve endpoints. Refs must hit a successful pass-1 write; refs of
		// failed nodes are reported the same way as never-declared refs.
		var fromL LineageID
		if es.FromRef != "" {
			lid, ok := refToLineage[es.FromRef]
			if !ok {
				res.Error = fmt.Sprintf("ref not found: %s", es.FromRef)
				out.Edges[i] = res
				continue
			}
			fromL = lid
		} else {
			fromL = LineageID(es.FromLineage)
		}

		var toL LineageID
		if es.ToRef != "" {
			lid, ok := refToLineage[es.ToRef]
			if !ok {
				res.Error = fmt.Sprintf("ref not found: %s", es.ToRef)
				out.Edges[i] = res
				continue
			}
			toL = lid
		} else {
			toL = LineageID(es.ToLineage)
		}

		e, err := store.PutEdge(ctx, EdgeInput{
			GraphID:     graphID,
			FromLineage: fromL,
			ToGraph:     GraphID(es.ToGraph),
			ToLineage:   toL,
			Kind:        es.Kind,
			Metadata:    es.Metadata,
			Ordinal:     es.Ordinal,
			CreatedBy:   es.CreatedBy,
		})
		if err != nil {
			res.Error = err.Error()
		} else {
			res.EdgeID = string(e.ID)
		}
		out.Edges[i] = res
	}

	return out, nil
}
