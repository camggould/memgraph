package mcp

import (
	"fmt"
	"testing"

	sdkmcp "github.com/modelcontextprotocol/go-sdk/mcp"
)

// ---- put_subgraph DTO mirrors (the real types are unexported in this package
// so tests redeclare just enough to decode StructuredContent.) ----

type tPutSubgraphNodeRes struct {
	Ref       string `json:"ref,omitempty"`
	LineageID string `json:"lineage_id,omitempty"`
	Version   int    `json:"version,omitempty"`
	Created   bool   `json:"created"`
	Error     string `json:"error,omitempty"`
}

type tPutSubgraphEdgeRes struct {
	EdgeID string `json:"edge_id,omitempty"`
	Error  string `json:"error,omitempty"`
}

type tPutSubgraphOut struct {
	Nodes []tPutSubgraphNodeRes `json:"nodes"`
	Edges []tPutSubgraphEdgeRes `json:"edges"`
}

// TestPutSubgraph_HappyPath_RefResolution covers the canonical use case: a
// fresh node with a ref, a second fresh node with a ref, an update of an
// existing lineage, and edges that wire refs to refs and a ref to an existing
// lineage. Every result must succeed and refs must resolve to the right
// lineage ids.
func TestPutSubgraph_HappyPath_RefResolution(t *testing.T) {
	store := openStore(t)
	cs, ctx := connect(t, store)

	var g graphOut
	callTool(t, cs, ctx, "memgraph_create_graph", map[string]any{"name": "g"}, &g)

	// Seed an existing lineage so we can exercise the update path.
	var existing nodeOut
	callTool(t, cs, ctx, "memgraph_put_node", map[string]any{
		"graph_id": g.ID, "kind": "fact", "content": "pre-existing fact",
	}, &existing)

	var out tPutSubgraphOut
	callTool(t, cs, ctx, "memgraph_put_subgraph", map[string]any{
		"graph_id": g.ID,
		"nodes": []map[string]any{
			{"ref": "decision", "kind": "decision", "content": "use postgres on render"},
			{"ref": "rationale", "kind": "fact", "content": "postgres has FTS we need"},
			{"lineage_id": existing.LineageID, "kind": "fact", "content": "pre-existing fact (refined)"},
		},
		"edges": []map[string]any{
			{"from_ref": "decision", "to_ref": "rationale", "kind": "because"},
			{"from_ref": "decision", "to_lineage": existing.LineageID, "kind": "cites"},
		},
	}, &out)

	if len(out.Nodes) != 3 || len(out.Edges) != 2 {
		t.Fatalf("unexpected output shape: %+v", out)
	}
	// Node 0: new lineage created via ref "decision".
	if out.Nodes[0].Error != "" || out.Nodes[0].Ref != "decision" || !out.Nodes[0].Created || out.Nodes[0].Version != 1 || out.Nodes[0].LineageID == "" {
		t.Fatalf("node[0] (decision) bad: %+v", out.Nodes[0])
	}
	// Node 1: new lineage created via ref "rationale".
	if out.Nodes[1].Error != "" || out.Nodes[1].Ref != "rationale" || !out.Nodes[1].Created || out.Nodes[1].Version != 1 {
		t.Fatalf("node[1] (rationale) bad: %+v", out.Nodes[1])
	}
	// Node 2: update on the existing lineage; Created should be false, version 2.
	if out.Nodes[2].Error != "" || out.Nodes[2].Created || out.Nodes[2].LineageID != existing.LineageID || out.Nodes[2].Version != 2 {
		t.Fatalf("node[2] (update) bad: %+v", out.Nodes[2])
	}
	// Edges: both must have edge_id and no error.
	if out.Edges[0].Error != "" || out.Edges[0].EdgeID == "" {
		t.Fatalf("edge[0] bad: %+v", out.Edges[0])
	}
	if out.Edges[1].Error != "" || out.Edges[1].EdgeID == "" {
		t.Fatalf("edge[1] bad: %+v", out.Edges[1])
	}

	// Cross-check: traversing from the new decision lineage should reach
	// both the rationale (via ref) and the existing lineage (via lineage_id).
	var tr traverseOut
	callTool(t, cs, ctx, "memgraph_traverse", map[string]any{
		"from_lineage": out.Nodes[0].LineageID,
		"max_depth":    1,
	}, &tr)
	want := map[string]bool{out.Nodes[1].LineageID: false, existing.LineageID: false}
	for _, n := range tr.Nodes {
		if _, ok := want[n.LineageID]; ok {
			want[n.LineageID] = true
		}
	}
	for lid, hit := range want {
		if !hit {
			t.Fatalf("traversal missed lineage %s; got nodes=%+v", lid, tr.Nodes)
		}
	}
}

// TestPutSubgraph_PartialFailure_BadKindThenDependentEdge: one node fails
// (empty kind under a kind whitelist) and an edge that depended on its ref
// must also fail with "ref not found: <ref>", while other items succeed.
func TestPutSubgraph_PartialFailure_BadKindThenDependentEdge(t *testing.T) {
	store := openStore(t)
	cs, ctx := connect(t, store)

	// Use a kind whitelist so we can deterministically force a node failure.
	var g graphOut
	callTool(t, cs, ctx, "memgraph_create_graph", map[string]any{
		"name":           "g",
		"kind_whitelist": []string{"fact"},
	}, &g)

	var out tPutSubgraphOut
	callTool(t, cs, ctx, "memgraph_put_subgraph", map[string]any{
		"graph_id": g.ID,
		"nodes": []map[string]any{
			{"ref": "ok", "kind": "fact", "content": "good node"},
			{"ref": "bad", "kind": "not-in-whitelist", "content": "kind-rejected node"},
		},
		"edges": []map[string]any{
			// Should fail: depends on the rejected node's ref.
			{"from_ref": "ok", "to_ref": "bad", "kind": "cites"},
			// Should succeed: only depends on the good node.
			{"from_ref": "ok", "to_ref": "ok", "kind": "self"},
		},
	}, &out)

	if len(out.Nodes) != 2 || len(out.Edges) != 2 {
		t.Fatalf("shape: %+v", out)
	}
	if out.Nodes[0].Error != "" || out.Nodes[0].LineageID == "" {
		t.Fatalf("good node should succeed: %+v", out.Nodes[0])
	}
	if out.Nodes[1].Error == "" {
		t.Fatalf("bad-kind node should fail: %+v", out.Nodes[1])
	}
	// Edge 0 depended on the failed ref.
	if out.Edges[0].Error == "" || out.Edges[0].EdgeID != "" {
		t.Fatalf("edge[0] should fail with ref-not-found, got %+v", out.Edges[0])
	}
	if got := out.Edges[0].Error; got != "ref not found: bad" {
		t.Fatalf("edge[0].error = %q want %q", got, "ref not found: bad")
	}
	// Edge 1 only depended on the surviving ref.
	if out.Edges[1].Error != "" || out.Edges[1].EdgeID == "" {
		t.Fatalf("edge[1] should succeed: %+v", out.Edges[1])
	}
}

// TestPutSubgraph_UnknownRef: an edge referencing a ref that was never
// declared fails with "ref not found: <ref>" and the call doesn't crash.
func TestPutSubgraph_UnknownRef(t *testing.T) {
	store := openStore(t)
	cs, ctx := connect(t, store)

	var g graphOut
	callTool(t, cs, ctx, "memgraph_create_graph", map[string]any{"name": "g"}, &g)

	var out tPutSubgraphOut
	callTool(t, cs, ctx, "memgraph_put_subgraph", map[string]any{
		"graph_id": g.ID,
		"nodes": []map[string]any{
			{"ref": "a", "kind": "fact", "content": "exists"},
		},
		"edges": []map[string]any{
			{"from_ref": "a", "to_ref": "nope", "kind": "cites"},
		},
	}, &out)

	if out.Edges[0].Error != "ref not found: nope" {
		t.Fatalf("edge[0].error = %q want %q", out.Edges[0].Error, "ref not found: nope")
	}
}

// TestPutSubgraph_Validation covers the shape errors that abort before any
// writes occur.
func TestPutSubgraph_Validation(t *testing.T) {
	store := openStore(t)
	cs, ctx := connect(t, store)

	var g graphOut
	callTool(t, cs, ctx, "memgraph_create_graph", map[string]any{"name": "g"}, &g)

	mustErr := func(name string, args map[string]any) {
		t.Helper()
		res, err := cs.CallTool(ctx, &sdkmcp.CallToolParams{Name: "memgraph_put_subgraph", Arguments: args})
		if err == nil && !res.IsError {
			t.Fatalf("%s: expected error, got %+v", name, res)
		}
	}

	// Empty nodes AND empty edges -> error.
	mustErr("empty", map[string]any{"graph_id": g.ID})

	// 51 nodes -> error.
	bigN := make([]map[string]any, 51)
	for i := range bigN {
		bigN[i] = map[string]any{"kind": "fact", "content": fmt.Sprintf("%d", i)}
	}
	mustErr("51 nodes", map[string]any{"graph_id": g.ID, "nodes": bigN})

	// 101 edges -> error. Need at least one node target; use lineage form.
	bigE := make([]map[string]any, 101)
	for i := range bigE {
		bigE[i] = map[string]any{"from_lineage": "x", "to_lineage": "y", "kind": "cites"}
	}
	mustErr("101 edges", map[string]any{"graph_id": g.ID, "edges": bigE})

	// Both from_ref AND from_lineage on the same edge -> per-item error.
	var out tPutSubgraphOut
	callTool(t, cs, ctx, "memgraph_put_subgraph", map[string]any{
		"graph_id": g.ID,
		"nodes":    []map[string]any{{"ref": "a", "kind": "fact", "content": "n"}},
		"edges": []map[string]any{
			{"from_ref": "a", "from_lineage": "some-lid", "to_ref": "a", "kind": "self"},
		},
	}, &out)
	if out.Edges[0].Error != "only one of from_ref/from_lineage allowed" {
		t.Fatalf("edge[0].error = %q want %q", out.Edges[0].Error, "only one of from_ref/from_lineage allowed")
	}
}

// TestPutSubgraph_ManualConflict_OtherNodesProceed: a node whose update hits
// the manual-conflict policy reports a non-empty error AND a lineage_id (the
// sibling head); other nodes in the same batch are unaffected.
func TestPutSubgraph_ManualConflict_OtherNodesProceed(t *testing.T) {
	store := openStore(t)
	cs, ctx := connect(t, store)

	var g graphOut
	callTool(t, cs, ctx, "memgraph_create_graph", map[string]any{
		"name":            "g",
		"conflict_policy": "manual",
	}, &g)

	// Seed v1; then write v2 so the next batch's based_on_version=1 will
	// detect a stale-base concurrent write.
	var n1 nodeOut
	callTool(t, cs, ctx, "memgraph_put_node", map[string]any{
		"graph_id": g.ID, "kind": "fact", "content": "v1",
	}, &n1)
	var n2 nodeOut
	callTool(t, cs, ctx, "memgraph_put_node", map[string]any{
		"graph_id": g.ID, "kind": "fact", "content": "v2",
		"lineage_id": n1.LineageID, "based_on_version": 1,
	}, &n2)
	if n2.Version != 2 {
		t.Fatalf("seed v2 didn't advance: %+v", n2)
	}

	var out tPutSubgraphOut
	callTool(t, cs, ctx, "memgraph_put_subgraph", map[string]any{
		"graph_id": g.ID,
		"nodes": []map[string]any{
			// This update is based on v1 but head is v2 -> manual conflict.
			{"lineage_id": n1.LineageID, "kind": "fact", "content": "stale-update", "based_on_version": 1},
			// This independent node should still succeed.
			{"ref": "fine", "kind": "fact", "content": "unrelated insert"},
		},
	}, &out)

	if len(out.Nodes) != 2 {
		t.Fatalf("expected 2 node results: %+v", out)
	}
	if out.Nodes[0].Error == "" {
		t.Fatalf("expected conflict error on node[0]: %+v", out.Nodes[0])
	}
	// Conflict still writes a sibling, so lineage_id IS populated.
	if out.Nodes[0].LineageID != n1.LineageID {
		t.Fatalf("conflict sibling should keep lineage %s, got %+v", n1.LineageID, out.Nodes[0])
	}
	if out.Nodes[1].Error != "" || out.Nodes[1].LineageID == "" || !out.Nodes[1].Created {
		t.Fatalf("independent node[1] should succeed: %+v", out.Nodes[1])
	}
}

// TestPutSubgraph_NodesOnlyOrEdgesOnly: spec says nodes and edges are each
// optional as long as at least one of them is non-empty.
func TestPutSubgraph_NodesOnlyOrEdgesOnly(t *testing.T) {
	store := openStore(t)
	cs, ctx := connect(t, store)

	var g graphOut
	callTool(t, cs, ctx, "memgraph_create_graph", map[string]any{"name": "g"}, &g)

	// Nodes only.
	var out1 tPutSubgraphOut
	callTool(t, cs, ctx, "memgraph_put_subgraph", map[string]any{
		"graph_id": g.ID,
		"nodes":    []map[string]any{{"kind": "fact", "content": "solo"}},
	}, &out1)
	if len(out1.Nodes) != 1 || out1.Nodes[0].Error != "" {
		t.Fatalf("nodes-only batch failed: %+v", out1)
	}

	// Edges only against existing lineages.
	var n1 nodeOut
	callTool(t, cs, ctx, "memgraph_put_node", map[string]any{
		"graph_id": g.ID, "kind": "fact", "content": "a",
	}, &n1)
	var n2 nodeOut
	callTool(t, cs, ctx, "memgraph_put_node", map[string]any{
		"graph_id": g.ID, "kind": "fact", "content": "b",
	}, &n2)
	var out2 tPutSubgraphOut
	callTool(t, cs, ctx, "memgraph_put_subgraph", map[string]any{
		"graph_id": g.ID,
		"edges": []map[string]any{
			{"from_lineage": n1.LineageID, "to_lineage": n2.LineageID, "kind": "cites"},
		},
	}, &out2)
	if len(out2.Edges) != 1 || out2.Edges[0].Error != "" || out2.Edges[0].EdgeID == "" {
		t.Fatalf("edges-only batch failed: %+v", out2)
	}
}

