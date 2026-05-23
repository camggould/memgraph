package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	memgraph "github.com/camggould/memgraph"
	"github.com/camggould/memgraph/store/sqlite"
	sdkmcp "github.com/modelcontextprotocol/go-sdk/mcp"
)

// connect spins up an in-memory MCP transport pair, registers the memgraph
// tools on the server, and returns a connected ClientSession.
func connect(t *testing.T, store memgraph.Store) (*sdkmcp.ClientSession, context.Context) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)

	s := New(store)
	srv := s.build()

	t1, t2 := sdkmcp.NewInMemoryTransports()
	if _, err := srv.Connect(ctx, t1, nil); err != nil {
		t.Fatalf("server.Connect: %v", err)
	}
	client := sdkmcp.NewClient(&sdkmcp.Implementation{Name: "test", Version: "0"}, nil)
	cs, err := client.Connect(ctx, t2, nil)
	if err != nil {
		t.Fatalf("client.Connect: %v", err)
	}
	t.Cleanup(func() { _ = cs.Close() })
	return cs, ctx
}

func openStore(t *testing.T) *sqlite.Store {
	t.Helper()
	p := filepath.Join(t.TempDir(), "memgraph.db")
	s, err := sqlite.Open(p)
	if err != nil {
		t.Fatalf("sqlite.Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// callTool dispatches a tool call and decodes StructuredContent into v.
func callTool(t *testing.T, cs *sdkmcp.ClientSession, ctx context.Context, name string, args any, v any) *sdkmcp.CallToolResult {
	t.Helper()
	res, err := cs.CallTool(ctx, &sdkmcp.CallToolParams{Name: name, Arguments: args})
	if err != nil {
		t.Fatalf("CallTool(%s): %v", name, err)
	}
	if res.IsError {
		t.Fatalf("CallTool(%s) returned IsError; content=%+v", name, res.Content)
	}
	if v != nil && res.StructuredContent != nil {
		raw, err := json.Marshal(res.StructuredContent)
		if err != nil {
			t.Fatalf("marshal structured: %v", err)
		}
		if err := json.Unmarshal(raw, v); err != nil {
			t.Fatalf("decode structured into %T: %v\nraw=%s", v, err, raw)
		}
	}
	return res
}

func TestListGraphsAndCreate(t *testing.T) {
	store := openStore(t)
	cs, ctx := connect(t, store)

	// create_graph
	var g graphOut
	callTool(t, cs, ctx, "memgraph_create_graph", map[string]any{
		"name": "primary",
	}, &g)
	if g.Name != "primary" || g.ConflictPolicy != "lww" || g.ID == "" {
		t.Fatalf("unexpected graph: %+v", g)
	}

	// list_graphs sees it
	var listed listGraphsOut
	callTool(t, cs, ctx, "memgraph_list_graphs", struct{}{}, &listed)
	if len(listed.Graphs) != 1 || listed.Graphs[0].ID != g.ID {
		t.Fatalf("list_graphs: %+v", listed)
	}
	if listed.Graphs[0].OutboundCount != 0 || listed.Graphs[0].InboundCount != 0 {
		t.Fatalf("expected zero symlink counts: %+v", listed.Graphs[0])
	}
}

func TestPutAndGetNode_VersionAdvances(t *testing.T) {
	store := openStore(t)
	cs, ctx := connect(t, store)

	var g graphOut
	callTool(t, cs, ctx, "memgraph_create_graph", map[string]any{"name": "g"}, &g)

	// v1
	var n1 nodeOut
	callTool(t, cs, ctx, "memgraph_put_node", map[string]any{
		"graph_id": g.ID, "kind": "fact", "content": "v1 body",
	}, &n1)
	if n1.Version != 1 || n1.LineageID == "" {
		t.Fatalf("expected v1: %+v", n1)
	}

	// v2: same lineage
	var n2 nodeOut
	callTool(t, cs, ctx, "memgraph_put_node", map[string]any{
		"graph_id": g.ID, "kind": "fact", "content": "v2 body",
		"lineage_id": n1.LineageID,
	}, &n2)
	if n2.Version != 2 || n2.LineageID != n1.LineageID {
		t.Fatalf("expected v2 same lineage: %+v", n2)
	}

	// get_node by lineage -> current (v2)
	var cur nodeOut
	callTool(t, cs, ctx, "memgraph_get_node", map[string]any{
		"lineage_id": n1.LineageID,
	}, &cur)
	if cur.Version != 2 || !cur.IsCurrent || cur.IsStale {
		t.Fatalf("expected current v2 non-stale: %+v", cur)
	}

	// get_node by node_id -> exact (v1, no longer current)
	var v1Get nodeOut
	callTool(t, cs, ctx, "memgraph_get_node", map[string]any{
		"node_id": n1.ID,
	}, &v1Get)
	if v1Get.Version != 1 || v1Get.IsCurrent {
		t.Fatalf("expected v1 non-current: %+v", v1Get)
	}

	// get_node mutual exclusion
	res, err := cs.CallTool(ctx, &sdkmcp.CallToolParams{
		Name: "memgraph_get_node",
		Arguments: map[string]any{"node_id": n1.ID, "lineage_id": n1.LineageID},
	})
	if err != nil {
		t.Fatalf("CallTool: %v", err)
	}
	if !res.IsError {
		t.Fatal("expected IsError when both node_id and lineage_id are given")
	}
}

func TestGetNode_IsStaleWhenFreshnessPast(t *testing.T) {
	store := openStore(t)
	cs, ctx := connect(t, store)

	var g graphOut
	callTool(t, cs, ctx, "memgraph_create_graph", map[string]any{"name": "g"}, &g)

	past := time.Now().Add(-1 * time.Hour).UTC().Format(time.RFC3339)
	var n nodeOut
	callTool(t, cs, ctx, "memgraph_put_node", map[string]any{
		"graph_id": g.ID, "kind": "fact", "content": "expired",
		"freshness_at": past,
	}, &n)

	var got nodeOut
	callTool(t, cs, ctx, "memgraph_get_node", map[string]any{
		"lineage_id": n.LineageID,
	}, &got)
	if !got.IsCurrent {
		t.Fatalf("expected current: %+v", got)
	}
	if !got.IsStale {
		t.Fatalf("expected stale (freshness=%v now=%v): %+v", got.FreshnessAt, time.Now(), got)
	}
}

func TestEdgeFlow_PutTraverseDelete(t *testing.T) {
	store := openStore(t)
	cs, ctx := connect(t, store)

	var g graphOut
	callTool(t, cs, ctx, "memgraph_create_graph", map[string]any{"name": "g"}, &g)

	mkNode := func(content string) nodeOut {
		var n nodeOut
		callTool(t, cs, ctx, "memgraph_put_node", map[string]any{
			"graph_id": g.ID, "kind": "fact", "content": content,
		}, &n)
		return n
	}
	a := mkNode("a")
	b := mkNode("b")

	var e edgeOut
	callTool(t, cs, ctx, "memgraph_put_edge", map[string]any{
		"graph_id":     g.ID,
		"from_lineage": a.LineageID,
		"to_lineage":   b.LineageID,
		"kind":         "cites",
	}, &e)
	if e.ID == "" || e.Kind != "cites" || e.ToGraph != g.ID {
		t.Fatalf("unexpected edge: %+v", e)
	}

	var tr traverseOut
	callTool(t, cs, ctx, "memgraph_traverse", map[string]any{
		"from_lineage": a.LineageID,
	}, &tr)
	if len(tr.Edges) != 1 || len(tr.Nodes) < 2 {
		t.Fatalf("expected to reach b: %+v", tr)
	}

	// delete edge
	var ok okOut
	callTool(t, cs, ctx, "memgraph_delete_edge", map[string]any{"edge_id": e.ID}, &ok)
	if !ok.OK {
		t.Fatal("expected ok")
	}

	var tr2 traverseOut
	callTool(t, cs, ctx, "memgraph_traverse", map[string]any{
		"from_lineage": a.LineageID,
	}, &tr2)
	if len(tr2.Edges) != 0 {
		t.Fatalf("expected no edges after delete: %+v", tr2)
	}
}

func TestMCP_Traverse_Direction(t *testing.T) {
	store := openStore(t)
	cs, ctx := connect(t, store)

	var g graphOut
	callTool(t, cs, ctx, "memgraph_create_graph", map[string]any{"name": "g"}, &g)

	mkNode := func(content string) nodeOut {
		var n nodeOut
		callTool(t, cs, ctx, "memgraph_put_node", map[string]any{
			"graph_id": g.ID, "kind": "fact", "content": content,
		}, &n)
		return n
	}
	a := mkNode("a")
	b := mkNode("b")
	c := mkNode("c")

	// a -cites-> b -cites-> c
	var e1, e2 edgeOut
	callTool(t, cs, ctx, "memgraph_put_edge", map[string]any{
		"graph_id":     g.ID,
		"from_lineage": a.LineageID,
		"to_lineage":   b.LineageID,
		"kind":         "cites",
	}, &e1)
	callTool(t, cs, ctx, "memgraph_put_edge", map[string]any{
		"graph_id":     g.ID,
		"from_lineage": b.LineageID,
		"to_lineage":   c.LineageID,
		"kind":         "cites",
	}, &e2)

	// Incoming from c reaches a and b (and c as the seed).
	var trIn traverseOut
	callTool(t, cs, ctx, "memgraph_traverse", map[string]any{
		"from_lineage": c.LineageID,
		"direction":    "incoming",
		"max_depth":    3,
	}, &trIn)
	seenIn := map[string]bool{}
	for _, n := range trIn.Nodes {
		seenIn[n.LineageID] = true
	}
	if !seenIn[a.LineageID] || !seenIn[b.LineageID] || !seenIn[c.LineageID] {
		t.Fatalf("incoming should reach a, b, c: %+v", trIn.Nodes)
	}
	if len(trIn.Edges) != 2 {
		t.Fatalf("incoming should return 2 edges, got %d: %+v", len(trIn.Edges), trIn.Edges)
	}

	// Both from b reaches a, b, c.
	var trBoth traverseOut
	callTool(t, cs, ctx, "memgraph_traverse", map[string]any{
		"from_lineage": b.LineageID,
		"direction":    "both",
		"max_depth":    3,
	}, &trBoth)
	seenBoth := map[string]bool{}
	for _, n := range trBoth.Nodes {
		seenBoth[n.LineageID] = true
	}
	if !seenBoth[a.LineageID] || !seenBoth[b.LineageID] || !seenBoth[c.LineageID] {
		t.Fatalf("both should reach a, b, c: %+v", trBoth.Nodes)
	}
	if len(trBoth.Edges) != 2 {
		t.Fatalf("both should return 2 deduped edges, got %d", len(trBoth.Edges))
	}

	// Invalid direction is rejected.
	res, err := cs.CallTool(ctx, &sdkmcp.CallToolParams{
		Name: "memgraph_traverse",
		Arguments: map[string]any{
			"from_lineage": a.LineageID,
			"direction":    "sideways",
		},
	})
	if err != nil {
		t.Fatalf("CallTool: %v", err)
	}
	if !res.IsError {
		t.Fatalf("expected IsError for invalid direction")
	}
}

func TestHistoryAndSearch(t *testing.T) {
	store := openStore(t)
	cs, ctx := connect(t, store)

	var g graphOut
	callTool(t, cs, ctx, "memgraph_create_graph", map[string]any{"name": "g"}, &g)

	var n1 nodeOut
	callTool(t, cs, ctx, "memgraph_put_node", map[string]any{
		"graph_id": g.ID, "kind": "fact", "content": "tabs over spaces",
		"tags": []string{"prefs"},
	}, &n1)
	var n2 nodeOut
	callTool(t, cs, ctx, "memgraph_put_node", map[string]any{
		"graph_id": g.ID, "kind": "fact", "content": "switched to spaces",
		"lineage_id": n1.LineageID,
	}, &n2)

	var h historyOut
	callTool(t, cs, ctx, "memgraph_history", map[string]any{"lineage_id": n1.LineageID}, &h)
	if len(h.Versions) != 2 || h.Versions[0].Version != 2 {
		t.Fatalf("expected history newest-first len 2: %+v", h)
	}

	var sr searchOut
	callTool(t, cs, ctx, "memgraph_search", map[string]any{
		"graph_id": g.ID, "text": "spaces",
	}, &sr)
	if len(sr.Hits) == 0 {
		t.Fatalf("expected at least one hit: %+v", sr)
	}
	// Only the current version should be searchable.
	for _, h := range sr.Hits {
		if !h.Node.IsCurrent {
			t.Fatalf("hit is not current: %+v", h.Node)
		}
	}
}

// ---- search_batch tests ----

type searchHitWithScoreT struct {
	Node           nodeOut `json:"node"`
	RRFScore       float64 `json:"rrf_score"`
	QueriesMatched []int   `json:"queries_matched"`
}

type searchBatchOutT struct {
	Hits         []searchHitWithScoreT `json:"hits"`
	QueryCount   int                   `json:"query_count"`
	UniqueHits   int                   `json:"unique_hits"`
	PerQueryHits []int                 `json:"per_query_hits"`
}

func TestSearchBatch_RRF_DedupesByLineage(t *testing.T) {
	store := openStore(t)
	cs, ctx := connect(t, store)

	var g graphOut
	callTool(t, cs, ctx, "memgraph_create_graph", map[string]any{"name": "g"}, &g)

	mk := func(content string) nodeOut {
		var n nodeOut
		callTool(t, cs, ctx, "memgraph_put_node", map[string]any{
			"graph_id": g.ID, "kind": "fact", "content": content,
		}, &n)
		return n
	}
	// "alpha bravo" should rank well for both "alpha" and "bravo" queries.
	shared := mk("alpha bravo overlap")
	onlyAlpha := mk("alpha solo")
	onlyBravo := mk("bravo solo")

	var out searchBatchOutT
	callTool(t, cs, ctx, "memgraph_search_batch", map[string]any{
		"graph_id": g.ID,
		"queries": []map[string]any{
			{"text": "alpha"},
			{"text": "bravo"},
		},
	}, &out)

	if out.QueryCount != 2 {
		t.Fatalf("QueryCount=%d want 2", out.QueryCount)
	}
	if len(out.PerQueryHits) != 2 {
		t.Fatalf("PerQueryHits=%v want len 2", out.PerQueryHits)
	}
	if out.UniqueHits < 3 {
		t.Fatalf("UniqueHits=%d want >=3 (saw lineages: %+v)", out.UniqueHits, out.Hits)
	}
	// Each lineage must appear at most once (deduped).
	seen := map[string]int{}
	for _, h := range out.Hits {
		seen[h.Node.LineageID]++
		if seen[h.Node.LineageID] > 1 {
			t.Fatalf("lineage %s appeared twice in deduped output", h.Node.LineageID)
		}
	}

	// Shared lineage must outrank both single-query lineages — it gets RRF
	// contributions from both queries, the singles get only one.
	var rankShared, rankAlpha, rankBravo = -1, -1, -1
	for i, h := range out.Hits {
		switch h.Node.LineageID {
		case shared.LineageID:
			rankShared = i
			if len(h.QueriesMatched) != 2 {
				t.Fatalf("shared lineage should match 2 queries, got %v", h.QueriesMatched)
			}
		case onlyAlpha.LineageID:
			rankAlpha = i
			if len(h.QueriesMatched) != 1 {
				t.Fatalf("alpha-only should match 1 query, got %v", h.QueriesMatched)
			}
		case onlyBravo.LineageID:
			rankBravo = i
			if len(h.QueriesMatched) != 1 {
				t.Fatalf("bravo-only should match 1 query, got %v", h.QueriesMatched)
			}
		}
	}
	if rankShared < 0 || rankAlpha < 0 || rankBravo < 0 {
		t.Fatalf("expected all 3 lineages in hits: shared=%d alpha=%d bravo=%d hits=%+v",
			rankShared, rankAlpha, rankBravo, out.Hits)
	}
	if rankShared > rankAlpha || rankShared > rankBravo {
		t.Fatalf("shared lineage should outrank singles; got ranks shared=%d alpha=%d bravo=%d",
			rankShared, rankAlpha, rankBravo)
	}

	// Explicit RRF math: shared appears at rank 1 in both query results
	// (only the shared lineage matches alpha+bravo, then the solos), so its
	// score should be 2 * 1/(60+1) = 0.03278... Allow slack since rank
	// ordering inside a single query depends on the underlying scoring.
	if out.Hits[rankShared].RRFScore <= out.Hits[rankAlpha].RRFScore {
		t.Fatalf("shared score (%v) should exceed alpha-only score (%v)",
			out.Hits[rankShared].RRFScore, out.Hits[rankAlpha].RRFScore)
	}
}

func TestSearchBatch_EmptyResults(t *testing.T) {
	store := openStore(t)
	cs, ctx := connect(t, store)

	var g graphOut
	callTool(t, cs, ctx, "memgraph_create_graph", map[string]any{"name": "g"}, &g)

	res, err := cs.CallTool(ctx, &sdkmcp.CallToolParams{
		Name: "memgraph_search_batch",
		Arguments: map[string]any{
			"graph_id": g.ID,
			"queries": []map[string]any{
				{"text": "nothingherexyz"},
				{"text": "alsonothingabc"},
				{"text": "stillnothingqrs"},
			},
		},
	})
	if err != nil {
		t.Fatalf("CallTool: %v", err)
	}
	if res.IsError {
		body := ""
		for _, c := range res.Content {
			if tc, ok := c.(*sdkmcp.TextContent); ok {
				body += tc.Text
			}
		}
		t.Fatalf("unexpected IsError: %s", body)
	}
	var out searchBatchOutT
	raw, _ := json.Marshal(res.StructuredContent)
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out.QueryCount != 3 {
		t.Fatalf("QueryCount=%d want 3", out.QueryCount)
	}
	if len(out.Hits) != 0 {
		t.Fatalf("expected empty hits, got %+v", out.Hits)
	}
	if out.UniqueHits != 0 {
		t.Fatalf("UniqueHits=%d want 0", out.UniqueHits)
	}
	if len(out.PerQueryHits) != 3 {
		t.Fatalf("PerQueryHits=%v want len 3", out.PerQueryHits)
	}
	for i, n := range out.PerQueryHits {
		if n != 0 {
			t.Fatalf("PerQueryHits[%d]=%d want 0", i, n)
		}
	}
}

func TestSearchBatch_TotalLimit(t *testing.T) {
	store := openStore(t)
	cs, ctx := connect(t, store)

	var g graphOut
	callTool(t, cs, ctx, "memgraph_create_graph", map[string]any{"name": "g"}, &g)

	// Seed 12 distinct lineages all matching "foo".
	for i := 0; i < 12; i++ {
		var n nodeOut
		callTool(t, cs, ctx, "memgraph_put_node", map[string]any{
			"graph_id": g.ID, "kind": "fact",
			"content": fmt.Sprintf("foo bar %d", i),
		}, &n)
	}

	var out searchBatchOutT
	callTool(t, cs, ctx, "memgraph_search_batch", map[string]any{
		"graph_id": g.ID,
		"queries":  []map[string]any{{"text": "foo"}},
		"limit":    5,
	}, &out)
	if len(out.Hits) != 5 {
		t.Fatalf("expected 5 hits after total limit, got %d", len(out.Hits))
	}
	if out.UniqueHits < 12 {
		t.Fatalf("UniqueHits=%d want >=12 (pre-truncation count)", out.UniqueHits)
	}
}

func TestSearchBatch_PerQueryLimitClamp(t *testing.T) {
	store := openStore(t)
	cs, ctx := connect(t, store)

	var g graphOut
	callTool(t, cs, ctx, "memgraph_create_graph", map[string]any{"name": "g"}, &g)

	// Seed more than 50 matching nodes so the clamp kicks in.
	for i := 0; i < 60; i++ {
		var n nodeOut
		callTool(t, cs, ctx, "memgraph_put_node", map[string]any{
			"graph_id": g.ID, "kind": "fact",
			"content": fmt.Sprintf("clampme entry %d", i),
		}, &n)
	}
	var out searchBatchOutT
	callTool(t, cs, ctx, "memgraph_search_batch", map[string]any{
		"graph_id": g.ID,
		"queries": []map[string]any{
			{"text": "clampme", "limit": 500},
		},
		"limit": 100,
	}, &out)
	if out.PerQueryHits[0] > 50 {
		t.Fatalf("per-query limit should clamp to 50, got %d", out.PerQueryHits[0])
	}
	if out.PerQueryHits[0] != 50 {
		t.Fatalf("expected exactly 50 hits at clamp, got %d (UniqueHits=%d)", out.PerQueryHits[0], out.UniqueHits)
	}
}

func TestSearchBatch_Validation(t *testing.T) {
	store := openStore(t)
	cs, ctx := connect(t, store)

	var g graphOut
	callTool(t, cs, ctx, "memgraph_create_graph", map[string]any{"name": "g"}, &g)

	// 0 queries → error.
	res, err := cs.CallTool(ctx, &sdkmcp.CallToolParams{
		Name: "memgraph_search_batch",
		Arguments: map[string]any{
			"graph_id": g.ID,
			"queries":  []map[string]any{},
		},
	})
	if err == nil && !res.IsError {
		t.Fatalf("expected error for 0 queries, got %+v", res)
	}

	// 9 queries → error.
	nine := make([]map[string]any, 9)
	for i := range nine {
		nine[i] = map[string]any{"text": fmt.Sprintf("q%d", i)}
	}
	res2, err := cs.CallTool(ctx, &sdkmcp.CallToolParams{
		Name: "memgraph_search_batch",
		Arguments: map[string]any{
			"graph_id": g.ID,
			"queries":  nine,
		},
	})
	if err == nil && !res2.IsError {
		t.Fatalf("expected error for 9 queries, got %+v", res2)
	}

	// missing graph_id → error.
	res3, err := cs.CallTool(ctx, &sdkmcp.CallToolParams{
		Name: "memgraph_search_batch",
		Arguments: map[string]any{
			"queries": []map[string]any{{"text": "x"}},
		},
	})
	if err == nil && !res3.IsError {
		t.Fatalf("expected error for missing graph_id, got %+v", res3)
	}
}

func TestPutNode_ManualConflict_BasedOnVersion(t *testing.T) {
	store := openStore(t)
	cs, ctx := connect(t, store)

	// Create a graph with manual conflict policy.
	var g graphOut
	callTool(t, cs, ctx, "memgraph_create_graph", map[string]any{
		"name":            "manual",
		"conflict_policy": "manual",
	}, &g)

	// Seed v1.
	var n1 nodeOut
	callTool(t, cs, ctx, "memgraph_put_node", map[string]any{
		"graph_id": g.ID, "kind": "fact", "content": "v1",
	}, &n1)

	// Non-conflicting write: based_on_version=1 matches current head.
	var n2a nodeOut
	res := callTool(t, cs, ctx, "memgraph_put_node", map[string]any{
		"graph_id":         g.ID,
		"kind":             "fact",
		"content":          "v2a",
		"lineage_id":       n1.LineageID,
		"based_on_version": 1,
	}, &n2a)
	if res.IsError {
		t.Fatalf("non-conflicting put unexpectedly reported error: %+v", res.Content)
	}
	if n2a.Version != 2 || len(n2a.Conflicts) != 0 {
		t.Fatalf("expected clean v2 with no conflicts: %+v", n2a)
	}

	// Conflicting write under manual: based_on_version=1 but head is now 2.
	// We bypass callTool (which fails on IsError) and inspect directly.
	resp, err := cs.CallTool(ctx, &sdkmcp.CallToolParams{
		Name: "memgraph_put_node",
		Arguments: map[string]any{
			"graph_id":         g.ID,
			"kind":             "fact",
			"content":          "v2b",
			"lineage_id":       n1.LineageID,
			"based_on_version": 1,
		},
	})
	if err != nil {
		t.Fatalf("CallTool: %v", err)
	}
	if !resp.IsError {
		t.Fatalf("expected IsError under manual conflict, got %+v", resp)
	}
	if resp.StructuredContent == nil {
		t.Fatalf("expected StructuredContent with the conflicting node, got nil")
	}
	var n2b nodeOut
	raw, err := json.Marshal(resp.StructuredContent)
	if err != nil {
		t.Fatalf("marshal structured: %v", err)
	}
	if err := json.Unmarshal(raw, &n2b); err != nil {
		t.Fatalf("decode node out: %v\nraw=%s", err, raw)
	}
	if n2b.ID == "" {
		t.Fatalf("expected node id on conflict, got %+v", n2b)
	}
	if len(n2b.Conflicts) != 1 || n2b.Conflicts[0] != n2a.ID {
		t.Fatalf("expected Conflicts=[%s], got %v", n2a.ID, n2b.Conflicts)
	}
}

func TestResources_LiveOnGraphCreate(t *testing.T) {
	store := openStore(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)

	s := New(store)
	srv := s.build()
	t.Cleanup(func() {
		if s.unsubResources != nil {
			s.unsubResources()
		}
	})

	t1, t2 := sdkmcp.NewInMemoryTransports()
	if _, err := srv.Connect(ctx, t1, nil); err != nil {
		t.Fatalf("server.Connect: %v", err)
	}

	// Subscribe to resource-list-changed notifications BEFORE Connect so we
	// don't race against the post-create notification.
	notified := make(chan struct{}, 4)
	client := sdkmcp.NewClient(&sdkmcp.Implementation{Name: "test", Version: "0"}, &sdkmcp.ClientOptions{
		ResourceListChangedHandler: func(context.Context, *sdkmcp.ResourceListChangedRequest) {
			select {
			case notified <- struct{}{}:
			default:
			}
		},
	})
	cs, err := client.Connect(ctx, t2, nil)
	if err != nil {
		t.Fatalf("client.Connect: %v", err)
	}
	t.Cleanup(func() { _ = cs.Close() })

	// Empty store → no graph resources yet.
	lr, err := cs.ListResources(ctx, &sdkmcp.ListResourcesParams{})
	if err != nil {
		t.Fatalf("ListResources: %v", err)
	}
	for _, r := range lr.Resources {
		t.Fatalf("expected no graph resources at startup, got %q", r.URI)
	}

	// Create a graph via the MCP tool.
	var g graphOut
	callTool(t, cs, ctx, "memgraph_create_graph", map[string]any{"name": "live"}, &g)
	if g.ID == "" {
		t.Fatalf("expected graph id: %+v", g)
	}

	// Expect a resource-list-changed notification.
	select {
	case <-notified:
	case <-time.After(2 * time.Second):
		t.Fatalf("did not receive resources/list_changed notification within timeout")
	}

	// ListResources now includes the new graph.
	lr2, err := cs.ListResources(ctx, &sdkmcp.ListResourcesParams{})
	if err != nil {
		t.Fatalf("ListResources after create: %v", err)
	}
	wantURI := "memgraph://" + g.ID
	found := false
	for _, r := range lr2.Resources {
		if r.URI == wantURI {
			found = true
			if r.Name != "live" {
				t.Fatalf("expected resource Name=live, got %q", r.Name)
			}
		}
	}
	if !found {
		t.Fatalf("expected resource %q in list; got %+v", wantURI, lr2.Resources)
	}

	// And the resource is readable, yielding the graph summary JSON.
	rr, err := cs.ReadResource(ctx, &sdkmcp.ReadResourceParams{URI: wantURI})
	if err != nil {
		t.Fatalf("ReadResource: %v", err)
	}
	if len(rr.Contents) != 1 {
		t.Fatalf("expected 1 content, got %d", len(rr.Contents))
	}
	var got graphOut
	if err := json.Unmarshal([]byte(rr.Contents[0].Text), &got); err != nil {
		t.Fatalf("decode graph: %v\nraw=%s", err, rr.Contents[0].Text)
	}
	if got.ID != g.ID || got.Name != "live" {
		t.Fatalf("graph summary mismatch: %+v", got)
	}
}

func TestResources_GraphAndLineage(t *testing.T) {
	store := openStore(t)
	// Seed before connecting so registerResources sees the graph.
	ctx := context.Background()
	g, err := store.CreateGraph(ctx, memgraph.GraphInput{Name: "rsrc"})
	if err != nil {
		t.Fatal(err)
	}
	n, err := store.PutNode(ctx, memgraph.NodeInput{
		GraphID: g.ID, Kind: "fact", Content: "hello", CreatedBy: "test",
	})
	if err != nil {
		t.Fatal(err)
	}

	cs, ctx := connect(t, store)

	// ListResources should include the graph.
	lr, err := cs.ListResources(ctx, &sdkmcp.ListResourcesParams{})
	if err != nil {
		t.Fatalf("ListResources: %v", err)
	}
	found := false
	for _, r := range lr.Resources {
		if r.URI == "memgraph://"+string(g.ID) {
			found = true
		}
	}
	if !found {
		t.Fatalf("graph resource not listed; got %+v", lr.Resources)
	}

	// ReadResource: graph
	rr, err := cs.ReadResource(ctx, &sdkmcp.ReadResourceParams{URI: "memgraph://" + string(g.ID)})
	if err != nil {
		t.Fatalf("ReadResource graph: %v", err)
	}
	if len(rr.Contents) != 1 {
		t.Fatalf("expected 1 content, got %d", len(rr.Contents))
	}
	var got graphOut
	if err := json.Unmarshal([]byte(rr.Contents[0].Text), &got); err != nil {
		t.Fatalf("decode graph: %v", err)
	}
	if got.ID != string(g.ID) {
		t.Fatalf("expected graph %s, got %s", g.ID, got.ID)
	}

	// ReadResource: lineage (via template)
	rr2, err := cs.ReadResource(ctx, &sdkmcp.ReadResourceParams{
		URI: "memgraph://" + string(g.ID) + "/" + string(n.LineageID),
	})
	if err != nil {
		t.Fatalf("ReadResource lineage: %v", err)
	}
	var gotNode nodeOut
	if err := json.Unmarshal([]byte(rr2.Contents[0].Text), &gotNode); err != nil {
		t.Fatalf("decode node: %v", err)
	}
	if gotNode.LineageID != string(n.LineageID) {
		t.Fatalf("lineage mismatch: %+v", gotNode)
	}
}

func TestListNodes_CompactStripsHeavyFields(t *testing.T) {
	store := openStore(t)
	cs, ctx := connect(t, store)

	var g graphOut
	callTool(t, cs, ctx, "memgraph_create_graph", map[string]any{"name": "g"}, &g)

	var n nodeOut
	callTool(t, cs, ctx, "memgraph_put_node", map[string]any{
		"graph_id": g.ID,
		"kind":     "fact",
		"content":  "heavy payload that compact mode should not return",
		"summary":  "canvas-label",
		"tags":     []string{"x"},
		"metadata": map[string]any{"k": "v"},
	}, &n)

	// Full mode: content/metadata/created_by present.
	var full listNodesOut
	callTool(t, cs, ctx, "memgraph_list_nodes", map[string]any{
		"graph_id": g.ID,
	}, &full)
	if len(full.Nodes) != 1 {
		t.Fatalf("full: want 1 node, got %d", len(full.Nodes))
	}
	if full.Nodes[0].Content == "" || full.Nodes[0].Metadata == nil || full.Nodes[0].CreatedBy == "" {
		t.Fatalf("full: missing heavy fields: %+v", full.Nodes[0])
	}

	// Compact mode: content/metadata/created_by absent (zero value).
	var compact listNodesOut
	callTool(t, cs, ctx, "memgraph_list_nodes", map[string]any{
		"graph_id": g.ID,
		"compact":  true,
	}, &compact)
	if len(compact.Nodes) != 1 {
		t.Fatalf("compact: want 1 node, got %d", len(compact.Nodes))
	}
	c := compact.Nodes[0]
	if c.Content != "" {
		t.Fatalf("compact: content not stripped: %q", c.Content)
	}
	if c.Metadata != nil {
		t.Fatalf("compact: metadata not stripped: %v", c.Metadata)
	}
	if c.CreatedBy != "" {
		t.Fatalf("compact: created_by not stripped: %q", c.CreatedBy)
	}
	if c.FreshnessAt != nil {
		t.Fatalf("compact: freshness_at not stripped: %v", c.FreshnessAt)
	}
	// Light fields kept.
	if c.LineageID != n.LineageID || c.Kind != "fact" || c.Summary != "canvas-label" || len(c.Tags) != 1 {
		t.Fatalf("compact: light fields missing: %+v", c)
	}
}
