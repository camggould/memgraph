package mcp

import (
	"context"
	"encoding/json"
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
