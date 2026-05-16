package sqlite_test

import (
	"context"
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	memgraph "github.com/camggould/memgraph"
	"github.com/camggould/memgraph/store/sqlite"
)

func openTestStore(t *testing.T) *sqlite.Store {
	t.Helper()
	p := filepath.Join(t.TempDir(), "memgraph.db")
	s, err := sqlite.Open(p)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func mustCreateGraph(t *testing.T, s *sqlite.Store, in memgraph.GraphInput) memgraph.Graph {
	t.Helper()
	g, err := s.CreateGraph(context.Background(), in)
	if err != nil {
		t.Fatalf("CreateGraph: %v", err)
	}
	return g
}

func mustPutNode(t *testing.T, s *sqlite.Store, in memgraph.NodeInput) memgraph.Node {
	t.Helper()
	n, err := s.PutNode(context.Background(), in)
	if err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	return n
}

func TestGraphRoundtrip(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	g, err := s.CreateGraph(ctx, memgraph.GraphInput{
		Name:          "test",
		KindWhitelist: []string{"fact"},
		Metadata:      map[string]any{"owner": "cam"},
	})
	if err != nil {
		t.Fatalf("CreateGraph: %v", err)
	}
	if g.ConflictPolicy != memgraph.ConflictPolicyLWW {
		t.Fatalf("default conflict policy: got %q", g.ConflictPolicy)
	}

	got, err := s.GetGraph(ctx, g.ID)
	if err != nil {
		t.Fatalf("GetGraph: %v", err)
	}
	if got.Name != "test" || got.Metadata["owner"] != "cam" || len(got.KindWhitelist) != 1 {
		t.Fatalf("graph roundtrip mismatch: %+v", got)
	}

	list, err := s.ListGraphs(ctx)
	if err != nil || len(list) != 1 {
		t.Fatalf("ListGraphs: %v len=%d", err, len(list))
	}

	if _, err := s.GetGraph(ctx, memgraph.GraphID("nonexistent")); !errors.Is(err, memgraph.ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestPutNodeVersioning(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	g := mustCreateGraph(t, s, memgraph.GraphInput{Name: "g"})

	v1 := mustPutNode(t, s, memgraph.NodeInput{
		GraphID: g.ID, Kind: "fact", Content: "alpha", CreatedBy: "test",
	})
	if v1.Version != 1 || v1.LineageID == "" {
		t.Fatalf("v1 unexpected: %+v", v1)
	}

	// Tiny sleep to ensure created_at strictly increases for time-travel.
	time.Sleep(2 * time.Millisecond)
	v1Time := time.Now().UTC()
	time.Sleep(2 * time.Millisecond)

	v2 := mustPutNode(t, s, memgraph.NodeInput{
		GraphID: g.ID, LineageID: v1.LineageID, Kind: "fact",
		Content: "beta", CreatedBy: "test",
	})
	if v2.Version != 2 || v2.LineageID != v1.LineageID {
		t.Fatalf("v2 unexpected: %+v", v2)
	}

	// Current = v2.
	cur, err := s.GetNodeByLineage(ctx, v1.LineageID, memgraph.ReadOpts{})
	if err != nil || cur.ID != v2.ID {
		t.Fatalf("current resolution: %v %+v", err, cur)
	}

	// v1 superseded.
	old, err := s.GetNodeByID(ctx, v1.ID)
	if err != nil || old.SupersededBy == nil || *old.SupersededBy != v2.ID {
		t.Fatalf("v1 should be superseded by v2: %v %+v", err, old)
	}

	// AtVersion.
	one := 1
	at, err := s.GetNodeByLineage(ctx, v1.LineageID, memgraph.ReadOpts{AtVersion: &one})
	if err != nil || at.ID != v1.ID {
		t.Fatalf("AtVersion=1 should return v1: %v %+v", err, at)
	}

	// AtTime between v1 and v2 returns v1.
	at, err = s.GetNodeByLineage(ctx, v1.LineageID, memgraph.ReadOpts{AtTime: &v1Time})
	if err != nil || at.ID != v1.ID {
		t.Fatalf("AtTime should return v1: %v %+v", err, at)
	}

	// History newest-first.
	hist, err := s.History(ctx, v1.LineageID)
	if err != nil || len(hist) != 2 || hist[0].ID != v2.ID || hist[1].ID != v1.ID {
		t.Fatalf("history bad: %v %+v", err, hist)
	}
}

func TestKindWhitelist(t *testing.T) {
	s := openTestStore(t)
	g := mustCreateGraph(t, s, memgraph.GraphInput{
		Name: "g", KindWhitelist: []string{"fact"},
	})
	if _, err := s.PutNode(context.Background(), memgraph.NodeInput{
		GraphID: g.ID, Kind: "decision", Content: "x", CreatedBy: "t",
	}); !errors.Is(err, memgraph.ErrKindNotAllowed) {
		t.Fatalf("expected ErrKindNotAllowed, got %v", err)
	}
	if _, err := s.PutNode(context.Background(), memgraph.NodeInput{
		GraphID: g.ID, Kind: "fact", Content: "x", CreatedBy: "t",
	}); err != nil {
		t.Fatalf("whitelisted kind should succeed: %v", err)
	}
}

func TestEdgesAndTraverse(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	g1 := mustCreateGraph(t, s, memgraph.GraphInput{Name: "g1"})
	g2 := mustCreateGraph(t, s, memgraph.GraphInput{Name: "g2"})

	a := mustPutNode(t, s, memgraph.NodeInput{GraphID: g1.ID, Kind: "fact", Content: "a", CreatedBy: "t"})
	b := mustPutNode(t, s, memgraph.NodeInput{GraphID: g1.ID, Kind: "fact", Content: "b", CreatedBy: "t"})
	c := mustPutNode(t, s, memgraph.NodeInput{GraphID: g1.ID, Kind: "fact", Content: "c", CreatedBy: "t"})
	x := mustPutNode(t, s, memgraph.NodeInput{GraphID: g2.ID, Kind: "fact", Content: "x", CreatedBy: "t"})

	// a -cites-> b
	if _, err := s.PutEdge(ctx, memgraph.EdgeInput{
		GraphID: g1.ID, FromLineage: a.LineageID, ToLineage: b.LineageID, Kind: "cites", CreatedBy: "t",
	}); err != nil {
		t.Fatalf("PutEdge: %v", err)
	}
	// b -cites-> c
	if _, err := s.PutEdge(ctx, memgraph.EdgeInput{
		GraphID: g1.ID, FromLineage: b.LineageID, ToLineage: c.LineageID, Kind: "cites", CreatedBy: "t",
	}); err != nil {
		t.Fatalf("PutEdge: %v", err)
	}
	// a -mentions-> b (different kind)
	if _, err := s.PutEdge(ctx, memgraph.EdgeInput{
		GraphID: g1.ID, FromLineage: a.LineageID, ToLineage: b.LineageID, Kind: "mentions", CreatedBy: "t",
	}); err != nil {
		t.Fatalf("PutEdge: %v", err)
	}
	// a -symlink (cites)-> x (cross-graph)
	if _, err := s.PutEdge(ctx, memgraph.EdgeInput{
		GraphID: g1.ID, FromLineage: a.LineageID, ToGraph: g2.ID, ToLineage: x.LineageID,
		Kind: "cites", CreatedBy: "t",
	}); err != nil {
		t.Fatalf("PutEdge symlink: %v", err)
	}

	out, err := s.Outgoing(ctx, a.LineageID, memgraph.TraverseOpts{EdgeKinds: []string{"cites"}})
	if err != nil {
		t.Fatalf("Outgoing: %v", err)
	}
	if len(out) != 2 {
		t.Fatalf("expected 2 cites-outgoing, got %d", len(out))
	}

	in, err := s.Incoming(ctx, b.LineageID, memgraph.TraverseOpts{})
	if err != nil {
		t.Fatalf("Incoming: %v", err)
	}
	if len(in) != 2 {
		t.Fatalf("expected 2 incoming to b, got %d", len(in))
	}

	// Traverse depth=1, no symlinks: should reach b but not c or x.
	tr1, err := s.Traverse(ctx, a.LineageID, memgraph.TraverseOpts{MaxDepth: 1})
	if err != nil {
		t.Fatalf("Traverse: %v", err)
	}
	seen := map[memgraph.LineageID]bool{}
	for _, n := range tr1.Nodes {
		seen[n.LineageID] = true
	}
	if !seen[a.LineageID] || !seen[b.LineageID] {
		t.Fatalf("traverse depth=1 missing a or b: %v", seen)
	}
	if seen[c.LineageID] || seen[x.LineageID] {
		t.Fatalf("traverse depth=1 should not reach c/x: %v", seen)
	}

	// Traverse depth=2, no symlinks: a,b,c but not x.
	tr2, err := s.Traverse(ctx, a.LineageID, memgraph.TraverseOpts{MaxDepth: 3})
	if err != nil {
		t.Fatalf("Traverse: %v", err)
	}
	seen = map[memgraph.LineageID]bool{}
	for _, n := range tr2.Nodes {
		seen[n.LineageID] = true
	}
	if !seen[c.LineageID] {
		t.Fatalf("traverse should reach c without symlinks")
	}
	if seen[x.LineageID] {
		t.Fatalf("traverse should NOT reach x without FollowSymlinks")
	}

	// FollowSymlinks=true reaches x.
	tr3, err := s.Traverse(ctx, a.LineageID, memgraph.TraverseOpts{
		MaxDepth: 3, FollowSymlinks: true,
	})
	if err != nil {
		t.Fatalf("Traverse: %v", err)
	}
	seen = map[memgraph.LineageID]bool{}
	for _, n := range tr3.Nodes {
		seen[n.LineageID] = true
	}
	if !seen[x.LineageID] {
		t.Fatalf("traverse with FollowSymlinks should reach x")
	}
}

func TestSearchCurrentOnly(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	g := mustCreateGraph(t, s, memgraph.GraphInput{Name: "g"})

	v1 := mustPutNode(t, s, memgraph.NodeInput{
		GraphID: g.ID, Kind: "fact",
		Content: "alpha beta zebra", Tags: []string{"zoo"}, CreatedBy: "t",
	})
	// Hit before supersede.
	hits, err := s.Search(ctx, g.ID, memgraph.SearchQuery{Text: "zebra"})
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(hits) != 1 {
		t.Fatalf("expected 1 hit, got %d", len(hits))
	}
	if hits[0].Snippet == "" {
		t.Fatalf("expected non-empty snippet")
	}

	// New version replaces "zebra" with "moose".
	_ = mustPutNode(t, s, memgraph.NodeInput{
		GraphID: g.ID, LineageID: v1.LineageID, Kind: "fact",
		Content: "alpha beta moose", CreatedBy: "t",
	})

	hits, err = s.Search(ctx, g.ID, memgraph.SearchQuery{Text: "zebra"})
	if err != nil {
		t.Fatalf("Search after supersede: %v", err)
	}
	if len(hits) != 0 {
		t.Fatalf("zebra should no longer match: %d hits", len(hits))
	}
	hits, err = s.Search(ctx, g.ID, memgraph.SearchQuery{Text: "moose"})
	if err != nil {
		t.Fatalf("Search moose: %v", err)
	}
	if len(hits) != 1 || hits[0].Node.Version != 2 {
		t.Fatalf("moose should match current v2: %+v", hits)
	}
}

func TestSymlinkManifest(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	g1 := mustCreateGraph(t, s, memgraph.GraphInput{Name: "g1"})
	g2 := mustCreateGraph(t, s, memgraph.GraphInput{Name: "g2"})
	g3 := mustCreateGraph(t, s, memgraph.GraphInput{Name: "g3"})

	a := mustPutNode(t, s, memgraph.NodeInput{GraphID: g1.ID, Kind: "fact", Content: "a", CreatedBy: "t"})
	b := mustPutNode(t, s, memgraph.NodeInput{GraphID: g2.ID, Kind: "fact", Content: "b", CreatedBy: "t"})
	c := mustPutNode(t, s, memgraph.NodeInput{GraphID: g3.ID, Kind: "fact", Content: "c", CreatedBy: "t"})

	// g1 -> g2 twice, g1 -> g3 once.
	for i := 0; i < 2; i++ {
		if _, err := s.PutEdge(ctx, memgraph.EdgeInput{
			GraphID: g1.ID, FromLineage: a.LineageID,
			ToGraph: g2.ID, ToLineage: b.LineageID,
			Kind: "cites", CreatedBy: "t",
		}); err != nil {
			t.Fatalf("PutEdge: %v", err)
		}
	}
	if _, err := s.PutEdge(ctx, memgraph.EdgeInput{
		GraphID: g1.ID, FromLineage: a.LineageID,
		ToGraph: g3.ID, ToLineage: c.LineageID,
		Kind: "cites", CreatedBy: "t",
	}); err != nil {
		t.Fatalf("PutEdge: %v", err)
	}

	man, err := s.SymlinkManifest(ctx, g1.ID)
	if err != nil {
		t.Fatalf("manifest: %v", err)
	}
	if len(man.Outbound) != 2 {
		t.Fatalf("g1 outbound expected 2 distinct graphs, got %d", len(man.Outbound))
	}
	totalOut := 0
	for _, r := range man.Outbound {
		totalOut += r.EdgeCount
	}
	if totalOut != 3 {
		t.Fatalf("g1 outbound edge count expected 3, got %d", totalOut)
	}

	man, err = s.SymlinkManifest(ctx, g2.ID)
	if err != nil {
		t.Fatalf("manifest g2: %v", err)
	}
	if len(man.Inbound) != 1 || man.Inbound[0].GraphID != g1.ID || man.Inbound[0].EdgeCount != 2 {
		t.Fatalf("g2 inbound expected 1 ref from g1 count=2, got %+v", man.Inbound)
	}
}

type captureHandler struct {
	nodes int32
	edges int32
}

func (h *captureHandler) OnNodeWritten(_ context.Context, _ memgraph.Node) { atomic.AddInt32(&h.nodes, 1) }
func (h *captureHandler) OnEdgeWritten(_ context.Context, _ memgraph.Edge) { atomic.AddInt32(&h.edges, 1) }

func TestSubscribe(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	g := mustCreateGraph(t, s, memgraph.GraphInput{Name: "g"})

	h := &captureHandler{}
	unsub, err := s.Subscribe(h)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	n := mustPutNode(t, s, memgraph.NodeInput{GraphID: g.ID, Kind: "fact", Content: "x", CreatedBy: "t"})
	if _, err := s.PutEdge(ctx, memgraph.EdgeInput{
		GraphID: g.ID, FromLineage: n.LineageID, ToLineage: n.LineageID,
		Kind: "self", CreatedBy: "t",
	}); err != nil {
		t.Fatalf("PutEdge: %v", err)
	}
	if got := atomic.LoadInt32(&h.nodes); got != 1 {
		t.Fatalf("expected 1 node notification, got %d", got)
	}
	if got := atomic.LoadInt32(&h.edges); got != 1 {
		t.Fatalf("expected 1 edge notification, got %d", got)
	}

	unsub()
	_ = mustPutNode(t, s, memgraph.NodeInput{GraphID: g.ID, Kind: "fact", Content: "y", CreatedBy: "t"})
	if got := atomic.LoadInt32(&h.nodes); got != 1 {
		t.Fatalf("after unsubscribe expected still 1, got %d", got)
	}
}
