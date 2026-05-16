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

func TestTagFilteringExactMatch(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	g := mustCreateGraph(t, s, memgraph.GraphInput{Name: "g"})

	// "bar" must NOT match a filter for "barb" (substring) or "barbecue".
	nBar := mustPutNode(t, s, memgraph.NodeInput{
		GraphID: g.ID, Kind: "fact", Content: "shortlived", Tags: []string{"bar"}, CreatedBy: "t",
	})
	nBarbecue := mustPutNode(t, s, memgraph.NodeInput{
		GraphID: g.ID, Kind: "fact", Content: "smokey", Tags: []string{"barbecue"}, CreatedBy: "t",
	})
	// A node carrying both tags to verify intersection AND semantics.
	nBoth := mustPutNode(t, s, memgraph.NodeInput{
		GraphID: g.ID, Kind: "fact", Content: "both", Tags: []string{"bar", "barbecue"}, CreatedBy: "t",
	})

	// Filter tag = "bar" matches only bar-tagged nodes (nBar and nBoth), NOT barbecue-only.
	got, err := s.ListNodes(ctx, g.ID, memgraph.NodeFilter{Tags: []string{"bar"}})
	if err != nil {
		t.Fatalf("ListNodes tag=bar: %v", err)
	}
	ids := map[memgraph.NodeID]bool{}
	for _, n := range got {
		ids[n.ID] = true
	}
	if !ids[nBar.ID] || !ids[nBoth.ID] || ids[nBarbecue.ID] {
		t.Fatalf("tag=bar substring leak: got ids=%v (want nBar+nBoth; not nBarbecue)", ids)
	}

	// Filter tag = "barb" matches NOTHING — no exact tag is "barb".
	got, err = s.ListNodes(ctx, g.ID, memgraph.NodeFilter{Tags: []string{"barb"}})
	if err != nil {
		t.Fatalf("ListNodes tag=barb: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("tag=barb should match nothing, got %d nodes", len(got))
	}

	// Multiple tags = AND: must have both "bar" AND "barbecue".
	got, err = s.ListNodes(ctx, g.ID, memgraph.NodeFilter{Tags: []string{"bar", "barbecue"}})
	if err != nil {
		t.Fatalf("ListNodes tag=bar,barbecue: %v", err)
	}
	if len(got) != 1 || got[0].ID != nBoth.ID {
		t.Fatalf("tag intersection: want only nBoth, got %+v", got)
	}

	// Search with tag filter respects the same exact-match semantics.
	hits, err := s.Search(ctx, g.ID, memgraph.SearchQuery{Text: "smokey", Tags: []string{"barb"}})
	if err != nil {
		t.Fatalf("Search tag=barb: %v", err)
	}
	if len(hits) != 0 {
		t.Fatalf("search tag=barb should return 0 hits, got %d", len(hits))
	}
	hits, err = s.Search(ctx, g.ID, memgraph.SearchQuery{Text: "smokey", Tags: []string{"barbecue"}})
	if err != nil {
		t.Fatalf("Search tag=barbecue: %v", err)
	}
	if len(hits) != 1 || hits[0].Node.ID != nBarbecue.ID {
		t.Fatalf("search tag=barbecue: want 1 hit (nBarbecue), got %+v", hits)
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

func TestManualConflictPolicy(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	g := mustCreateGraph(t, s, memgraph.GraphInput{
		Name:           "manual",
		ConflictPolicy: memgraph.ConflictPolicyManual,
	})

	// v1 seeds the lineage.
	v1 := mustPutNode(t, s, memgraph.NodeInput{
		GraphID: g.ID, Kind: "fact", Content: "v1", CreatedBy: "t",
	})

	// Two writers both believe v1 is current. First wins cleanly (single
	// head → matched → supersedes v1). Second sees v1 superseded already
	// but its BasedOnVersion=1 doesn't match the new head (v2) — conflict.
	basedOn := 1
	v2a, err := s.PutNode(ctx, memgraph.NodeInput{
		GraphID: g.ID, LineageID: v1.LineageID, Kind: "fact",
		Content: "v2a", CreatedBy: "alice", BasedOnVersion: &basedOn,
	})
	if err != nil {
		t.Fatalf("first concurrent put should succeed: %v", err)
	}
	if v2a.Version != 2 || len(v2a.Conflicts) != 0 {
		t.Fatalf("v2a unexpected: %+v", v2a)
	}
	// v2a is the only head right now; v2a.Version=2.

	v2b, err := s.PutNode(ctx, memgraph.NodeInput{
		GraphID: g.ID, LineageID: v1.LineageID, Kind: "fact",
		Content: "v2b", CreatedBy: "bob", BasedOnVersion: &basedOn,
	})
	if !errors.Is(err, memgraph.ErrConflictManual) {
		t.Fatalf("second concurrent put under manual should return ErrConflictManual, got %v", err)
	}
	if v2b.ID == "" {
		t.Fatalf("v2b should still be returned: %+v", v2b)
	}
	if len(v2b.Conflicts) != 1 || v2b.Conflicts[0] != v2a.ID {
		t.Fatalf("v2b.Conflicts should be [v2a.ID]: %+v", v2b.Conflicts)
	}

	// Both v2a and v2b must remain as non-superseded heads.
	a, err := s.GetNodeByID(ctx, v2a.ID)
	if err != nil {
		t.Fatalf("GetNodeByID v2a: %v", err)
	}
	if a.SupersededBy != nil {
		t.Fatalf("v2a should still be a head: %+v", a)
	}
	if len(a.Conflicts) != 1 || a.Conflicts[0] != v2b.ID {
		t.Fatalf("v2a should know about v2b sibling: %+v", a.Conflicts)
	}
	b, err := s.GetNodeByID(ctx, v2b.ID)
	if err != nil {
		t.Fatalf("GetNodeByID v2b: %v", err)
	}
	if b.SupersededBy != nil {
		t.Fatalf("v2b should still be a head: %+v", b)
	}
	if len(b.Conflicts) != 1 || b.Conflicts[0] != v2a.ID {
		t.Fatalf("v2b should know about v2a sibling: %+v", b.Conflicts)
	}

	// GetNodeByLineage returns the head with the HIGHEST version, with
	// Conflicts populated. v2b was written second so its version is one
	// greater than v2a's; v2b should win the tiebreak.
	cur, err := s.GetNodeByLineage(ctx, v1.LineageID, memgraph.ReadOpts{})
	if err != nil {
		t.Fatalf("GetNodeByLineage: %v", err)
	}
	if cur.ID != v2b.ID {
		t.Fatalf("current should be highest-version head v2b: %+v", cur)
	}
	if len(cur.Conflicts) != 1 || cur.Conflicts[0] != v2a.ID {
		t.Fatalf("current.Conflicts should reference v2a: %+v", cur.Conflicts)
	}

	// Search must surface BOTH heads when content matches.
	hits, err := s.Search(ctx, g.ID, memgraph.SearchQuery{Text: "v2a"})
	if err != nil {
		t.Fatalf("Search v2a: %v", err)
	}
	if len(hits) != 1 || hits[0].Node.ID != v2a.ID {
		t.Fatalf("expected to find v2a, got %+v", hits)
	}
	hits, err = s.Search(ctx, g.ID, memgraph.SearchQuery{Text: "v2b"})
	if err != nil {
		t.Fatalf("Search v2b: %v", err)
	}
	if len(hits) != 1 || hits[0].Node.ID != v2b.ID {
		t.Fatalf("expected to find v2b, got %+v", hits)
	}

	// Resolve: a write with BasedOnVersion matching either sibling should
	// supersede BOTH siblings. We pass v2a's version (2). The resolution
	// becomes max(heads.version)+1 = v2b.Version + 1.
	resolveBase := v2a.Version
	v3, err := s.PutNode(ctx, memgraph.NodeInput{
		GraphID: g.ID, LineageID: v1.LineageID, Kind: "fact",
		Content: "v3 resolution", CreatedBy: "carol", BasedOnVersion: &resolveBase,
	})
	if err != nil {
		t.Fatalf("resolution put should succeed: %v", err)
	}
	if v3.Version != v2b.Version+1 || len(v3.Conflicts) != 0 {
		t.Fatalf("v3 unexpected: %+v", v3)
	}

	cur, err = s.GetNodeByLineage(ctx, v1.LineageID, memgraph.ReadOpts{})
	if err != nil {
		t.Fatalf("GetNodeByLineage after resolve: %v", err)
	}
	if cur.ID != v3.ID || len(cur.Conflicts) != 0 {
		t.Fatalf("after resolve, current should be v3 with no conflicts: %+v", cur)
	}

	for _, id := range []memgraph.NodeID{v2a.ID, v2b.ID} {
		got, err := s.GetNodeByID(ctx, id)
		if err != nil {
			t.Fatalf("GetNodeByID %v: %v", id, err)
		}
		if got.SupersededBy == nil || *got.SupersededBy != v3.ID {
			t.Fatalf("expected %v superseded by v3, got %+v", id, got)
		}
	}
}

func TestLWWUnderConcurrentBasedOn(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	g := mustCreateGraph(t, s, memgraph.GraphInput{
		Name:           "lww",
		ConflictPolicy: memgraph.ConflictPolicyLWW,
	})

	v1 := mustPutNode(t, s, memgraph.NodeInput{
		GraphID: g.ID, Kind: "fact", Content: "v1", CreatedBy: "t",
	})

	basedOn := 1
	v2a, err := s.PutNode(ctx, memgraph.NodeInput{
		GraphID: g.ID, LineageID: v1.LineageID, Kind: "fact",
		Content: "v2a", CreatedBy: "alice", BasedOnVersion: &basedOn,
	})
	if err != nil {
		t.Fatalf("first put: %v", err)
	}
	v2b, err := s.PutNode(ctx, memgraph.NodeInput{
		GraphID: g.ID, LineageID: v1.LineageID, Kind: "fact",
		Content: "v2b", CreatedBy: "bob", BasedOnVersion: &basedOn,
	})
	if err != nil {
		t.Fatalf("second LWW put should NOT error, got %v", err)
	}
	if len(v2b.Conflicts) != 0 {
		t.Fatalf("LWW should not record conflicts: %+v", v2b.Conflicts)
	}

	// v2a should now be superseded by v2b.
	a, err := s.GetNodeByID(ctx, v2a.ID)
	if err != nil {
		t.Fatalf("GetNodeByID: %v", err)
	}
	if a.SupersededBy == nil || *a.SupersededBy != v2b.ID {
		t.Fatalf("v2a should be superseded by v2b under LWW: %+v", a)
	}

	// Exactly one head, no conflicts surfaced.
	cur, err := s.GetNodeByLineage(ctx, v1.LineageID, memgraph.ReadOpts{})
	if err != nil {
		t.Fatalf("GetNodeByLineage: %v", err)
	}
	if cur.ID != v2b.ID || len(cur.Conflicts) != 0 {
		t.Fatalf("LWW current should be v2b with no conflicts: %+v", cur)
	}
}

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
