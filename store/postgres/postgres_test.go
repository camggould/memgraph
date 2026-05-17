package postgres_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	memgraph "github.com/camggould/memgraph"
	"github.com/camggould/memgraph/store/postgres"
	"github.com/jackc/pgx/v5"
)

// resolveDSN returns the DSN to test against. Priority:
//  1. MEMGRAPH_POSTGRES_DSN env var (explicit).
//  2. A locally reachable Postgres via libpq defaults (PGHOST, PGUSER, etc.),
//     defaulting to the current user's connection on localhost.
// If neither works, the test skips.
func resolveDSN(t *testing.T) string {
	t.Helper()
	if dsn := os.Getenv("MEMGRAPH_POSTGRES_DSN"); dsn != "" {
		return dsn
	}
	// Try a default local connection to the "postgres" database. pgx
	// honors libpq env vars (PGHOST/PGUSER/PGPORT) and the unix socket
	// search path.
	dsn := "postgres:///postgres"
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Skip("MEMGRAPH_POSTGRES_DSN not set and no local Postgres reachable")
	}
	_ = conn.Close(ctx)
	return dsn
}

// openTestStore creates a unique schema in the target Postgres database,
// sets search_path to that schema, opens a Store against it, and arranges
// to drop the schema on cleanup. This gives each test an isolated namespace
// without the cost of creating a database.
func openTestStore(t *testing.T) *postgres.Store {
	t.Helper()
	baseDSN := resolveDSN(t)

	buf := make([]byte, 8)
	if _, err := rand.Read(buf); err != nil {
		t.Fatalf("rand: %v", err)
	}
	schema := "memgraph_test_" + hex.EncodeToString(buf)

	// Create the schema using a one-shot admin connection.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	admin, err := pgx.Connect(ctx, baseDSN)
	if err != nil {
		t.Fatalf("connect admin: %v", err)
	}
	if _, err := admin.Exec(ctx, fmt.Sprintf(`CREATE SCHEMA %q`, schema)); err != nil {
		admin.Close(ctx)
		t.Fatalf("create schema: %v", err)
	}
	admin.Close(ctx)

	// Build a per-store DSN that pins search_path to the new schema.
	storeDSN := withSearchPath(baseDSN, schema)
	s, err := postgres.Open(storeDSN)
	if err != nil {
		// Best-effort cleanup.
		ctx2, c2 := context.WithTimeout(context.Background(), 5*time.Second)
		defer c2()
		if admin2, err2 := pgx.Connect(ctx2, baseDSN); err2 == nil {
			_, _ = admin2.Exec(ctx2, fmt.Sprintf(`DROP SCHEMA %q CASCADE`, schema))
			admin2.Close(ctx2)
		}
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() {
		_ = s.Close()
		ctx2, c2 := context.WithTimeout(context.Background(), 5*time.Second)
		defer c2()
		admin2, err2 := pgx.Connect(ctx2, baseDSN)
		if err2 != nil {
			return
		}
		defer admin2.Close(ctx2)
		_, _ = admin2.Exec(ctx2, fmt.Sprintf(`DROP SCHEMA %q CASCADE`, schema))
	})
	return s
}

// withSearchPath appends/overrides search_path on the DSN. Works for both
// URL-style (postgres://) and keyword/value DSNs.
func withSearchPath(dsn, schema string) string {
	// Search path option that includes the new schema first, then public
	// so pg_trgm functions (installed in public) remain resolvable.
	opt := "-c search_path=" + schema + ",public"
	if strings.HasPrefix(dsn, "postgres://") || strings.HasPrefix(dsn, "postgresql://") {
		sep := "?"
		if strings.Contains(dsn, "?") {
			sep = "&"
		}
		return dsn + sep + "options=" + urlEscape(opt)
	}
	// keyword/value DSN
	return dsn + " options='" + opt + "'"
}

func urlEscape(s string) string {
	// Minimal escaper for our known options string.
	r := strings.NewReplacer(" ", "%20", "=", "%3D", ",", "%2C")
	return r.Replace(s)
}

func mustCreateGraph(t *testing.T, s *postgres.Store, in memgraph.GraphInput) memgraph.Graph {
	t.Helper()
	g, err := s.CreateGraph(context.Background(), in)
	if err != nil {
		t.Fatalf("CreateGraph: %v", err)
	}
	return g
}

func mustPutNode(t *testing.T, s *postgres.Store, in memgraph.NodeInput) memgraph.Node {
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

	cur, err := s.GetNodeByLineage(ctx, v1.LineageID, memgraph.ReadOpts{})
	if err != nil || cur.ID != v2.ID {
		t.Fatalf("current resolution: %v %+v", err, cur)
	}

	old, err := s.GetNodeByID(ctx, v1.ID)
	if err != nil || old.SupersededBy == nil || *old.SupersededBy != v2.ID {
		t.Fatalf("v1 should be superseded by v2: %v %+v", err, old)
	}

	one := 1
	at, err := s.GetNodeByLineage(ctx, v1.LineageID, memgraph.ReadOpts{AtVersion: &one})
	if err != nil || at.ID != v1.ID {
		t.Fatalf("AtVersion=1 should return v1: %v %+v", err, at)
	}

	at, err = s.GetNodeByLineage(ctx, v1.LineageID, memgraph.ReadOpts{AtTime: &v1Time})
	if err != nil || at.ID != v1.ID {
		t.Fatalf("AtTime should return v1: %v %+v", err, at)
	}

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

	if _, err := s.PutEdge(ctx, memgraph.EdgeInput{
		GraphID: g1.ID, FromLineage: a.LineageID, ToLineage: b.LineageID, Kind: "cites", CreatedBy: "t",
	}); err != nil {
		t.Fatalf("PutEdge: %v", err)
	}
	if _, err := s.PutEdge(ctx, memgraph.EdgeInput{
		GraphID: g1.ID, FromLineage: b.LineageID, ToLineage: c.LineageID, Kind: "cites", CreatedBy: "t",
	}); err != nil {
		t.Fatalf("PutEdge: %v", err)
	}
	if _, err := s.PutEdge(ctx, memgraph.EdgeInput{
		GraphID: g1.ID, FromLineage: a.LineageID, ToLineage: b.LineageID, Kind: "mentions", CreatedBy: "t",
	}); err != nil {
		t.Fatalf("PutEdge: %v", err)
	}
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

// buildChainABC creates A->B->C in a fresh graph plus a disconnected D.
func buildChainABC(t *testing.T, s *postgres.Store) (memgraph.Node, memgraph.Node, memgraph.Node, memgraph.Node) {
	t.Helper()
	ctx := context.Background()
	g := mustCreateGraph(t, s, memgraph.GraphInput{Name: "chain"})
	a := mustPutNode(t, s, memgraph.NodeInput{GraphID: g.ID, Kind: "fact", Content: "a", CreatedBy: "t"})
	b := mustPutNode(t, s, memgraph.NodeInput{GraphID: g.ID, Kind: "fact", Content: "b", CreatedBy: "t"})
	c := mustPutNode(t, s, memgraph.NodeInput{GraphID: g.ID, Kind: "fact", Content: "c", CreatedBy: "t"})
	d := mustPutNode(t, s, memgraph.NodeInput{GraphID: g.ID, Kind: "fact", Content: "d", CreatedBy: "t"})
	if _, err := s.PutEdge(ctx, memgraph.EdgeInput{
		GraphID: g.ID, FromLineage: a.LineageID, ToLineage: b.LineageID, Kind: "cites", CreatedBy: "t",
	}); err != nil {
		t.Fatalf("PutEdge a->b: %v", err)
	}
	if _, err := s.PutEdge(ctx, memgraph.EdgeInput{
		GraphID: g.ID, FromLineage: b.LineageID, ToLineage: c.LineageID, Kind: "cites", CreatedBy: "t",
	}); err != nil {
		t.Fatalf("PutEdge b->c: %v", err)
	}
	return a, b, c, d
}

func TestTraverse_DirectionIncoming(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	a, b, c, d := buildChainABC(t, s)

	tr, err := s.Traverse(ctx, c.LineageID, memgraph.TraverseOpts{
		MaxDepth:  3,
		Direction: memgraph.TraverseIncoming,
	})
	if err != nil {
		t.Fatalf("Traverse: %v", err)
	}
	seen := map[memgraph.LineageID]bool{}
	for _, n := range tr.Nodes {
		seen[n.LineageID] = true
	}
	if !seen[a.LineageID] || !seen[b.LineageID] || !seen[c.LineageID] {
		t.Fatalf("incoming traverse from c should reach a, b, c: %v", seen)
	}
	if seen[d.LineageID] {
		t.Fatalf("incoming traverse must not reach disconnected d: %v", seen)
	}
	if len(tr.Edges) != 2 {
		t.Fatalf("expected 2 edges, got %d", len(tr.Edges))
	}
}

func TestTraverse_DirectionBoth(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	a, b, c, d := buildChainABC(t, s)

	tr, err := s.Traverse(ctx, b.LineageID, memgraph.TraverseOpts{
		MaxDepth:  3,
		Direction: memgraph.TraverseBoth,
	})
	if err != nil {
		t.Fatalf("Traverse: %v", err)
	}
	seen := map[memgraph.LineageID]bool{}
	for _, n := range tr.Nodes {
		seen[n.LineageID] = true
	}
	if !seen[a.LineageID] || !seen[b.LineageID] || !seen[c.LineageID] {
		t.Fatalf("both traverse from b should reach a, b, c: %v", seen)
	}
	if seen[d.LineageID] {
		t.Fatalf("both traverse must not reach disconnected d: %v", seen)
	}
	if len(tr.Edges) != 2 {
		t.Fatalf("expected 2 deduped edges, got %d", len(tr.Edges))
	}
}

func TestTraverse_DefaultIsOutgoing(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	a, b, c, _ := buildChainABC(t, s)

	tr, err := s.Traverse(ctx, a.LineageID, memgraph.TraverseOpts{MaxDepth: 3})
	if err != nil {
		t.Fatalf("Traverse: %v", err)
	}
	seen := map[memgraph.LineageID]bool{}
	for _, n := range tr.Nodes {
		seen[n.LineageID] = true
	}
	if !seen[a.LineageID] || !seen[b.LineageID] || !seen[c.LineageID] {
		t.Fatalf("default direction (outgoing) from a should reach a, b, c: %v", seen)
	}

	tr2, err := s.Traverse(ctx, c.LineageID, memgraph.TraverseOpts{MaxDepth: 3})
	if err != nil {
		t.Fatalf("Traverse: %v", err)
	}
	if len(tr2.Nodes) != 1 || tr2.Nodes[0].LineageID != c.LineageID {
		t.Fatalf("default direction from c should only reach c: %+v", tr2.Nodes)
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

	nBar := mustPutNode(t, s, memgraph.NodeInput{
		GraphID: g.ID, Kind: "fact", Content: "shortlived", Tags: []string{"bar"}, CreatedBy: "t",
	})
	nBarbecue := mustPutNode(t, s, memgraph.NodeInput{
		GraphID: g.ID, Kind: "fact", Content: "smokey", Tags: []string{"barbecue"}, CreatedBy: "t",
	})
	nBoth := mustPutNode(t, s, memgraph.NodeInput{
		GraphID: g.ID, Kind: "fact", Content: "both", Tags: []string{"bar", "barbecue"}, CreatedBy: "t",
	})

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

	got, err = s.ListNodes(ctx, g.ID, memgraph.NodeFilter{Tags: []string{"barb"}})
	if err != nil {
		t.Fatalf("ListNodes tag=barb: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("tag=barb should match nothing, got %d nodes", len(got))
	}

	got, err = s.ListNodes(ctx, g.ID, memgraph.NodeFilter{Tags: []string{"bar", "barbecue"}})
	if err != nil {
		t.Fatalf("ListNodes tag=bar,barbecue: %v", err)
	}
	if len(got) != 1 || got[0].ID != nBoth.ID {
		t.Fatalf("tag intersection: want only nBoth, got %+v", got)
	}

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
	nodes  int32
	edges  int32
	graphs int32
}

func (h *captureHandler) OnNodeWritten(_ context.Context, _ memgraph.Node) { atomic.AddInt32(&h.nodes, 1) }
func (h *captureHandler) OnEdgeWritten(_ context.Context, _ memgraph.Edge) { atomic.AddInt32(&h.edges, 1) }
func (h *captureHandler) OnGraphCreated(_ context.Context, _ memgraph.Graph) {
	atomic.AddInt32(&h.graphs, 1)
}

func TestManualConflictPolicy(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	g := mustCreateGraph(t, s, memgraph.GraphInput{
		Name:           "manual",
		ConflictPolicy: memgraph.ConflictPolicyManual,
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
		t.Fatalf("first concurrent put should succeed: %v", err)
	}
	if v2a.Version != 2 || len(v2a.Conflicts) != 0 {
		t.Fatalf("v2a unexpected: %+v", v2a)
	}

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

	a, err := s.GetNodeByID(ctx, v2a.ID)
	if err != nil {
		t.Fatalf("GetNodeByID: %v", err)
	}
	if a.SupersededBy == nil || *a.SupersededBy != v2b.ID {
		t.Fatalf("v2a should be superseded by v2b under LWW: %+v", a)
	}

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

func TestSubscribe_Graph(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	h := &captureHandler{}
	unsub, err := s.Subscribe(h)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	t.Cleanup(unsub)

	g, err := s.CreateGraph(ctx, memgraph.GraphInput{Name: "g"})
	if err != nil {
		t.Fatalf("CreateGraph: %v", err)
	}
	if got := atomic.LoadInt32(&h.graphs); got != 1 {
		t.Fatalf("expected 1 graph notification after CreateGraph, got %d", got)
	}

	newName := "renamed"
	if _, err := s.UpdateGraphConfig(ctx, g.ID, memgraph.GraphConfigPatch{Name: &newName}); err != nil {
		t.Fatalf("UpdateGraphConfig: %v", err)
	}
	if got := atomic.LoadInt32(&h.graphs); got != 2 {
		t.Fatalf("expected 2 graph notifications after UpdateGraphConfig, got %d", got)
	}

	unsub()
	if _, err := s.CreateGraph(ctx, memgraph.GraphInput{Name: "ignored"}); err != nil {
		t.Fatalf("CreateGraph: %v", err)
	}
	if got := atomic.LoadInt32(&h.graphs); got != 2 {
		t.Fatalf("after unsubscribe expected still 2 graph notifications, got %d", got)
	}
}
