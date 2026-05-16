package kbmigrate_test

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"sort"
	"testing"

	memgraph "github.com/camggould/memgraph"
	"github.com/camggould/memgraph/internal/kbmigrate"
	"github.com/camggould/memgraph/store/sqlite"
	_ "modernc.org/sqlite"
)

// kbRow is a convenience record for seeding the kb fixture DB.
type kbRow struct {
	id        string
	title     string
	body      string
	tags      sql.NullString // raw JSON string, or NULL
	source    sql.NullString
	context   sql.NullString
	workspace sql.NullString
	links     sql.NullString
	created   sql.NullString
	modified  sql.NullString
	filePath  sql.NullString
}

func nullable(s string) sql.NullString {
	return sql.NullString{String: s, Valid: true}
}

func null() sql.NullString { return sql.NullString{} }

// makeKBDB creates a kb SQLite DB at path and inserts the given rows.
func makeKBDB(t *testing.T, path string, rows []kbRow) {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+path)
	if err != nil {
		t.Fatalf("open kb db: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	if _, err := db.Exec(`CREATE TABLE notes (
		id TEXT PRIMARY KEY,
		title TEXT,
		body TEXT,
		tags TEXT,
		source TEXT,
		context TEXT,
		workspace TEXT,
		links TEXT,
		created TEXT,
		modified TEXT,
		file_path TEXT
	)`); err != nil {
		t.Fatalf("create notes table: %v", err)
	}

	for _, r := range rows {
		if _, err := db.Exec(
			`INSERT INTO notes(id, title, body, tags, source, context, workspace,
			                   links, created, modified, file_path)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			r.id, r.title, r.body, r.tags, r.source, r.context, r.workspace,
			r.links, r.created, r.modified, r.filePath,
		); err != nil {
			t.Fatalf("insert note %s: %v", r.id, err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close kb db: %v", err)
	}
}

func openTarget(t *testing.T) (*sqlite.Store, string) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "memgraph.db")
	store, err := sqlite.Open(path)
	if err != nil {
		t.Fatalf("open target store: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	return store, path
}

func TestMigrate_HappyPath_ThreeWorkspaces(t *testing.T) {
	dir := t.TempDir()
	kbPath := filepath.Join(dir, "kb.db")

	// Three workspaces: explicit "work", "personal", and one note
	// with NULL workspace + one with whitespace-only — both should land
	// in "default".
	makeKBDB(t, kbPath, []kbRow{
		{
			id: "n1", title: "Tabs vs spaces", body: "Prefer tabs in Go",
			tags:      nullable(`["go","style"]`),
			workspace: nullable("work"),
			links:     nullable(`["n2"]`),
			created:   nullable("2026-01-01T00:00:00Z"),
		},
		{
			id: "n2", title: "Gofmt", body: "Run gofmt on save",
			tags:      nullable(`["go"]`),
			workspace: nullable("work"),
			source:    nullable("docs"),
			created:   nullable("2026-01-02T00:00:00Z"),
		},
		{
			id: "n3", title: "Recipe", body: "Sourdough",
			workspace: nullable("personal"),
			created:   nullable("2026-01-03T00:00:00Z"),
		},
		{
			id: "n4", title: "Scratch", body: "Random idea",
			workspace: null(),
			created:   nullable("2026-01-04T00:00:00Z"),
		},
		{
			id: "n5", title: "Other", body: "Whitespace ws",
			workspace: nullable("   "),
			created:   nullable("2026-01-05T00:00:00Z"),
		},
	})

	store, _ := openTarget(t)
	ctx := context.Background()

	report, err := kbmigrate.Migrate(ctx, kbPath, store, kbmigrate.Options{})
	if err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	// 3 graphs: work (2), personal (1), default (2). Ordering follows
	// first-seen workspace (work, personal, default).
	if got, want := len(report.Graphs), 3; got != want {
		t.Fatalf("graphs: got %d, want %d (%+v)", got, want, report.Graphs)
	}

	wantOrder := []struct {
		name  string
		nodes int
	}{
		{"work", 2}, {"personal", 1}, {"default", 2},
	}
	for i, w := range wantOrder {
		if report.Graphs[i].Name != w.name {
			t.Errorf("graph[%d].Name = %q, want %q", i, report.Graphs[i].Name, w.name)
		}
		if report.Graphs[i].NodeCount != w.nodes {
			t.Errorf("graph[%d].NodeCount = %d, want %d", i, report.Graphs[i].NodeCount, w.nodes)
		}
	}

	// Verify the graphs really exist in the target store.
	graphs, err := store.ListGraphs(ctx)
	if err != nil {
		t.Fatalf("ListGraphs: %v", err)
	}
	names := map[string]bool{}
	for _, g := range graphs {
		names[g.Name] = true
	}
	for _, name := range []string{"work", "personal", "default"} {
		if !names[name] {
			t.Errorf("graph %q not present in store", name)
		}
	}

	// One edge (n1 -> n2, both in work). No skipped links.
	if report.EdgesCreated != 1 {
		t.Errorf("EdgesCreated = %d, want 1", report.EdgesCreated)
	}
	if len(report.SkippedLinks) != 0 {
		t.Errorf("SkippedLinks = %d, want 0 (%+v)", len(report.SkippedLinks), report.SkippedLinks)
	}

	// Inspect the work graph: 2 fact nodes, n1 has its tags and metadata.
	var workGraph memgraph.GraphID
	for _, g := range report.Graphs {
		if g.Name == "work" {
			workGraph = g.ID
		}
	}
	nodes, err := store.ListNodes(ctx, workGraph, memgraph.NodeFilter{})
	if err != nil {
		t.Fatalf("ListNodes: %v", err)
	}
	if len(nodes) != 2 {
		t.Fatalf("work graph nodes = %d, want 2", len(nodes))
	}

	var n1Node memgraph.Node
	for _, n := range nodes {
		if n.Summary == "Tabs vs spaces" {
			n1Node = n
		}
	}
	if n1Node.Kind != "fact" {
		t.Errorf("n1.Kind = %q, want fact", n1Node.Kind)
	}
	if n1Node.Content != "Prefer tabs in Go" {
		t.Errorf("n1.Content = %q", n1Node.Content)
	}
	if n1Node.CreatedBy != kbmigrate.CreatedBy {
		t.Errorf("n1.CreatedBy = %q, want %q", n1Node.CreatedBy, kbmigrate.CreatedBy)
	}
	gotTags := append([]string(nil), n1Node.Tags...)
	sort.Strings(gotTags)
	wantTags := []string{"go", "style"}
	if fmt.Sprint(gotTags) != fmt.Sprint(wantTags) {
		t.Errorf("n1.Tags = %v, want %v", gotTags, wantTags)
	}
	if got := n1Node.Metadata["kb_workspace"]; got != "work" {
		t.Errorf("n1.Metadata[kb_workspace] = %v, want work", got)
	}
	if got := n1Node.Metadata["kb_created"]; got != "2026-01-01T00:00:00Z" {
		t.Errorf("n1.Metadata[kb_created] = %v", got)
	}

	// Edge n1->n2 is intra-graph.
	out, err := store.Outgoing(ctx, n1Node.LineageID, memgraph.TraverseOpts{})
	if err != nil {
		t.Fatalf("Outgoing: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("n1 outgoing = %d, want 1", len(out))
	}
	e := out[0]
	if e.Kind != "cites" {
		t.Errorf("edge.Kind = %q, want cites", e.Kind)
	}
	if e.GraphID != e.ToGraph {
		t.Errorf("intra-graph edge mis-routed: GraphID=%s ToGraph=%s", e.GraphID, e.ToGraph)
	}
	if e.CreatedBy != kbmigrate.CreatedBy {
		t.Errorf("edge.CreatedBy = %q", e.CreatedBy)
	}
}

func TestMigrate_CrossWorkspaceLinkBecomesSymlink(t *testing.T) {
	dir := t.TempDir()
	kbPath := filepath.Join(dir, "kb.db")

	makeKBDB(t, kbPath, []kbRow{
		{
			id: "a", title: "A", body: "in work",
			workspace: nullable("work"),
			links:     nullable(`["b"]`),
			created:   nullable("2026-01-01T00:00:00Z"),
		},
		{
			id: "b", title: "B", body: "in personal",
			workspace: nullable("personal"),
			created:   nullable("2026-01-02T00:00:00Z"),
		},
	})

	store, _ := openTarget(t)
	ctx := context.Background()

	report, err := kbmigrate.Migrate(ctx, kbPath, store, kbmigrate.Options{})
	if err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	if report.EdgesCreated != 1 {
		t.Fatalf("EdgesCreated = %d, want 1", report.EdgesCreated)
	}

	// Find the lineage of "a" so we can fetch its outgoing edge.
	var workID, personalID memgraph.GraphID
	for _, g := range report.Graphs {
		switch g.Name {
		case "work":
			workID = g.ID
		case "personal":
			personalID = g.ID
		}
	}
	if workID == "" || personalID == "" {
		t.Fatalf("missing graphs: work=%q personal=%q", workID, personalID)
	}
	workNodes, err := store.ListNodes(ctx, workID, memgraph.NodeFilter{})
	if err != nil {
		t.Fatalf("ListNodes: %v", err)
	}
	if len(workNodes) != 1 {
		t.Fatalf("work nodes = %d, want 1", len(workNodes))
	}
	out, err := store.Outgoing(ctx, workNodes[0].LineageID, memgraph.TraverseOpts{})
	if err != nil {
		t.Fatalf("Outgoing: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("outgoing = %d, want 1", len(out))
	}
	e := out[0]
	if e.GraphID == e.ToGraph {
		t.Errorf("expected symlink: GraphID=%s ToGraph=%s (should differ)", e.GraphID, e.ToGraph)
	}
	if e.GraphID != workID {
		t.Errorf("edge.GraphID = %s, want %s (work)", e.GraphID, workID)
	}
	if e.ToGraph != personalID {
		t.Errorf("edge.ToGraph = %s, want %s (personal)", e.ToGraph, personalID)
	}
}

func TestMigrate_MissingLinkTargetIsReported(t *testing.T) {
	dir := t.TempDir()
	kbPath := filepath.Join(dir, "kb.db")

	makeKBDB(t, kbPath, []kbRow{
		{
			id: "a", title: "A", body: "alone",
			workspace: nullable("work"),
			links:     nullable(`["does-not-exist","also-missing"]`),
			created:   nullable("2026-01-01T00:00:00Z"),
		},
	})

	store, _ := openTarget(t)
	ctx := context.Background()
	report, err := kbmigrate.Migrate(ctx, kbPath, store, kbmigrate.Options{})
	if err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	if report.EdgesCreated != 0 {
		t.Errorf("EdgesCreated = %d, want 0", report.EdgesCreated)
	}
	if len(report.SkippedLinks) != 2 {
		t.Fatalf("SkippedLinks = %d, want 2 (%+v)", len(report.SkippedLinks), report.SkippedLinks)
	}
	got := map[string]bool{}
	for _, sl := range report.SkippedLinks {
		got[sl.FromKBNoteID+"->"+sl.ToKBNoteID] = true
	}
	for _, want := range []string{"a->does-not-exist", "a->also-missing"} {
		if !got[want] {
			t.Errorf("missing skipped link entry: %s", want)
		}
	}
}

func TestMigrate_DryRunDoesNotWrite(t *testing.T) {
	dir := t.TempDir()
	kbPath := filepath.Join(dir, "kb.db")

	makeKBDB(t, kbPath, []kbRow{
		{
			id: "a", title: "A", body: "x",
			workspace: nullable("work"),
			links:     nullable(`["b","missing"]`),
			created:   nullable("2026-01-01T00:00:00Z"),
		},
		{
			id: "b", title: "B", body: "y",
			workspace: nullable("personal"),
			created:   nullable("2026-01-02T00:00:00Z"),
		},
	})

	store, _ := openTarget(t)
	ctx := context.Background()

	report, err := kbmigrate.Migrate(ctx, kbPath, store, kbmigrate.Options{DryRun: true})
	if err != nil {
		t.Fatalf("Migrate dry-run: %v", err)
	}
	if !report.DryRun {
		t.Errorf("report.DryRun = false, want true")
	}
	if len(report.Graphs) != 2 {
		t.Errorf("dry-run Graphs = %d, want 2", len(report.Graphs))
	}
	if report.EdgesCreated != 1 {
		// 1 valid link (a->b), 1 missing
		t.Errorf("dry-run EdgesCreated = %d, want 1", report.EdgesCreated)
	}
	if len(report.SkippedLinks) != 1 {
		t.Errorf("dry-run SkippedLinks = %d, want 1", len(report.SkippedLinks))
	}

	// Target store must be untouched.
	graphs, err := store.ListGraphs(ctx)
	if err != nil {
		t.Fatalf("ListGraphs: %v", err)
	}
	if len(graphs) != 0 {
		t.Errorf("dry-run leaked graphs into store: %d", len(graphs))
	}
}

func TestMigrate_SecondRunIsNoop(t *testing.T) {
	dir := t.TempDir()
	kbPath := filepath.Join(dir, "kb.db")

	makeKBDB(t, kbPath, []kbRow{
		{
			id: "n1", title: "A", body: "a-body",
			workspace: nullable("work"),
			links:     nullable(`["n2"]`),
			created:   nullable("2026-01-01T00:00:00Z"),
		},
		{
			id: "n2", title: "B", body: "b-body",
			workspace: nullable("work"),
			created:   nullable("2026-01-02T00:00:00Z"),
		},
		{
			id: "n3", title: "C", body: "c-body",
			workspace: nullable("personal"),
			created:   nullable("2026-01-03T00:00:00Z"),
		},
	})

	store, _ := openTarget(t)
	ctx := context.Background()

	r1, err := kbmigrate.Migrate(ctx, kbPath, store, kbmigrate.Options{})
	if err != nil {
		t.Fatalf("first Migrate: %v", err)
	}
	if r1.NodesCreated != 3 || r1.NodesSkipped != 0 {
		t.Fatalf("first run: NodesCreated=%d NodesSkipped=%d, want 3/0", r1.NodesCreated, r1.NodesSkipped)
	}
	if r1.EdgesCreated != 1 || r1.EdgesSkipped != 0 {
		t.Fatalf("first run: EdgesCreated=%d EdgesSkipped=%d, want 1/0", r1.EdgesCreated, r1.EdgesSkipped)
	}
	for _, g := range r1.Graphs {
		if g.Reused {
			t.Errorf("first run: graph %q marked reused", g.Name)
		}
	}

	// Snapshot post-first-run state.
	graphs1, err := store.ListGraphs(ctx)
	if err != nil {
		t.Fatalf("ListGraphs after r1: %v", err)
	}
	nodeIDsByGraph := map[memgraph.GraphID][]memgraph.NodeID{}
	for _, g := range graphs1 {
		ns, err := store.ListNodes(ctx, g.ID, memgraph.NodeFilter{})
		if err != nil {
			t.Fatalf("ListNodes: %v", err)
		}
		for _, n := range ns {
			nodeIDsByGraph[g.ID] = append(nodeIDsByGraph[g.ID], n.ID)
		}
	}

	// Second run: same source, same target.
	r2, err := kbmigrate.Migrate(ctx, kbPath, store, kbmigrate.Options{})
	if err != nil {
		t.Fatalf("second Migrate: %v", err)
	}
	if r2.NodesCreated != 0 {
		t.Errorf("second run NodesCreated = %d, want 0", r2.NodesCreated)
	}
	if r2.NodesSkipped != 3 {
		t.Errorf("second run NodesSkipped = %d, want 3", r2.NodesSkipped)
	}
	if r2.EdgesCreated != 0 {
		t.Errorf("second run EdgesCreated = %d, want 0", r2.EdgesCreated)
	}
	if r2.EdgesSkipped != 1 {
		t.Errorf("second run EdgesSkipped = %d, want 1", r2.EdgesSkipped)
	}
	for _, g := range r2.Graphs {
		if !g.Reused {
			t.Errorf("second run: graph %q not marked reused", g.Name)
		}
	}

	// Same number of graphs in the store (no duplicates).
	graphs2, err := store.ListGraphs(ctx)
	if err != nil {
		t.Fatalf("ListGraphs after r2: %v", err)
	}
	if len(graphs2) != len(graphs1) {
		t.Fatalf("graph count drifted: r1=%d r2=%d", len(graphs1), len(graphs2))
	}

	// Identical node IDs per graph — nothing was rewritten.
	for _, g := range graphs2 {
		ns, err := store.ListNodes(ctx, g.ID, memgraph.NodeFilter{})
		if err != nil {
			t.Fatalf("ListNodes: %v", err)
		}
		if len(ns) != len(nodeIDsByGraph[g.ID]) {
			t.Errorf("graph %q node count changed: was %d, now %d",
				g.Name, len(nodeIDsByGraph[g.ID]), len(ns))
		}
		gotIDs := map[memgraph.NodeID]bool{}
		for _, n := range ns {
			gotIDs[n.ID] = true
			if n.Version != 1 {
				t.Errorf("graph %q node %s: version=%d, want 1 (no new versions on re-migrate)",
					g.Name, n.ID, n.Version)
			}
		}
		for _, id := range nodeIDsByGraph[g.ID] {
			if !gotIDs[id] {
				t.Errorf("graph %q lost node id %s after re-migrate", g.Name, id)
			}
		}
	}
}

func TestMigrate_AddedNoteOnRemigrate(t *testing.T) {
	dir := t.TempDir()
	kbPath := filepath.Join(dir, "kb.db")

	// Initial DB: just n1.
	makeKBDB(t, kbPath, []kbRow{
		{
			id: "n1", title: "A", body: "a",
			workspace: nullable("work"),
			created:   nullable("2026-01-01T00:00:00Z"),
		},
	})

	store, _ := openTarget(t)
	ctx := context.Background()

	r1, err := kbmigrate.Migrate(ctx, kbPath, store, kbmigrate.Options{})
	if err != nil {
		t.Fatalf("first Migrate: %v", err)
	}
	if r1.NodesCreated != 1 {
		t.Fatalf("first run NodesCreated = %d, want 1", r1.NodesCreated)
	}

	// Capture n1's node ID so we can prove it isn't rewritten.
	graphs, _ := store.ListGraphs(ctx)
	if len(graphs) != 1 {
		t.Fatalf("graphs after r1 = %d, want 1", len(graphs))
	}
	workID := graphs[0].ID
	ns, _ := store.ListNodes(ctx, workID, memgraph.NodeFilter{})
	if len(ns) != 1 {
		t.Fatalf("nodes after r1 = %d, want 1", len(ns))
	}
	n1ID := ns[0].ID
	n1Lineage := ns[0].LineageID

	// Add n2 to the kb source and re-migrate. We need a fresh kb DB
	// file with both rows, since the test fixture builder doesn't
	// support inserts after construction.
	kbPath2 := filepath.Join(dir, "kb2.db")
	makeKBDB(t, kbPath2, []kbRow{
		{
			id: "n1", title: "A", body: "a",
			workspace: nullable("work"),
			created:   nullable("2026-01-01T00:00:00Z"),
		},
		{
			id: "n2", title: "B", body: "b",
			workspace: nullable("work"),
			links:     nullable(`["n1"]`),
			created:   nullable("2026-01-02T00:00:00Z"),
		},
	})

	r2, err := kbmigrate.Migrate(ctx, kbPath2, store, kbmigrate.Options{})
	if err != nil {
		t.Fatalf("second Migrate: %v", err)
	}
	if r2.NodesCreated != 1 || r2.NodesSkipped != 1 {
		t.Errorf("second run: NodesCreated=%d NodesSkipped=%d, want 1/1",
			r2.NodesCreated, r2.NodesSkipped)
	}
	if r2.EdgesCreated != 1 || r2.EdgesSkipped != 0 {
		t.Errorf("second run: EdgesCreated=%d EdgesSkipped=%d, want 1/0",
			r2.EdgesCreated, r2.EdgesSkipped)
	}

	// Only one graph still.
	graphs, _ = store.ListGraphs(ctx)
	if len(graphs) != 1 {
		t.Fatalf("graphs after r2 = %d, want 1", len(graphs))
	}
	if graphs[0].ID != workID {
		t.Fatalf("graph ID changed: %s -> %s", workID, graphs[0].ID)
	}

	// n1 is untouched (same NodeID, version 1).
	got, err := store.GetNodeByLineage(ctx, n1Lineage, memgraph.ReadOpts{})
	if err != nil {
		t.Fatalf("GetNodeByLineage(n1): %v", err)
	}
	if got.ID != n1ID {
		t.Errorf("n1 NodeID changed: was %s, now %s", n1ID, got.ID)
	}
	if got.Version != 1 {
		t.Errorf("n1 Version = %d, want 1", got.Version)
	}
}

func TestMigrate_StableLineageIDsAreDeterministic(t *testing.T) {
	dir := t.TempDir()
	kbPath := filepath.Join(dir, "kb.db")

	makeKBDB(t, kbPath, []kbRow{
		{
			id: "n1", title: "A", body: "a",
			workspace: nullable("work"),
			created:   nullable("2026-01-01T00:00:00Z"),
		},
		{
			id: "n2", title: "B", body: "b",
			workspace: nullable("work"),
			created:   nullable("2026-01-02T00:00:00Z"),
		},
	})

	// Migrate into target #1.
	store1, _ := openTarget(t)
	ctx := context.Background()
	if _, err := kbmigrate.Migrate(ctx, kbPath, store1, kbmigrate.Options{}); err != nil {
		t.Fatalf("Migrate s1: %v", err)
	}
	g1s, _ := store1.ListGraphs(ctx)
	lineage1 := map[string]memgraph.LineageID{}
	for _, g := range g1s {
		ns, _ := store1.ListNodes(ctx, g.ID, memgraph.NodeFilter{})
		for _, n := range ns {
			lineage1[n.Summary] = n.LineageID
		}
	}

	// Migrate the same source into a fresh target #2.
	store2, _ := openTarget(t)
	if _, err := kbmigrate.Migrate(ctx, kbPath, store2, kbmigrate.Options{}); err != nil {
		t.Fatalf("Migrate s2: %v", err)
	}
	g2s, _ := store2.ListGraphs(ctx)
	lineage2 := map[string]memgraph.LineageID{}
	for _, g := range g2s {
		ns, _ := store2.ListNodes(ctx, g.ID, memgraph.NodeFilter{})
		for _, n := range ns {
			lineage2[n.Summary] = n.LineageID
		}
	}

	if len(lineage1) != 2 || len(lineage2) != 2 {
		t.Fatalf("expected 2 nodes per store; got %d / %d", len(lineage1), len(lineage2))
	}
	for k, v := range lineage1 {
		if lineage2[k] != v {
			t.Errorf("lineage drift for %q: store1=%s store2=%s", k, v, lineage2[k])
		}
	}
}

func TestMigrate_TagsParsing(t *testing.T) {
	dir := t.TempDir()
	kbPath := filepath.Join(dir, "kb.db")

	makeKBDB(t, kbPath, []kbRow{
		{id: "valid", title: "V", body: "b", tags: nullable(`["a","b"]`),
			workspace: nullable("ws"), created: nullable("2026-01-01T00:00:00Z")},
		{id: "empty", title: "E", body: "b", tags: nullable(""),
			workspace: nullable("ws"), created: nullable("2026-01-02T00:00:00Z")},
		{id: "nullt", title: "N", body: "b", tags: null(),
			workspace: nullable("ws"), created: nullable("2026-01-03T00:00:00Z")},
	})

	store, _ := openTarget(t)
	ctx := context.Background()

	report, err := kbmigrate.Migrate(ctx, kbPath, store, kbmigrate.Options{})
	if err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	if len(report.Graphs) != 1 || report.Graphs[0].NodeCount != 3 {
		t.Fatalf("unexpected report: %+v", report.Graphs)
	}

	nodes, err := store.ListNodes(ctx, report.Graphs[0].ID, memgraph.NodeFilter{})
	if err != nil {
		t.Fatalf("ListNodes: %v", err)
	}
	for _, n := range nodes {
		switch n.Summary {
		case "V":
			if fmt.Sprint(n.Tags) != fmt.Sprint([]string{"a", "b"}) {
				t.Errorf("V tags = %v, want [a b]", n.Tags)
			}
		case "E", "N":
			if len(n.Tags) != 0 {
				t.Errorf("%s tags = %v, want empty", n.Summary, n.Tags)
			}
		}
	}
}
