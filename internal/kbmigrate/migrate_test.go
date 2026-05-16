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
