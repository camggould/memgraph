// Package kbmigrate imports a camggould/kb SQLite database into a
// memgraph deployment.
//
// kb's data model is a single SQLite `notes` table with a JSON
// `links` field referencing other notes by ID and a `workspace`
// column. memgraph partitions by workspace (one graph per distinct
// value, NULL/empty/whitespace → "default") and promotes `links` to
// real `cites` edges. Cross-workspace links become cross-graph
// symlinks.
//
// Migration is idempotent: re-running Migrate against the same kb
// source picks up any net-new content and leaves existing nodes /
// edges untouched. Two mechanisms make this work:
//
//   - Stable lineage IDs derived from sha256(workspace, kb_note_id)
//     so the same kb note always maps to the same memgraph lineage.
//   - Per-workspace graph identity stored in graph.metadata
//     (kb_managed=true, kb_workspace=<ws>) so re-runs locate the
//     existing graph instead of creating a new one.
//
// Content updates on existing notes are NOT detected here; that's a
// v1.2 concern. For v1.1 the rule is: existing lineage → skip.
package kbmigrate

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"path/filepath"
	"strings"

	memgraph "github.com/camggould/memgraph"
	_ "modernc.org/sqlite"
)

// CreatedBy is the opaque provenance string stamped on every node
// and edge written by the migrator.
const CreatedBy = "kb-migration"

// defaultWorkspace names the bucket for notes whose kb.workspace
// column is NULL, empty, or whitespace-only.
const defaultWorkspace = "default"

// Graph metadata keys used to identify a graph as kb-managed and to
// look it up on a subsequent migration run.
const (
	mdKeyKBManaged   = "kb_managed"
	mdKeyKBWorkspace = "kb_workspace"
)

// Options controls migration behavior.
type Options struct {
	// DryRun validates the source and reports what would be migrated
	// without writing anything to the target store.
	DryRun bool
}

// Report summarizes a migration run.
type Report struct {
	SourcePath   string
	TargetPath   string // populated by the caller, not Migrate itself
	Graphs       []GraphReport
	NodesCreated int
	NodesSkipped int
	EdgesCreated int
	EdgesSkipped int
	SkippedLinks []SkippedLink
	DryRun       bool
}

// GraphReport describes a single graph that was (or would be) created
// or reused during migration.
type GraphReport struct {
	Name          string
	ID            memgraph.GraphID
	NodeCount     int  // total nodes in the graph after the run
	NodesCreated  int  // nodes inserted by this run
	NodesSkipped  int  // notes whose lineage already existed
	Reused        bool // true if an existing kb-managed graph was matched
}

// SkippedLink records a kb link whose target note was not present in
// the source database.
type SkippedLink struct {
	FromKBNoteID string
	ToKBNoteID   string
}

// kbNote is the subset of the kb schema we care about.
type kbNote struct {
	ID        string
	Title     string
	Body      string
	TagsRaw   sql.NullString
	Source    sql.NullString
	Context   sql.NullString
	Workspace sql.NullString
	LinksRaw  sql.NullString
	Created   sql.NullString
	Modified  sql.NullString
	FilePath  sql.NullString
}

// nodeRef is the result of resolving a note to its memgraph identity,
// keyed by kb note ID in the lookup map.
type nodeRef struct {
	LineageID memgraph.LineageID
	GraphID   memgraph.GraphID
}

// stableLineageID derives a deterministic memgraph LineageID from the
// (workspace, kb_note_id) pair. It's a hex prefix of a sha256 — not a
// real ULID, but a valid LineageID (the type is just a string).
func stableLineageID(workspace, kbNoteID string) memgraph.LineageID {
	h := sha256.Sum256([]byte(workspace + "\x00" + kbNoteID))
	return memgraph.LineageID(hex.EncodeToString(h[:])[:26])
}

// Migrate reads a kb SQLite database at kbDBPath and writes its
// contents into store as one graph per distinct workspace.
// Re-running against the same source is a noop on already-migrated
// notes; net-new notes/links are added.
func Migrate(ctx context.Context, kbDBPath string, store memgraph.Store, opts Options) (Report, error) {
	abs, err := filepath.Abs(kbDBPath)
	if err != nil {
		abs = kbDBPath
	}
	report := Report{
		SourcePath: abs,
		DryRun:     opts.DryRun,
	}

	notes, err := readKBNotes(ctx, kbDBPath)
	if err != nil {
		return report, err
	}

	// Bucket notes by workspace, preserving discovery order for
	// deterministic graph creation order.
	var workspaceOrder []string
	byWorkspace := map[string][]kbNote{}
	for _, n := range notes {
		ws := normalizeWorkspace(n.Workspace)
		if _, ok := byWorkspace[ws]; !ok {
			workspaceOrder = append(workspaceOrder, ws)
		}
		byWorkspace[ws] = append(byWorkspace[ws], n)
	}

	// In non-dry-run mode, scan existing graphs once so we can match
	// each workspace to an existing kb-managed graph if present.
	var existingByWorkspace map[string]memgraph.Graph
	if !opts.DryRun {
		existingByWorkspace, err = findKBManagedGraphs(ctx, store)
		if err != nil {
			return report, fmt.Errorf("scan existing graphs: %w", err)
		}
	}

	// Pass 1: ensure graphs exist, insert nodes for net-new lineages.
	noteIndex := make(map[string]nodeRef, len(notes))
	graphs := make([]GraphReport, 0, len(workspaceOrder))

	for _, ws := range workspaceOrder {
		bucket := byWorkspace[ws]
		gr := GraphReport{Name: ws}

		if opts.DryRun {
			// Dry run: still validate tags parse cleanly so we surface
			// errors before pretending to succeed. We don't query the
			// target so we can't distinguish new vs. existing; report
			// every note as a would-create.
			for _, note := range bucket {
				if _, err := parseTags(note.TagsRaw); err != nil {
					return report, fmt.Errorf("note %s: parse tags: %w", note.ID, err)
				}
				gr.NodesCreated++
			}
			gr.NodeCount = len(bucket)
			graphs = append(graphs, gr)
			report.NodesCreated += gr.NodesCreated
			continue
		}

		// Locate-or-create the workspace's graph.
		var graphID memgraph.GraphID
		if existing, ok := existingByWorkspace[ws]; ok {
			graphID = existing.ID
			gr.Reused = true
		} else {
			g, err := store.CreateGraph(ctx, memgraph.GraphInput{
				Name: ws,
				Metadata: map[string]any{
					mdKeyKBManaged:   true,
					mdKeyKBWorkspace: ws,
				},
			})
			if err != nil {
				return report, fmt.Errorf("create graph %q: %w", ws, err)
			}
			graphID = g.ID
		}
		gr.ID = graphID

		for _, note := range bucket {
			lineageID := stableLineageID(ws, note.ID)

			// Skip if this lineage is already present.
			if existing, err := store.GetNodeByLineage(ctx, lineageID, memgraph.ReadOpts{}); err == nil {
				noteIndex[note.ID] = nodeRef{
					LineageID: existing.LineageID,
					GraphID:   existing.GraphID,
				}
				gr.NodesSkipped++
				report.NodesSkipped++
				continue
			} else if !errors.Is(err, memgraph.ErrNotFound) {
				return report, fmt.Errorf("note %s: lookup lineage: %w", note.ID, err)
			}

			tags, err := parseTags(note.TagsRaw)
			if err != nil {
				return report, fmt.Errorf("note %s: parse tags: %w", note.ID, err)
			}
			node, err := store.PutNode(ctx, memgraph.NodeInput{
				GraphID:   graphID,
				LineageID: lineageID,
				Kind:      "fact",
				Content:   note.Body,
				Summary:   note.Title,
				Tags:      tags,
				Metadata:  buildMetadata(note),
				CreatedBy: CreatedBy,
			})
			if err != nil {
				return report, fmt.Errorf("note %s: put node: %w", note.ID, err)
			}
			noteIndex[note.ID] = nodeRef{
				LineageID: node.LineageID,
				GraphID:   node.GraphID,
			}
			gr.NodesCreated++
			report.NodesCreated++
		}

		// NodeCount is the total node count in the graph after this
		// run (including any pre-existing nodes a previous migration
		// or other writer created).
		existing, err := store.ListNodes(ctx, graphID, memgraph.NodeFilter{})
		if err != nil {
			return report, fmt.Errorf("graph %q: list nodes: %w", ws, err)
		}
		gr.NodeCount = len(existing)
		graphs = append(graphs, gr)
	}
	report.Graphs = graphs

	// Pass 2: walk links and create cites edges.
	//
	// In dry-run mode we never inserted nodes, so noteIndex is empty;
	// fall back to a presence set built from the source rows. Dry-run
	// reports every valid link as "would-create" — it doesn't try to
	// detect which already exist in the target.
	presence := make(map[string]bool, len(notes))
	for _, n := range notes {
		presence[n.ID] = true
	}

	for _, note := range notes {
		linkIDs, err := parseLinks(note.LinksRaw)
		if err != nil {
			return report, fmt.Errorf("note %s: parse links: %w", note.ID, err)
		}
		if len(linkIDs) == 0 {
			continue
		}
		for _, toKBID := range linkIDs {
			if !presence[toKBID] {
				report.SkippedLinks = append(report.SkippedLinks, SkippedLink{
					FromKBNoteID: note.ID, ToKBNoteID: toKBID,
				})
				continue
			}
			if opts.DryRun {
				report.EdgesCreated++
				continue
			}
			from, ok := noteIndex[note.ID]
			if !ok {
				return report, fmt.Errorf("note %s: missing from index after pass 1", note.ID)
			}
			to, ok := noteIndex[toKBID]
			if !ok {
				return report, fmt.Errorf("note %s: missing from index after pass 1", toKBID)
			}

			// Skip if an equivalent edge already exists.
			exists, err := edgeExists(ctx, store, from.GraphID, from.LineageID, to.LineageID, "cites")
			if err != nil {
				return report, fmt.Errorf("edge %s->%s: lookup: %w", note.ID, toKBID, err)
			}
			if exists {
				report.EdgesSkipped++
				continue
			}

			if _, err := store.PutEdge(ctx, memgraph.EdgeInput{
				GraphID:     from.GraphID,
				FromLineage: from.LineageID,
				ToGraph:     to.GraphID,
				ToLineage:   to.LineageID,
				Kind:        "cites",
				CreatedBy:   CreatedBy,
			}); err != nil {
				return report, fmt.Errorf("edge %s->%s: %w", note.ID, toKBID, err)
			}
			report.EdgesCreated++
		}
	}

	return report, nil
}

// findKBManagedGraphs returns kb-managed graphs in store keyed by
// their kb workspace name. A graph is kb-managed iff its metadata
// has kb_managed == true. If two graphs claim the same workspace,
// the first one found wins (creation order); duplicates are ignored.
func findKBManagedGraphs(ctx context.Context, store memgraph.Store) (map[string]memgraph.Graph, error) {
	all, err := store.ListGraphs(ctx)
	if err != nil {
		return nil, err
	}
	out := map[string]memgraph.Graph{}
	for _, g := range all {
		if g.Metadata == nil {
			continue
		}
		managed, _ := g.Metadata[mdKeyKBManaged].(bool)
		if !managed {
			continue
		}
		ws, _ := g.Metadata[mdKeyKBWorkspace].(string)
		if ws == "" {
			continue
		}
		if _, exists := out[ws]; exists {
			continue
		}
		out[ws] = g
	}
	return out, nil
}

// edgeExists returns true iff store already has an outgoing edge of
// kind `kind` from `from` whose ToLineage matches `to`. Edge sets per
// lineage are small (links per note), so a linear scan is fine.
func edgeExists(ctx context.Context, store memgraph.Store, graphID memgraph.GraphID,
	from, to memgraph.LineageID, kind string) (bool, error) {
	edges, err := store.Outgoing(ctx, from, memgraph.TraverseOpts{EdgeKinds: []string{kind}})
	if err != nil {
		return false, err
	}
	for _, e := range edges {
		if e.GraphID == graphID && e.ToLineage == to && e.Kind == kind {
			return true, nil
		}
	}
	return false, nil
}

// normalizeWorkspace maps NULL/empty/whitespace workspace values to
// "default". Workspace names are otherwise case-sensitive and
// preserved exactly.
func normalizeWorkspace(ns sql.NullString) string {
	if !ns.Valid {
		return defaultWorkspace
	}
	if strings.TrimSpace(ns.String) == "" {
		return defaultWorkspace
	}
	return ns.String
}

// parseTags decodes kb's tags column (JSON-encoded array stored as a
// text blob). NULL and empty string yield nil with no error.
func parseTags(ns sql.NullString) ([]string, error) {
	if !ns.Valid {
		return nil, nil
	}
	s := strings.TrimSpace(ns.String)
	if s == "" {
		return nil, nil
	}
	var out []string
	if err := json.Unmarshal([]byte(s), &out); err != nil {
		return nil, err
	}
	return out, nil
}

// parseLinks decodes kb's links column (JSON-encoded array of note
// IDs). NULL/empty yield nil with no error.
func parseLinks(ns sql.NullString) ([]string, error) {
	if !ns.Valid {
		return nil, nil
	}
	s := strings.TrimSpace(ns.String)
	if s == "" {
		return nil, nil
	}
	var out []string
	if err := json.Unmarshal([]byte(s), &out); err != nil {
		return nil, err
	}
	return out, nil
}

// buildMetadata projects kb's auxiliary columns into memgraph
// metadata, skipping empty values.
func buildMetadata(n kbNote) map[string]any {
	md := map[string]any{}
	setIfPresent(md, "kb_source", n.Source)
	setIfPresent(md, "kb_context", n.Context)
	setIfPresent(md, "kb_workspace", n.Workspace)
	setIfPresent(md, "kb_file_path", n.FilePath)
	setIfPresent(md, "kb_created", n.Created)
	setIfPresent(md, "kb_modified", n.Modified)
	if len(md) == 0 {
		return nil
	}
	return md
}

func setIfPresent(md map[string]any, key string, ns sql.NullString) {
	if !ns.Valid {
		return
	}
	if ns.String == "" {
		return
	}
	md[key] = ns.String
}

// readKBNotes opens the kb SQLite DB read-only and reads every row of
// the notes table.
func readKBNotes(ctx context.Context, path string) ([]kbNote, error) {
	// modernc.org/sqlite accepts a file: URI with query params. mode=ro
	// is honored; we URL-escape the path to handle spaces and such.
	dsn := fmt.Sprintf("file:%s?mode=ro", url.PathEscape(path))
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open kb db: %w", err)
	}
	defer db.Close()
	if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("ping kb db: %w", err)
	}

	rows, err := db.QueryContext(ctx,
		`SELECT id, title, body, tags, source, context, workspace,
		        links, created, modified, file_path
		   FROM notes
		   ORDER BY created ASC, id ASC`)
	if err != nil {
		return nil, fmt.Errorf("query kb notes: %w", err)
	}
	defer rows.Close()

	var out []kbNote
	for rows.Next() {
		var n kbNote
		// title/body may technically be NULL in a malformed kb DB; we
		// scan into sql.NullString and coerce.
		var title, body sql.NullString
		if err := rows.Scan(&n.ID, &title, &body, &n.TagsRaw,
			&n.Source, &n.Context, &n.Workspace,
			&n.LinksRaw, &n.Created, &n.Modified, &n.FilePath); err != nil {
			return nil, fmt.Errorf("scan kb note: %w", err)
		}
		if title.Valid {
			n.Title = title.String
		}
		if body.Valid {
			n.Body = body.String
		}
		out = append(out, n)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}
