// Package sqlite provides the embedded SQLite reference implementation of
// memgraph.Store.
//
// Default driver: modernc.org/sqlite (pure Go, cgo-free) to preserve
// single-binary cross-compilation.
package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	memgraph "github.com/camggould/memgraph"
	_ "modernc.org/sqlite"
)

const tsLayout = time.RFC3339Nano

// Store is the SQLite-backed memgraph.Store implementation.
type Store struct {
	db   *sql.DB
	subs *subscribers
	// writeMu serializes all writes. modernc.org/sqlite has a single
	// writer at the DB level anyway; this also gives us a clean fence
	// for "fire subscribers after commit" semantics.
	writeMu sync.Mutex
}

// Open opens (or creates) the SQLite database at path and applies the schema.
func Open(path string) (*Store, error) {
	// _pragma works on modernc.org/sqlite via DSN params; we also set
	// pragmas explicitly in applySchema so callers using ":memory:" or
	// shared cache DSNs still get them.
	dsn := fmt.Sprintf("file:%s?_pragma=foreign_keys(1)&_pragma=journal_mode(wal)", path)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("sqlite open: %w", err)
	}
	// Single connection for WAL on temp paths is fine; pool default is ok.
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("sqlite ping: %w", err)
	}
	if err := applySchema(db); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("sqlite schema: %w", err)
	}
	return &Store{db: db, subs: newSubscribers()}, nil
}

// Compile-time interface assertion.
var _ memgraph.Store = (*Store)(nil)

func (s *Store) Close() error { return s.db.Close() }

// --- helpers ---

func marshalJSON(v any) (sql.NullString, error) {
	if v == nil {
		return sql.NullString{}, nil
	}
	// Treat empty slice/map as NULL to keep storage tidy.
	switch x := v.(type) {
	case []string:
		if x == nil {
			return sql.NullString{}, nil
		}
	case map[string]any:
		if x == nil {
			return sql.NullString{}, nil
		}
	}
	b, err := json.Marshal(v)
	if err != nil {
		return sql.NullString{}, err
	}
	return sql.NullString{String: string(b), Valid: true}, nil
}

func unmarshalStringSlice(ns sql.NullString) ([]string, error) {
	if !ns.Valid || ns.String == "" {
		return nil, nil
	}
	var out []string
	if err := json.Unmarshal([]byte(ns.String), &out); err != nil {
		return nil, err
	}
	return out, nil
}

func unmarshalJSONMap(ns sql.NullString) (map[string]any, error) {
	if !ns.Valid || ns.String == "" {
		return nil, nil
	}
	var out map[string]any
	if err := json.Unmarshal([]byte(ns.String), &out); err != nil {
		return nil, err
	}
	return out, nil
}

func tsValue(t time.Time) string { return t.UTC().Format(tsLayout) }

func tsNullable(t *time.Time) sql.NullString {
	if t == nil {
		return sql.NullString{}
	}
	return sql.NullString{String: t.UTC().Format(tsLayout), Valid: true}
}

func tsParse(s string) (time.Time, error) {
	return time.Parse(tsLayout, s)
}

func tsParseNullable(ns sql.NullString) (*time.Time, error) {
	if !ns.Valid || ns.String == "" {
		return nil, nil
	}
	t, err := tsParse(ns.String)
	if err != nil {
		return nil, err
	}
	return &t, nil
}

// --- Graphs ---

func (s *Store) CreateGraph(ctx context.Context, in memgraph.GraphInput) (memgraph.Graph, error) {
	if in.Name == "" {
		return memgraph.Graph{}, fmt.Errorf("%w: graph name is required", memgraph.ErrInvalidInput)
	}
	policy := in.ConflictPolicy
	if policy == "" {
		policy = memgraph.ConflictPolicyLWW
	}
	id := memgraph.NewGraphID()
	now := time.Now().UTC()

	kw, err := marshalJSON(in.KindWhitelist)
	if err != nil {
		return memgraph.Graph{}, err
	}
	md, err := marshalJSON(in.Metadata)
	if err != nil {
		return memgraph.Graph{}, err
	}

	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	if _, err := s.db.ExecContext(ctx,
		`INSERT INTO graphs(id, name, conflict_policy, kind_whitelist, metadata, created_at)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		string(id), in.Name, string(policy), kw, md, tsValue(now)); err != nil {
		return memgraph.Graph{}, err
	}
	g := memgraph.Graph{
		ID:             id,
		Name:           in.Name,
		ConflictPolicy: policy,
		KindWhitelist:  in.KindWhitelist,
		Metadata:       in.Metadata,
		CreatedAt:      now,
	}
	s.subs.notifyGraph(ctx, g)
	return g, nil
}

func (s *Store) GetGraph(ctx context.Context, id memgraph.GraphID) (memgraph.Graph, error) {
	return s.getGraph(ctx, s.db, id)
}

func (s *Store) getGraph(ctx context.Context, q querier, id memgraph.GraphID) (memgraph.Graph, error) {
	row := q.QueryRowContext(ctx,
		`SELECT id, name, conflict_policy, kind_whitelist, metadata, created_at
		   FROM graphs WHERE id = ?`, string(id))
	return scanGraph(row)
}

type rowScanner interface{ Scan(...any) error }

func scanGraph(rs rowScanner) (memgraph.Graph, error) {
	var (
		g                              memgraph.Graph
		idStr, name, policy, createdAt string
		kw, md                         sql.NullString
	)
	if err := rs.Scan(&idStr, &name, &policy, &kw, &md, &createdAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return memgraph.Graph{}, memgraph.ErrNotFound
		}
		return memgraph.Graph{}, err
	}
	kwSlice, err := unmarshalStringSlice(kw)
	if err != nil {
		return memgraph.Graph{}, err
	}
	mdMap, err := unmarshalJSONMap(md)
	if err != nil {
		return memgraph.Graph{}, err
	}
	t, err := tsParse(createdAt)
	if err != nil {
		return memgraph.Graph{}, err
	}
	g = memgraph.Graph{
		ID:             memgraph.GraphID(idStr),
		Name:           name,
		ConflictPolicy: memgraph.ConflictPolicy(policy),
		KindWhitelist:  kwSlice,
		Metadata:       mdMap,
		CreatedAt:      t,
	}
	return g, nil
}

func (s *Store) ListGraphs(ctx context.Context) ([]memgraph.Graph, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, name, conflict_policy, kind_whitelist, metadata, created_at
		   FROM graphs ORDER BY created_at ASC, id ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []memgraph.Graph
	for rows.Next() {
		g, err := scanGraph(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, g)
	}
	return out, rows.Err()
}

func (s *Store) UpdateGraphConfig(ctx context.Context, id memgraph.GraphID, patch memgraph.GraphConfigPatch) (memgraph.Graph, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return memgraph.Graph{}, err
	}
	defer tx.Rollback()

	g, err := s.getGraph(ctx, tx, id)
	if err != nil {
		return memgraph.Graph{}, err
	}
	if patch.Name != nil {
		g.Name = *patch.Name
	}
	if patch.ConflictPolicy != nil {
		g.ConflictPolicy = *patch.ConflictPolicy
	}
	if patch.KindWhitelist != nil {
		// Caller passed an explicit slice; len==0 clears.
		if len(patch.KindWhitelist) == 0 {
			g.KindWhitelist = nil
		} else {
			g.KindWhitelist = patch.KindWhitelist
		}
	}
	if patch.Metadata != nil {
		if len(patch.Metadata) == 0 {
			g.Metadata = nil
		} else {
			g.Metadata = patch.Metadata
		}
	}
	kw, err := marshalJSON(g.KindWhitelist)
	if err != nil {
		return memgraph.Graph{}, err
	}
	md, err := marshalJSON(g.Metadata)
	if err != nil {
		return memgraph.Graph{}, err
	}
	if _, err := tx.ExecContext(ctx,
		`UPDATE graphs SET name=?, conflict_policy=?, kind_whitelist=?, metadata=? WHERE id=?`,
		g.Name, string(g.ConflictPolicy), kw, md, string(id)); err != nil {
		return memgraph.Graph{}, err
	}
	if err := tx.Commit(); err != nil {
		return memgraph.Graph{}, err
	}
	s.subs.notifyGraph(ctx, g)
	return g, nil
}

// --- Nodes ---

type querier interface {
	QueryRowContext(ctx context.Context, q string, args ...any) *sql.Row
	QueryContext(ctx context.Context, q string, args ...any) (*sql.Rows, error)
	ExecContext(ctx context.Context, q string, args ...any) (sql.Result, error)
}

func (s *Store) PutNode(ctx context.Context, in memgraph.NodeInput) (memgraph.Node, error) {
	if in.GraphID == "" {
		return memgraph.Node{}, fmt.Errorf("%w: graph_id required", memgraph.ErrInvalidInput)
	}
	if in.Kind == "" {
		return memgraph.Node{}, fmt.Errorf("%w: kind required", memgraph.ErrInvalidInput)
	}

	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return memgraph.Node{}, err
	}
	defer tx.Rollback()

	g, err := s.getGraph(ctx, tx, in.GraphID)
	if err != nil {
		if errors.Is(err, memgraph.ErrNotFound) {
			return memgraph.Node{}, fmt.Errorf("%w: graph %q", memgraph.ErrNotFound, in.GraphID)
		}
		return memgraph.Node{}, err
	}
	if len(g.KindWhitelist) > 0 {
		ok := false
		for _, k := range g.KindWhitelist {
			if k == in.Kind {
				ok = true
				break
			}
		}
		if !ok {
			return memgraph.Node{}, fmt.Errorf("%w: %q not in whitelist for graph %q",
				memgraph.ErrKindNotAllowed, in.Kind, in.GraphID)
		}
	}

	lineageID := in.LineageID

	// Load all non-superseded heads on this lineage, newest version first.
	// This drives both version numbering and conflict detection.
	type head struct {
		id      memgraph.NodeID
		version int
	}
	var heads []head
	if lineageID != "" {
		rows, err := tx.QueryContext(ctx,
			`SELECT id, version FROM nodes
			  WHERE lineage_id = ? AND superseded_by IS NULL
			  ORDER BY version DESC, id ASC`, string(lineageID))
		if err != nil {
			return memgraph.Node{}, err
		}
		for rows.Next() {
			var idStr string
			var v int
			if err := rows.Scan(&idStr, &v); err != nil {
				rows.Close()
				return memgraph.Node{}, err
			}
			heads = append(heads, head{id: memgraph.NodeID(idStr), version: v})
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return memgraph.Node{}, err
		}
	} else {
		lineageID = memgraph.NewLineageID()
	}

	// Compute the new version number.
	version := 1
	if len(heads) > 0 {
		version = heads[0].version + 1
	}

	// Decide which existing heads (if any) to supersede.
	//
	//   - BasedOnVersion == nil  : LWW — supersede all current heads.
	//   - BasedOnVersion matches the single highest head (and there's only
	//     one head) : non-concurrent; supersede that head.
	//   - BasedOnVersion matches some head and there are multiple heads
	//     (i.e. the writer is the resolver) : supersede ALL heads.
	//   - BasedOnVersion mismatches under manual : supersede none (sibling
	//     write, conflict recorded).
	//   - BasedOnVersion mismatches under lww : supersede all heads (LWW
	//     ignores the hint when it loses; the prior heads still lose).
	var toSupersede []memgraph.NodeID
	conflict := false
	if in.BasedOnVersion == nil {
		for _, h := range heads {
			toSupersede = append(toSupersede, h.id)
		}
	} else {
		// Find the head matching BasedOnVersion, if any.
		matched := false
		for _, h := range heads {
			if h.version == *in.BasedOnVersion {
				matched = true
				break
			}
		}
		if matched {
			// Resolver path (or trivial single-head non-conflict).
			// Supersede every current head so the new version becomes the
			// unambiguous head.
			for _, h := range heads {
				toSupersede = append(toSupersede, h.id)
			}
		} else if len(heads) == 0 {
			// Seeding a new lineage with an unmet hint — treat as fresh write.
		} else {
			// Concurrent write.
			switch g.ConflictPolicy {
			case memgraph.ConflictPolicyManual:
				conflict = true
				// Do not supersede any head; the new version is a sibling.
			default: // ConflictPolicyLWW or unspecified
				for _, h := range heads {
					toSupersede = append(toSupersede, h.id)
				}
			}
		}
	}

	id := memgraph.NewNodeID()
	now := time.Now().UTC()

	tags, err := marshalJSON(in.Tags)
	if err != nil {
		return memgraph.Node{}, err
	}
	md, err := marshalJSON(in.Metadata)
	if err != nil {
		return memgraph.Node{}, err
	}

	if _, err := tx.ExecContext(ctx,
		`INSERT INTO nodes(id, graph_id, lineage_id, version, kind, content, summary,
		                   tags, metadata, freshness_at, created_at, created_by, superseded_by)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)`,
		string(id), string(in.GraphID), string(lineageID), version,
		in.Kind, in.Content, nullableString(in.Summary),
		tags, md, tsNullable(in.FreshnessAt),
		tsValue(now), in.CreatedBy); err != nil {
		return memgraph.Node{}, err
	}

	// Supersede the resolved set: mark superseded_by and drop their FTS
	// rows. Heads we leave alone keep their FTS row (so under manual both
	// siblings remain searchable).
	for _, sid := range toSupersede {
		if _, err := tx.ExecContext(ctx,
			`UPDATE nodes SET superseded_by = ? WHERE id = ?`,
			string(id), string(sid)); err != nil {
			return memgraph.Node{}, err
		}
		if _, err := tx.ExecContext(ctx,
			`DELETE FROM nodes_fts WHERE node_id = ?`, string(sid)); err != nil {
			return memgraph.Node{}, err
		}
	}

	// Always insert FTS for the new version: it is a non-superseded head
	// regardless of whether it stands alone or sits alongside a sibling.
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO nodes_fts(content, summary, tags, node_id) VALUES (?, ?, ?, ?)`,
		in.Content, in.Summary, ftsTagsString(in.Tags), string(id)); err != nil {
		return memgraph.Node{}, err
	}

	if err := tx.Commit(); err != nil {
		return memgraph.Node{}, err
	}

	node := memgraph.Node{
		ID:          id,
		GraphID:     in.GraphID,
		LineageID:   lineageID,
		Version:     version,
		Kind:        in.Kind,
		Content:     in.Content,
		Summary:     in.Summary,
		Tags:        in.Tags,
		Metadata:    in.Metadata,
		FreshnessAt: in.FreshnessAt,
		CreatedAt:   now,
		CreatedBy:   in.CreatedBy,
	}
	if conflict {
		// Heads that survived (we didn't supersede them) are siblings of
		// the just-written node. Surface their IDs on the returned Node.
		for _, h := range heads {
			node.Conflicts = append(node.Conflicts, h.id)
		}
	}
	s.subs.notifyNode(ctx, node)
	if conflict {
		return node, memgraph.ErrConflictManual
	}
	return node, nil
}

// populateConflicts fills Node.Conflicts if n is a current head and other
// non-superseded heads exist on the same lineage.
func (s *Store) populateConflicts(ctx context.Context, q querier, n *memgraph.Node) error {
	if n.SupersededBy != nil {
		return nil
	}
	rows, err := q.QueryContext(ctx,
		`SELECT id FROM nodes
		  WHERE lineage_id = ? AND superseded_by IS NULL AND id != ?`,
		string(n.LineageID), string(n.ID))
	if err != nil {
		return err
	}
	defer rows.Close()
	var siblings []memgraph.NodeID
	for rows.Next() {
		var idStr string
		if err := rows.Scan(&idStr); err != nil {
			return err
		}
		siblings = append(siblings, memgraph.NodeID(idStr))
	}
	if err := rows.Err(); err != nil {
		return err
	}
	n.Conflicts = siblings
	return nil
}

func nullableString(s string) sql.NullString {
	if s == "" {
		return sql.NullString{}
	}
	return sql.NullString{String: s, Valid: true}
}

func ftsTagsString(tags []string) string {
	// FTS5 tokenizer breaks on spaces; join tags with spaces so each is
	// independently searchable.
	return strings.Join(tags, " ")
}

const nodeColumns = `id, graph_id, lineage_id, version, kind, content, summary, tags, metadata, freshness_at, created_at, created_by, superseded_by`

const nodeColumnsN = `n.id, n.graph_id, n.lineage_id, n.version, n.kind, n.content, n.summary, n.tags, n.metadata, n.freshness_at, n.created_at, n.created_by, n.superseded_by`

func scanNode(rs rowScanner) (memgraph.Node, error) {
	var (
		n                                                memgraph.Node
		idStr, graphID, lineageID, kind, content, cb, ca string
		version                                          int
		summary, tags, md, freshness, supersededBy       sql.NullString
	)
	if err := rs.Scan(&idStr, &graphID, &lineageID, &version, &kind, &content,
		&summary, &tags, &md, &freshness, &ca, &cb, &supersededBy); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return memgraph.Node{}, memgraph.ErrNotFound
		}
		return memgraph.Node{}, err
	}
	tagsSlice, err := unmarshalStringSlice(tags)
	if err != nil {
		return memgraph.Node{}, err
	}
	mdMap, err := unmarshalJSONMap(md)
	if err != nil {
		return memgraph.Node{}, err
	}
	fresh, err := tsParseNullable(freshness)
	if err != nil {
		return memgraph.Node{}, err
	}
	createdAt, err := tsParse(ca)
	if err != nil {
		return memgraph.Node{}, err
	}
	n = memgraph.Node{
		ID:          memgraph.NodeID(idStr),
		GraphID:     memgraph.GraphID(graphID),
		LineageID:   memgraph.LineageID(lineageID),
		Version:     version,
		Kind:        kind,
		Content:     content,
		Summary:     summary.String,
		Tags:        tagsSlice,
		Metadata:    mdMap,
		FreshnessAt: fresh,
		CreatedAt:   createdAt,
		CreatedBy:   cb,
	}
	if supersededBy.Valid {
		nid := memgraph.NodeID(supersededBy.String)
		n.SupersededBy = &nid
	}
	return n, nil
}

func (s *Store) GetNodeByLineage(ctx context.Context, id memgraph.LineageID, opts memgraph.ReadOpts) (memgraph.Node, error) {
	switch {
	case opts.AtVersion != nil:
		row := s.db.QueryRowContext(ctx,
			`SELECT `+nodeColumns+` FROM nodes WHERE lineage_id = ? AND version = ?`,
			string(id), *opts.AtVersion)
		n, err := scanNode(row)
		if err != nil {
			return memgraph.Node{}, err
		}
		if err := s.populateConflicts(ctx, s.db, &n); err != nil {
			return memgraph.Node{}, err
		}
		return n, nil
	case opts.AtTime != nil:
		row := s.db.QueryRowContext(ctx,
			`SELECT `+nodeColumns+` FROM nodes
			   WHERE lineage_id = ? AND created_at <= ?
			   ORDER BY version DESC LIMIT 1`,
			string(id), tsValue(*opts.AtTime))
		n, err := scanNode(row)
		if err != nil {
			return memgraph.Node{}, err
		}
		if err := s.populateConflicts(ctx, s.db, &n); err != nil {
			return memgraph.Node{}, err
		}
		return n, nil
	default:
		// Pick the head with the highest version; tiebreak on id ascending
		// for determinism when two concurrent siblings happen to share a
		// version number (which they shouldn't given our +1 scheme, but
		// the ORDER BY is cheap insurance).
		row := s.db.QueryRowContext(ctx,
			`SELECT `+nodeColumns+` FROM nodes
			   WHERE lineage_id = ? AND superseded_by IS NULL
			   ORDER BY version DESC, id ASC LIMIT 1`,
			string(id))
		n, err := scanNode(row)
		if err != nil {
			return memgraph.Node{}, err
		}
		if err := s.populateConflicts(ctx, s.db, &n); err != nil {
			return memgraph.Node{}, err
		}
		return n, nil
	}
}

func (s *Store) GetNodeByID(ctx context.Context, id memgraph.NodeID) (memgraph.Node, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT `+nodeColumns+` FROM nodes WHERE id = ?`, string(id))
	n, err := scanNode(row)
	if err != nil {
		return memgraph.Node{}, err
	}
	if err := s.populateConflicts(ctx, s.db, &n); err != nil {
		return memgraph.Node{}, err
	}
	return n, nil
}

func (s *Store) History(ctx context.Context, id memgraph.LineageID) ([]memgraph.Node, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT `+nodeColumns+` FROM nodes WHERE lineage_id = ? ORDER BY version DESC`,
		string(id))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []memgraph.Node
	for rows.Next() {
		n, err := scanNode(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for i := range out {
		if err := s.populateConflicts(ctx, s.db, &out[i]); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func (s *Store) ListNodes(ctx context.Context, graphID memgraph.GraphID, f memgraph.NodeFilter) ([]memgraph.Node, error) {
	var args []any
	q := `SELECT ` + nodeColumns + ` FROM nodes WHERE graph_id = ? AND superseded_by IS NULL`
	args = append(args, string(graphID))
	if len(f.Kinds) > 0 {
		q += ` AND kind IN (` + placeholders(len(f.Kinds)) + `)`
		for _, k := range f.Kinds {
			args = append(args, k)
		}
	}
	// Tag filtering: nodes.tags is a JSON array; require an exact element
	// match per requested tag via json_each so substrings can't match
	// (e.g. "bar" must not match a row tagged "barbecue").
	for _, t := range f.Tags {
		q += ` AND EXISTS (SELECT 1 FROM json_each(nodes.tags) WHERE value = ?)`
		args = append(args, t)
	}
	q += ` ORDER BY created_at DESC, id DESC`
	if f.Limit > 0 {
		q += ` LIMIT ?`
		args = append(args, f.Limit)
		if f.Offset > 0 {
			q += ` OFFSET ?`
			args = append(args, f.Offset)
		}
	}
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []memgraph.Node
	for rows.Next() {
		n, err := scanNode(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for i := range out {
		if err := s.populateConflicts(ctx, s.db, &out[i]); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func placeholders(n int) string {
	if n <= 0 {
		return ""
	}
	return strings.Repeat("?,", n-1) + "?"
}

// --- Edges ---

func (s *Store) PutEdge(ctx context.Context, in memgraph.EdgeInput) (memgraph.Edge, error) {
	if in.GraphID == "" || in.FromLineage == "" || in.ToLineage == "" || in.Kind == "" {
		return memgraph.Edge{}, fmt.Errorf("%w: graph_id, from_lineage, to_lineage, kind required",
			memgraph.ErrInvalidInput)
	}
	toGraph := in.ToGraph
	if toGraph == "" {
		toGraph = in.GraphID
	}

	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	if _, err := s.getGraph(ctx, s.db, in.GraphID); err != nil {
		if errors.Is(err, memgraph.ErrNotFound) {
			return memgraph.Edge{}, fmt.Errorf("%w: graph %q", memgraph.ErrNotFound, in.GraphID)
		}
		return memgraph.Edge{}, err
	}

	id := memgraph.NewEdgeID()
	now := time.Now().UTC()
	md, err := marshalJSON(in.Metadata)
	if err != nil {
		return memgraph.Edge{}, err
	}
	var ord sql.NullInt64
	if in.Ordinal != nil {
		ord = sql.NullInt64{Int64: int64(*in.Ordinal), Valid: true}
	}
	if _, err := s.db.ExecContext(ctx,
		`INSERT INTO edges(id, graph_id, from_lineage, to_graph, to_lineage,
		                   kind, metadata, ordinal, created_at, created_by)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		string(id), string(in.GraphID), string(in.FromLineage),
		string(toGraph), string(in.ToLineage),
		in.Kind, md, ord, tsValue(now), in.CreatedBy); err != nil {
		return memgraph.Edge{}, err
	}
	e := memgraph.Edge{
		ID:          id,
		GraphID:     in.GraphID,
		FromLineage: in.FromLineage,
		ToGraph:     toGraph,
		ToLineage:   in.ToLineage,
		Kind:        in.Kind,
		Metadata:    in.Metadata,
		Ordinal:     in.Ordinal,
		CreatedAt:   now,
		CreatedBy:   in.CreatedBy,
	}
	s.subs.notifyEdge(ctx, e)
	return e, nil
}

func (s *Store) DeleteEdge(ctx context.Context, id memgraph.EdgeID) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	res, err := s.db.ExecContext(ctx, `DELETE FROM edges WHERE id = ?`, string(id))
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return memgraph.ErrNotFound
	}
	return nil
}

const edgeColumns = `id, graph_id, from_lineage, to_graph, to_lineage, kind,
                     metadata, ordinal, created_at, created_by`

func scanEdge(rs rowScanner) (memgraph.Edge, error) {
	var (
		e                                                memgraph.Edge
		idStr, graphID, fromL, toGraph, toL, kind, cb, ca string
		md                                               sql.NullString
		ord                                              sql.NullInt64
	)
	if err := rs.Scan(&idStr, &graphID, &fromL, &toGraph, &toL, &kind,
		&md, &ord, &ca, &cb); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return memgraph.Edge{}, memgraph.ErrNotFound
		}
		return memgraph.Edge{}, err
	}
	mdMap, err := unmarshalJSONMap(md)
	if err != nil {
		return memgraph.Edge{}, err
	}
	createdAt, err := tsParse(ca)
	if err != nil {
		return memgraph.Edge{}, err
	}
	e = memgraph.Edge{
		ID:          memgraph.EdgeID(idStr),
		GraphID:     memgraph.GraphID(graphID),
		FromLineage: memgraph.LineageID(fromL),
		ToGraph:     memgraph.GraphID(toGraph),
		ToLineage:   memgraph.LineageID(toL),
		Kind:        kind,
		Metadata:    mdMap,
		CreatedAt:   createdAt,
		CreatedBy:   cb,
	}
	if ord.Valid {
		v := int(ord.Int64)
		e.Ordinal = &v
	}
	return e, nil
}

func (s *Store) Outgoing(ctx context.Context, from memgraph.LineageID, opts memgraph.TraverseOpts) ([]memgraph.Edge, error) {
	return s.edgesBy(ctx, "from_lineage", from, opts)
}

func (s *Store) Incoming(ctx context.Context, to memgraph.LineageID, opts memgraph.TraverseOpts) ([]memgraph.Edge, error) {
	return s.edgesBy(ctx, "to_lineage", to, opts)
}

func (s *Store) edgesBy(ctx context.Context, column string, lineage memgraph.LineageID, opts memgraph.TraverseOpts) ([]memgraph.Edge, error) {
	q := `SELECT ` + edgeColumns + ` FROM edges WHERE ` + column + ` = ?`
	args := []any{string(lineage)}
	if len(opts.EdgeKinds) > 0 {
		q += ` AND kind IN (` + placeholders(len(opts.EdgeKinds)) + `)`
		for _, k := range opts.EdgeKinds {
			args = append(args, k)
		}
	}
	q += ` ORDER BY ordinal IS NULL, ordinal ASC, created_at ASC`
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []memgraph.Edge
	for rows.Next() {
		e, err := scanEdge(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

func (s *Store) Traverse(ctx context.Context, from memgraph.LineageID, opts memgraph.TraverseOpts) (memgraph.TraversalResult, error) {
	maxDepth := opts.MaxDepth
	if maxDepth <= 0 {
		maxDepth = 1
	}
	maxNodes := opts.MaxNodes
	if maxNodes <= 0 {
		maxNodes = 1024
	}
	dir := opts.Direction
	if dir == "" {
		dir = memgraph.TraverseOutgoing
	}

	visitedNodes := map[memgraph.LineageID]bool{from: true}
	seenEdges := map[memgraph.EdgeID]bool{}
	var nodes []memgraph.Node
	var edges []memgraph.Edge

	// Seed root node; missing root is fine — we still walk edges.
	if n, err := s.GetNodeByLineage(ctx, from, memgraph.ReadOpts{}); err == nil {
		nodes = append(nodes, n)
	} else if !errors.Is(err, memgraph.ErrNotFound) {
		return memgraph.TraversalResult{}, err
	}

	frontier := []memgraph.LineageID{from}
	for depth := 0; depth < maxDepth && len(frontier) > 0; depth++ {
		var next []memgraph.LineageID
		for _, cur := range frontier {
			var outEdges, inEdges []memgraph.Edge
			if dir != memgraph.TraverseIncoming {
				es, err := s.Outgoing(ctx, cur, opts)
				if err != nil {
					return memgraph.TraversalResult{}, err
				}
				outEdges = es
			}
			if dir != memgraph.TraverseOutgoing {
				es, err := s.Incoming(ctx, cur, opts)
				if err != nil {
					return memgraph.TraversalResult{}, err
				}
				inEdges = es
			}
			for _, e := range outEdges {
				if !opts.FollowSymlinks && e.ToGraph != e.GraphID {
					continue
				}
				if seenEdges[e.ID] {
					continue
				}
				seenEdges[e.ID] = true
				edges = append(edges, e)
				if visitedNodes[e.ToLineage] {
					continue
				}
				visitedNodes[e.ToLineage] = true
				if n, err := s.GetNodeByLineage(ctx, e.ToLineage, memgraph.ReadOpts{}); err == nil {
					nodes = append(nodes, n)
					if len(nodes) >= maxNodes {
						return memgraph.TraversalResult{Nodes: nodes, Edges: edges}, nil
					}
				} else if !errors.Is(err, memgraph.ErrNotFound) {
					return memgraph.TraversalResult{}, err
				}
				next = append(next, e.ToLineage)
			}
			for _, e := range inEdges {
				// Cross-graph hop: incoming edge originates in a different
				// graph than the current node. Mirror the outgoing check.
				if !opts.FollowSymlinks && e.GraphID != e.ToGraph {
					continue
				}
				if seenEdges[e.ID] {
					continue
				}
				seenEdges[e.ID] = true
				edges = append(edges, e)
				if visitedNodes[e.FromLineage] {
					continue
				}
				visitedNodes[e.FromLineage] = true
				if n, err := s.GetNodeByLineage(ctx, e.FromLineage, memgraph.ReadOpts{}); err == nil {
					nodes = append(nodes, n)
					if len(nodes) >= maxNodes {
						return memgraph.TraversalResult{Nodes: nodes, Edges: edges}, nil
					}
				} else if !errors.Is(err, memgraph.ErrNotFound) {
					return memgraph.TraversalResult{}, err
				}
				next = append(next, e.FromLineage)
			}
		}
		frontier = next
	}
	return memgraph.TraversalResult{Nodes: nodes, Edges: edges}, nil
}

// --- Search ---

func (s *Store) Search(ctx context.Context, graphID memgraph.GraphID, q memgraph.SearchQuery) ([]memgraph.SearchHit, error) {
	if q.Text == "" {
		return nil, nil
	}
	limit := q.Limit
	if limit <= 0 {
		limit = 20
	}

	// FTS join scoped to graph + current versions. Score uses bm25; smaller
	// is "better" so we negate to keep "higher = better" semantics in API.
	args := []any{q.Text, string(graphID)}
	sqlQ := `SELECT ` + nodeColumnsN + `,
	                snippet(nodes_fts, 0, '[', ']', '...', 12) AS snip,
	                bm25(nodes_fts) AS score
	          FROM nodes_fts
	          JOIN nodes n ON n.id = nodes_fts.node_id
	         WHERE nodes_fts MATCH ?
	           AND n.graph_id = ?
	           AND n.superseded_by IS NULL`
	if len(q.Kinds) > 0 {
		sqlQ += ` AND n.kind IN (` + placeholders(len(q.Kinds)) + `)`
		for _, k := range q.Kinds {
			args = append(args, k)
		}
	}
	for _, t := range q.Tags {
		sqlQ += ` AND EXISTS (SELECT 1 FROM json_each(n.tags) WHERE value = ?)`
		args = append(args, t)
	}
	if q.FreshOnly {
		sqlQ += ` AND (n.freshness_at IS NULL OR n.freshness_at >= ?)`
		args = append(args, tsValue(time.Now().UTC()))
	}
	sqlQ += ` ORDER BY score ASC LIMIT ?`
	args = append(args, limit)

	rows, err := s.db.QueryContext(ctx, sqlQ, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var hits []memgraph.SearchHit
	for rows.Next() {
		// Scan nodeColumns + snippet + score.
		var (
			idStr, graphIDStr, lineageID, kind, content, cb, ca string
			version                                             int
			summary, tags, md, freshness, supersededBy          sql.NullString
			snip                                                string
			score                                               float64
		)
		if err := rows.Scan(&idStr, &graphIDStr, &lineageID, &version, &kind, &content,
			&summary, &tags, &md, &freshness, &ca, &cb, &supersededBy, &snip, &score); err != nil {
			return nil, err
		}
		tagsSlice, err := unmarshalStringSlice(tags)
		if err != nil {
			return nil, err
		}
		mdMap, err := unmarshalJSONMap(md)
		if err != nil {
			return nil, err
		}
		fresh, err := tsParseNullable(freshness)
		if err != nil {
			return nil, err
		}
		createdAt, err := tsParse(ca)
		if err != nil {
			return nil, err
		}
		n := memgraph.Node{
			ID:          memgraph.NodeID(idStr),
			GraphID:     memgraph.GraphID(graphIDStr),
			LineageID:   memgraph.LineageID(lineageID),
			Version:     version,
			Kind:        kind,
			Content:     content,
			Summary:     summary.String,
			Tags:        tagsSlice,
			Metadata:    mdMap,
			FreshnessAt: fresh,
			CreatedAt:   createdAt,
			CreatedBy:   cb,
		}
		if supersededBy.Valid {
			nid := memgraph.NodeID(supersededBy.String)
			n.SupersededBy = &nid
		}
		hits = append(hits, memgraph.SearchHit{Node: n, Snippet: snip, Score: -score})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for i := range hits {
		if err := s.populateConflicts(ctx, s.db, &hits[i].Node); err != nil {
			return nil, err
		}
	}
	return hits, nil
}

// --- Symlink manifest ---

func (s *Store) SymlinkManifest(ctx context.Context, graphID memgraph.GraphID) (memgraph.SymlinkManifest, error) {
	var out memgraph.SymlinkManifest

	rows, err := s.db.QueryContext(ctx,
		`SELECT to_graph, COUNT(*) FROM edges
		  WHERE graph_id = ? AND to_graph != graph_id
		  GROUP BY to_graph
		  ORDER BY to_graph ASC`, string(graphID))
	if err != nil {
		return out, err
	}
	for rows.Next() {
		var g string
		var c int
		if err := rows.Scan(&g, &c); err != nil {
			rows.Close()
			return out, err
		}
		out.Outbound = append(out.Outbound, memgraph.GraphRef{
			GraphID: memgraph.GraphID(g), EdgeCount: c,
		})
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return out, err
	}

	rows, err = s.db.QueryContext(ctx,
		`SELECT graph_id, COUNT(*) FROM edges
		  WHERE to_graph = ? AND graph_id != to_graph
		  GROUP BY graph_id
		  ORDER BY graph_id ASC`, string(graphID))
	if err != nil {
		return out, err
	}
	defer rows.Close()
	for rows.Next() {
		var g string
		var c int
		if err := rows.Scan(&g, &c); err != nil {
			return out, err
		}
		out.Inbound = append(out.Inbound, memgraph.GraphRef{
			GraphID: memgraph.GraphID(g), EdgeCount: c,
		})
	}
	return out, rows.Err()
}

// --- Subscriptions ---

func (s *Store) Subscribe(h memgraph.WriteHandler) (memgraph.Unsubscribe, error) {
	if h == nil {
		return nil, fmt.Errorf("%w: nil handler", memgraph.ErrInvalidInput)
	}
	return s.subs.add(h), nil
}
