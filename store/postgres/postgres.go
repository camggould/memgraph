// Package postgres provides the Postgres reference implementation of
// memgraph.Store, using pgx and pg_trgm for full-text search.
//
// Suitable for cloud and multi-process / multi-writer deployments.
//
// Tests require a reachable Postgres instance. Set MEMGRAPH_POSTGRES_DSN
// to a DSN with permissions to create/drop schemas; otherwise tests skip.
//
// Subscribe is in-process only; cross-process notifications via
// LISTEN/NOTIFY are a future enhancement.
package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	memgraph "github.com/camggould/memgraph"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Store is the Postgres-backed memgraph.Store implementation.
type Store struct {
	pool    *pgxpool.Pool
	subs    *subscribers
	useTrgm bool
	// writeMu serializes writes within this process. Cross-process safety
	// is provided by SELECT ... FOR UPDATE in PutNode.
	writeMu sync.Mutex
}

// Open dials Postgres with the given DSN and applies the schema.
func Open(dsn string) (*Store, error) {
	return OpenContext(context.Background(), dsn)
}

// OpenContext is like Open but takes a context for the initial connection
// and schema application.
func OpenContext(ctx context.Context, dsn string) (*Store, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("postgres open: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("postgres ping: %w", err)
	}
	useTrgm, err := applySchema(ctx, pool)
	if err != nil {
		pool.Close()
		return nil, fmt.Errorf("postgres schema: %w", err)
	}
	return &Store{pool: pool, subs: newSubscribers(), useTrgm: useTrgm}, nil
}

// Compile-time interface assertion.
var _ memgraph.Store = (*Store)(nil)

func (s *Store) Close() error {
	s.pool.Close()
	return nil
}

// --- helpers ---

// jsonbBytes turns a Go value into JSONB-compatible bytes for pgx, returning
// nil to mean SQL NULL. Empty slices/maps collapse to NULL to keep storage
// tidy and to match the SQLite store's behavior.
func jsonbBytes(v any) ([]byte, error) {
	if v == nil {
		return nil, nil
	}
	switch x := v.(type) {
	case []string:
		if len(x) == 0 {
			return nil, nil
		}
	case map[string]any:
		if len(x) == 0 {
			return nil, nil
		}
	}
	return json.Marshal(v)
}

func unmarshalStringSlice(b []byte) ([]string, error) {
	if len(b) == 0 {
		return nil, nil
	}
	var out []string
	if err := json.Unmarshal(b, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func unmarshalJSONMap(b []byte) (map[string]any, error) {
	if len(b) == 0 {
		return nil, nil
	}
	var out map[string]any
	if err := json.Unmarshal(b, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// nullableString returns nil for empty strings (mapped to SQL NULL by pgx
// when passed as *string), or a pointer to the value.
func nullableString(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func tsNullable(t *time.Time) *time.Time {
	if t == nil {
		return nil
	}
	u := t.UTC()
	return &u
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

	kw, err := jsonbBytes(in.KindWhitelist)
	if err != nil {
		return memgraph.Graph{}, err
	}
	md, err := jsonbBytes(in.Metadata)
	if err != nil {
		return memgraph.Graph{}, err
	}

	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	if _, err := s.pool.Exec(ctx,
		`INSERT INTO graphs(id, name, conflict_policy, kind_whitelist, metadata, created_at)
		 VALUES ($1, $2, $3, $4, $5, $6)`,
		string(id), in.Name, string(policy), kw, md, now); err != nil {
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
	return s.getGraph(ctx, s.pool, id)
}

// rowScanner is the minimal Scan interface for both pgx.Row and pgx.Rows.
type rowScanner interface{ Scan(...any) error }

// querier abstracts pgxpool.Pool and pgx.Tx for shared helpers. Both expose
// the same Query/QueryRow signatures.
type querier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

func (s *Store) getGraph(ctx context.Context, q querier, id memgraph.GraphID) (memgraph.Graph, error) {
	row := q.QueryRow(ctx,
		`SELECT id, name, conflict_policy, kind_whitelist, metadata, created_at
		   FROM graphs WHERE id = $1`, string(id))
	return scanGraph(row)
}

func scanGraph(rs rowScanner) (memgraph.Graph, error) {
	var (
		g                  memgraph.Graph
		idStr, name, policy string
		createdAt          time.Time
		kw, md             []byte
	)
	if err := rs.Scan(&idStr, &name, &policy, &kw, &md, &createdAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
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
	g = memgraph.Graph{
		ID:             memgraph.GraphID(idStr),
		Name:           name,
		ConflictPolicy: memgraph.ConflictPolicy(policy),
		KindWhitelist:  kwSlice,
		Metadata:       mdMap,
		CreatedAt:      createdAt.UTC(),
	}
	return g, nil
}

func (s *Store) ListGraphs(ctx context.Context) ([]memgraph.Graph, error) {
	rows, err := s.pool.Query(ctx,
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

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return memgraph.Graph{}, err
	}
	defer tx.Rollback(ctx)

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
	kw, err := jsonbBytes(g.KindWhitelist)
	if err != nil {
		return memgraph.Graph{}, err
	}
	md, err := jsonbBytes(g.Metadata)
	if err != nil {
		return memgraph.Graph{}, err
	}
	if _, err := tx.Exec(ctx,
		`UPDATE graphs SET name=$1, conflict_policy=$2, kind_whitelist=$3, metadata=$4 WHERE id=$5`,
		g.Name, string(g.ConflictPolicy), kw, md, string(id)); err != nil {
		return memgraph.Graph{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return memgraph.Graph{}, err
	}
	s.subs.notifyGraph(ctx, g)
	return g, nil
}

// --- Nodes ---

func (s *Store) PutNode(ctx context.Context, in memgraph.NodeInput) (memgraph.Node, error) {
	if in.GraphID == "" {
		return memgraph.Node{}, fmt.Errorf("%w: graph_id required", memgraph.ErrInvalidInput)
	}
	if in.Kind == "" {
		return memgraph.Node{}, fmt.Errorf("%w: kind required", memgraph.ErrInvalidInput)
	}

	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return memgraph.Node{}, err
	}
	defer tx.Rollback(ctx)

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

	type head struct {
		id      memgraph.NodeID
		version int
	}
	var heads []head
	if lineageID != "" {
		// SELECT FOR UPDATE locks the lineage's head rows so concurrent
		// putters on the same lineage serialize against each other.
		rows, err := tx.Query(ctx,
			`SELECT id, version FROM nodes
			  WHERE lineage_id = $1 AND superseded_by IS NULL
			  ORDER BY version DESC, id ASC
			  FOR UPDATE`, string(lineageID))
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

	version := 1
	if len(heads) > 0 {
		version = heads[0].version + 1
	}

	var toSupersede []memgraph.NodeID
	conflict := false
	if in.BasedOnVersion == nil {
		for _, h := range heads {
			toSupersede = append(toSupersede, h.id)
		}
	} else {
		matched := false
		for _, h := range heads {
			if h.version == *in.BasedOnVersion {
				matched = true
				break
			}
		}
		if matched {
			for _, h := range heads {
				toSupersede = append(toSupersede, h.id)
			}
		} else if len(heads) == 0 {
			// Seeding a new lineage with an unmet hint — treat as fresh write.
		} else {
			switch g.ConflictPolicy {
			case memgraph.ConflictPolicyManual:
				conflict = true
			default:
				for _, h := range heads {
					toSupersede = append(toSupersede, h.id)
				}
			}
		}
	}

	id := memgraph.NewNodeID()
	now := time.Now().UTC()

	tags, err := jsonbBytes(in.Tags)
	if err != nil {
		return memgraph.Node{}, err
	}
	md, err := jsonbBytes(in.Metadata)
	if err != nil {
		return memgraph.Node{}, err
	}

	if _, err := tx.Exec(ctx,
		`INSERT INTO nodes(id, graph_id, lineage_id, version, kind, content, summary,
		                   tags, metadata, freshness_at, created_at, created_by, superseded_by)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, NULL)`,
		string(id), string(in.GraphID), string(lineageID), version,
		in.Kind, in.Content, nullableString(in.Summary),
		tags, md, tsNullable(in.FreshnessAt),
		now, in.CreatedBy); err != nil {
		return memgraph.Node{}, err
	}

	for _, sid := range toSupersede {
		if _, err := tx.Exec(ctx,
			`UPDATE nodes SET superseded_by = $1 WHERE id = $2`,
			string(id), string(sid)); err != nil {
			return memgraph.Node{}, err
		}
	}

	if err := tx.Commit(ctx); err != nil {
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
	rows, err := q.Query(ctx,
		`SELECT id FROM nodes
		  WHERE lineage_id = $1 AND superseded_by IS NULL AND id != $2`,
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

const nodeColumns = `id, graph_id, lineage_id, version, kind, content, summary, tags, metadata, freshness_at, created_at, created_by, superseded_by`

const nodeColumnsN = `n.id, n.graph_id, n.lineage_id, n.version, n.kind, n.content, n.summary, n.tags, n.metadata, n.freshness_at, n.created_at, n.created_by, n.superseded_by`

func scanNode(rs rowScanner) (memgraph.Node, error) {
	var (
		n                                 memgraph.Node
		idStr, graphID, lineageID, kind, content, cb string
		version                           int
		summary, supersededBy             *string
		createdAt                         time.Time
		freshness                         *time.Time
		tags, md                          []byte
	)
	if err := rs.Scan(&idStr, &graphID, &lineageID, &version, &kind, &content,
		&summary, &tags, &md, &freshness, &createdAt, &cb, &supersededBy); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
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
	var sumStr string
	if summary != nil {
		sumStr = *summary
	}
	var fresh *time.Time
	if freshness != nil {
		u := freshness.UTC()
		fresh = &u
	}
	n = memgraph.Node{
		ID:          memgraph.NodeID(idStr),
		GraphID:     memgraph.GraphID(graphID),
		LineageID:   memgraph.LineageID(lineageID),
		Version:     version,
		Kind:        kind,
		Content:     content,
		Summary:     sumStr,
		Tags:        tagsSlice,
		Metadata:    mdMap,
		FreshnessAt: fresh,
		CreatedAt:   createdAt.UTC(),
		CreatedBy:   cb,
	}
	if supersededBy != nil {
		nid := memgraph.NodeID(*supersededBy)
		n.SupersededBy = &nid
	}
	return n, nil
}

func (s *Store) GetNodeByLineage(ctx context.Context, id memgraph.LineageID, opts memgraph.ReadOpts) (memgraph.Node, error) {
	switch {
	case opts.AtVersion != nil:
		row := s.pool.QueryRow(ctx,
			`SELECT `+nodeColumns+` FROM nodes WHERE lineage_id = $1 AND version = $2`,
			string(id), *opts.AtVersion)
		n, err := scanNode(row)
		if err != nil {
			return memgraph.Node{}, err
		}
		if err := s.populateConflicts(ctx, s.pool, &n); err != nil {
			return memgraph.Node{}, err
		}
		return n, nil
	case opts.AtTime != nil:
		row := s.pool.QueryRow(ctx,
			`SELECT `+nodeColumns+` FROM nodes
			   WHERE lineage_id = $1 AND created_at <= $2
			   ORDER BY version DESC LIMIT 1`,
			string(id), opts.AtTime.UTC())
		n, err := scanNode(row)
		if err != nil {
			return memgraph.Node{}, err
		}
		if err := s.populateConflicts(ctx, s.pool, &n); err != nil {
			return memgraph.Node{}, err
		}
		return n, nil
	default:
		row := s.pool.QueryRow(ctx,
			`SELECT `+nodeColumns+` FROM nodes
			   WHERE lineage_id = $1 AND superseded_by IS NULL
			   ORDER BY version DESC, id ASC LIMIT 1`,
			string(id))
		n, err := scanNode(row)
		if err != nil {
			return memgraph.Node{}, err
		}
		if err := s.populateConflicts(ctx, s.pool, &n); err != nil {
			return memgraph.Node{}, err
		}
		return n, nil
	}
}

func (s *Store) GetNodeByID(ctx context.Context, id memgraph.NodeID) (memgraph.Node, error) {
	row := s.pool.QueryRow(ctx,
		`SELECT `+nodeColumns+` FROM nodes WHERE id = $1`, string(id))
	n, err := scanNode(row)
	if err != nil {
		return memgraph.Node{}, err
	}
	if err := s.populateConflicts(ctx, s.pool, &n); err != nil {
		return memgraph.Node{}, err
	}
	return n, nil
}

func (s *Store) History(ctx context.Context, id memgraph.LineageID) ([]memgraph.Node, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT `+nodeColumns+` FROM nodes WHERE lineage_id = $1 ORDER BY version DESC`,
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
		if err := s.populateConflicts(ctx, s.pool, &out[i]); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func (s *Store) ListNodes(ctx context.Context, graphID memgraph.GraphID, f memgraph.NodeFilter) ([]memgraph.Node, error) {
	var args []any
	q := `SELECT ` + nodeColumns + ` FROM nodes WHERE graph_id = $1 AND superseded_by IS NULL`
	args = append(args, string(graphID))
	if len(f.Kinds) > 0 {
		q += ` AND kind IN (` + placeholders(len(f.Kinds), len(args)+1) + `)`
		for _, k := range f.Kinds {
			args = append(args, k)
		}
	}
	// Tag filtering: JSONB containment with an array of tags requires ALL
	// elements to be present — exact AND semantics, no substring leaks.
	if len(f.Tags) > 0 {
		tagsJSON, err := json.Marshal(f.Tags)
		if err != nil {
			return nil, err
		}
		q += ` AND tags @> $` + strconv.Itoa(len(args)+1) + `::jsonb`
		args = append(args, string(tagsJSON))
	}
	q += ` ORDER BY created_at DESC, id DESC`
	if f.Limit > 0 {
		q += ` LIMIT $` + strconv.Itoa(len(args)+1)
		args = append(args, f.Limit)
		if f.Offset > 0 {
			q += ` OFFSET $` + strconv.Itoa(len(args)+1)
			args = append(args, f.Offset)
		}
	}
	rows, err := s.pool.Query(ctx, q, args...)
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
		if err := s.populateConflicts(ctx, s.pool, &out[i]); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func placeholders(n, start int) string {
	if n <= 0 {
		return ""
	}
	var b strings.Builder
	for i := 0; i < n; i++ {
		if i > 0 {
			b.WriteString(",")
		}
		b.WriteString("$")
		b.WriteString(strconv.Itoa(start + i))
	}
	return b.String()
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

	if _, err := s.getGraph(ctx, s.pool, in.GraphID); err != nil {
		if errors.Is(err, memgraph.ErrNotFound) {
			return memgraph.Edge{}, fmt.Errorf("%w: graph %q", memgraph.ErrNotFound, in.GraphID)
		}
		return memgraph.Edge{}, err
	}

	id := memgraph.NewEdgeID()
	now := time.Now().UTC()
	md, err := jsonbBytes(in.Metadata)
	if err != nil {
		return memgraph.Edge{}, err
	}
	var ord *int
	if in.Ordinal != nil {
		v := *in.Ordinal
		ord = &v
	}
	if _, err := s.pool.Exec(ctx,
		`INSERT INTO edges(id, graph_id, from_lineage, to_graph, to_lineage,
		                   kind, metadata, ordinal, created_at, created_by)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
		string(id), string(in.GraphID), string(in.FromLineage),
		string(toGraph), string(in.ToLineage),
		in.Kind, md, ord, now, in.CreatedBy); err != nil {
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

	res, err := s.pool.Exec(ctx, `DELETE FROM edges WHERE id = $1`, string(id))
	if err != nil {
		return err
	}
	if res.RowsAffected() == 0 {
		return memgraph.ErrNotFound
	}
	return nil
}

const edgeColumns = `id, graph_id, from_lineage, to_graph, to_lineage, kind,
                     metadata, ordinal, created_at, created_by`

func scanEdge(rs rowScanner) (memgraph.Edge, error) {
	var (
		e                                          memgraph.Edge
		idStr, graphID, fromL, toGraph, toL, kind, cb string
		md                                         []byte
		ord                                        *int
		createdAt                                  time.Time
	)
	if err := rs.Scan(&idStr, &graphID, &fromL, &toGraph, &toL, &kind,
		&md, &ord, &createdAt, &cb); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return memgraph.Edge{}, memgraph.ErrNotFound
		}
		return memgraph.Edge{}, err
	}
	mdMap, err := unmarshalJSONMap(md)
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
		CreatedAt:   createdAt.UTC(),
		CreatedBy:   cb,
	}
	if ord != nil {
		v := *ord
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
	q := `SELECT ` + edgeColumns + ` FROM edges WHERE ` + column + ` = $1`
	args := []any{string(lineage)}
	if len(opts.EdgeKinds) > 0 {
		q += ` AND kind IN (` + placeholders(len(opts.EdgeKinds), len(args)+1) + `)`
		for _, k := range opts.EdgeKinds {
			args = append(args, k)
		}
	}
	// NULLs sort last in Postgres by default for ASC, matching SQLite's
	// "ordinal IS NULL, ordinal ASC" with explicit NULLS LAST.
	q += ` ORDER BY ordinal ASC NULLS LAST, created_at ASC`
	rows, err := s.pool.Query(ctx, q, args...)
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

	args := []any{q.Text, string(graphID)}
	var scoreExpr, whereMatch, snippetExpr string
	if s.useTrgm {
		// pg_trgm similarity in [0,1]; higher is better. We require a literal
		// substring match (ILIKE) so e.g. searching "v2a" does not also match
		// "v2b" via trigram similarity. The similarity score is used purely
		// for ranking among hits that already contain the substring.
		scoreExpr = `similarity(coalesce(n.content,'') || ' ' || coalesce(n.summary,''), $1)`
		whereMatch = `(n.content ILIKE '%' || $1 || '%' OR coalesce(n.summary,'') ILIKE '%' || $1 || '%')`
		snippetExpr = `n.content`
	} else {
		// tsvector fallback. ts_rank for score, ts_headline for snippet.
		scoreExpr = `ts_rank(n.search_tsv, plainto_tsquery('simple', $1))`
		whereMatch = `n.search_tsv @@ plainto_tsquery('simple', $1)`
		snippetExpr = `ts_headline('simple', coalesce(n.content,''), plainto_tsquery('simple', $1), 'StartSel=[,StopSel=],MaxFragments=1,MaxWords=12,MinWords=1')`
	}

	sqlQ := `SELECT ` + nodeColumnsN + `, ` + snippetExpr + ` AS snip, ` + scoreExpr + ` AS score
	          FROM nodes n
	         WHERE ` + whereMatch + `
	           AND n.graph_id = $2
	           AND n.superseded_by IS NULL`
	if len(q.Kinds) > 0 {
		sqlQ += ` AND n.kind IN (` + placeholders(len(q.Kinds), len(args)+1) + `)`
		for _, k := range q.Kinds {
			args = append(args, k)
		}
	}
	if len(q.Tags) > 0 {
		tagsJSON, err := json.Marshal(q.Tags)
		if err != nil {
			return nil, err
		}
		sqlQ += ` AND n.tags @> $` + strconv.Itoa(len(args)+1) + `::jsonb`
		args = append(args, string(tagsJSON))
	}
	if q.FreshOnly {
		sqlQ += ` AND (n.freshness_at IS NULL OR n.freshness_at >= $` + strconv.Itoa(len(args)+1) + `)`
		args = append(args, time.Now().UTC())
	}
	sqlQ += ` ORDER BY score DESC LIMIT $` + strconv.Itoa(len(args)+1)
	args = append(args, limit)

	rows, err := s.pool.Query(ctx, sqlQ, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var hits []memgraph.SearchHit
	for rows.Next() {
		var (
			idStr, graphIDStr, lineageID, kind, content, cb string
			version                                         int
			summary, supersededBy                           *string
			createdAt                                       time.Time
			freshness                                       *time.Time
			tags, md                                        []byte
			snip                                            string
			score                                           float64
		)
		if err := rows.Scan(&idStr, &graphIDStr, &lineageID, &version, &kind, &content,
			&summary, &tags, &md, &freshness, &createdAt, &cb, &supersededBy, &snip, &score); err != nil {
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
		var sumStr string
		if summary != nil {
			sumStr = *summary
		}
		var fresh *time.Time
		if freshness != nil {
			u := freshness.UTC()
			fresh = &u
		}
		n := memgraph.Node{
			ID:          memgraph.NodeID(idStr),
			GraphID:     memgraph.GraphID(graphIDStr),
			LineageID:   memgraph.LineageID(lineageID),
			Version:     version,
			Kind:        kind,
			Content:     content,
			Summary:     sumStr,
			Tags:        tagsSlice,
			Metadata:    mdMap,
			FreshnessAt: fresh,
			CreatedAt:   createdAt.UTC(),
			CreatedBy:   cb,
		}
		if supersededBy != nil {
			nid := memgraph.NodeID(*supersededBy)
			n.SupersededBy = &nid
		}
		// Truncate snippet for trgm path (content can be large).
		if s.useTrgm && len(snip) > 200 {
			snip = snip[:200] + "..."
		}
		hits = append(hits, memgraph.SearchHit{Node: n, Snippet: snip, Score: score})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for i := range hits {
		if err := s.populateConflicts(ctx, s.pool, &hits[i].Node); err != nil {
			return nil, err
		}
	}
	return hits, nil
}

// --- Symlink manifest ---

func (s *Store) SymlinkManifest(ctx context.Context, graphID memgraph.GraphID) (memgraph.SymlinkManifest, error) {
	var out memgraph.SymlinkManifest

	rows, err := s.pool.Query(ctx,
		`SELECT to_graph, COUNT(*) FROM edges
		  WHERE graph_id = $1 AND to_graph != graph_id
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

	rows, err = s.pool.Query(ctx,
		`SELECT graph_id, COUNT(*) FROM edges
		  WHERE to_graph = $1 AND graph_id != to_graph
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
