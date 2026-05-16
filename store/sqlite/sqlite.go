// Package sqlite provides the embedded SQLite reference implementation of
// memgraph.Store.
//
// Default driver: modernc.org/sqlite (pure Go, cgo-free) to preserve
// single-binary cross-compilation. A cgo build using mattn/go-sqlite3 will
// be exposed behind a build tag once perf becomes load-bearing.
package sqlite

import (
	"context"

	memgraph "github.com/camggould/memgraph"
)

// Store is the SQLite-backed memgraph.Store implementation.
//
// TODO(v1): schema, FTS5 indexing, lineage-current materialized view,
// symlink manifest maintenance, write subscriptions.
type Store struct {
	path string
}

// Open opens (or initializes) a memgraph SQLite store at the given file path.
func Open(path string) (*Store, error) {
	return &Store{path: path}, memgraph.ErrNotImplemented
}

// Compile-time interface assertion.
var _ memgraph.Store = (*Store)(nil)

func (s *Store) CreateGraph(ctx context.Context, in memgraph.GraphInput) (memgraph.Graph, error) {
	return memgraph.Graph{}, memgraph.ErrNotImplemented
}

func (s *Store) GetGraph(ctx context.Context, id memgraph.GraphID) (memgraph.Graph, error) {
	return memgraph.Graph{}, memgraph.ErrNotImplemented
}

func (s *Store) ListGraphs(ctx context.Context) ([]memgraph.Graph, error) {
	return nil, memgraph.ErrNotImplemented
}

func (s *Store) UpdateGraphConfig(ctx context.Context, id memgraph.GraphID, patch memgraph.GraphConfigPatch) (memgraph.Graph, error) {
	return memgraph.Graph{}, memgraph.ErrNotImplemented
}

func (s *Store) PutNode(ctx context.Context, in memgraph.NodeInput) (memgraph.Node, error) {
	return memgraph.Node{}, memgraph.ErrNotImplemented
}

func (s *Store) GetNodeByLineage(ctx context.Context, id memgraph.LineageID, opts memgraph.ReadOpts) (memgraph.Node, error) {
	return memgraph.Node{}, memgraph.ErrNotImplemented
}

func (s *Store) GetNodeByID(ctx context.Context, id memgraph.NodeID) (memgraph.Node, error) {
	return memgraph.Node{}, memgraph.ErrNotImplemented
}

func (s *Store) History(ctx context.Context, id memgraph.LineageID) ([]memgraph.Node, error) {
	return nil, memgraph.ErrNotImplemented
}

func (s *Store) ListNodes(ctx context.Context, graphID memgraph.GraphID, f memgraph.NodeFilter) ([]memgraph.Node, error) {
	return nil, memgraph.ErrNotImplemented
}

func (s *Store) PutEdge(ctx context.Context, in memgraph.EdgeInput) (memgraph.Edge, error) {
	return memgraph.Edge{}, memgraph.ErrNotImplemented
}

func (s *Store) DeleteEdge(ctx context.Context, id memgraph.EdgeID) error {
	return memgraph.ErrNotImplemented
}

func (s *Store) Outgoing(ctx context.Context, from memgraph.LineageID, opts memgraph.TraverseOpts) ([]memgraph.Edge, error) {
	return nil, memgraph.ErrNotImplemented
}

func (s *Store) Incoming(ctx context.Context, to memgraph.LineageID, opts memgraph.TraverseOpts) ([]memgraph.Edge, error) {
	return nil, memgraph.ErrNotImplemented
}

func (s *Store) Traverse(ctx context.Context, from memgraph.LineageID, opts memgraph.TraverseOpts) (memgraph.TraversalResult, error) {
	return memgraph.TraversalResult{}, memgraph.ErrNotImplemented
}

func (s *Store) Search(ctx context.Context, graphID memgraph.GraphID, q memgraph.SearchQuery) ([]memgraph.SearchHit, error) {
	return nil, memgraph.ErrNotImplemented
}

func (s *Store) SymlinkManifest(ctx context.Context, graphID memgraph.GraphID) (memgraph.SymlinkManifest, error) {
	return memgraph.SymlinkManifest{}, memgraph.ErrNotImplemented
}

func (s *Store) Subscribe(h memgraph.WriteHandler) (memgraph.Unsubscribe, error) {
	return nil, memgraph.ErrNotImplemented
}

func (s *Store) Close() error { return nil }
