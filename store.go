package memgraph

import "context"

// Store is the contract every storage backend must satisfy. memgraph's
// reference implementations (SQLite, Postgres) live in subpackages of
// store/. Third parties may implement Store against any backend.
//
// All methods are expected to be safe for concurrent use.
type Store interface {
	// --- Graphs ---

	CreateGraph(ctx context.Context, in GraphInput) (Graph, error)
	GetGraph(ctx context.Context, id GraphID) (Graph, error)
	ListGraphs(ctx context.Context) ([]Graph, error)
	UpdateGraphConfig(ctx context.Context, id GraphID, patch GraphConfigPatch) (Graph, error)

	// --- Nodes (versioned) ---

	// PutNode writes a new version. If in.LineageID is empty, a new lineage
	// is started at version 1. Otherwise, the lineage gains a new version.
	PutNode(ctx context.Context, in NodeInput) (Node, error)

	// GetNodeByLineage returns the current version of a lineage by default.
	// ReadOpts may pin to a specific version or point-in-time.
	GetNodeByLineage(ctx context.Context, id LineageID, opts ReadOpts) (Node, error)

	// GetNodeByID returns a specific node version exactly as written.
	GetNodeByID(ctx context.Context, id NodeID) (Node, error)

	// History returns all versions of a lineage, newest first.
	History(ctx context.Context, id LineageID) ([]Node, error)

	// ListNodes is a low-cardinality enumeration; for ranked/scored queries
	// use Search.
	ListNodes(ctx context.Context, graphID GraphID, f NodeFilter) ([]Node, error)

	// --- Edges ---

	PutEdge(ctx context.Context, in EdgeInput) (Edge, error)
	DeleteEdge(ctx context.Context, id EdgeID) error
	Outgoing(ctx context.Context, from LineageID, opts TraverseOpts) ([]Edge, error)
	Incoming(ctx context.Context, to LineageID, opts TraverseOpts) ([]Edge, error)
	Traverse(ctx context.Context, from LineageID, opts TraverseOpts) (TraversalResult, error)

	// --- Indexes ---

	Search(ctx context.Context, graphID GraphID, q SearchQuery) ([]SearchHit, error)
	SymlinkManifest(ctx context.Context, graphID GraphID) (SymlinkManifest, error)

	// --- Hooks (for derived indexes; v1.1 vector index uses this) ---

	Subscribe(h WriteHandler) (Unsubscribe, error)

	// Close releases any resources held by the store.
	Close() error
}

// WriteHandler is notified when nodes or edges are written, or when graphs
// are created or have their configuration updated. Used by derived indexes
// (e.g. the v1.1 vector index) and by transport adapters that need to keep
// live state in sync with the store (e.g. the MCP server's resource list).
//
// Handlers fire AFTER the underlying write has been committed.
type WriteHandler interface {
	OnNodeWritten(ctx context.Context, n Node)
	OnEdgeWritten(ctx context.Context, e Edge)
	// OnGraphCreated fires after a graph is created OR has its configuration
	// updated. Implementations that only care about the existence of a graph
	// can treat both cases the same way; implementations that care about the
	// distinction can compare g.CreatedAt against time.Now().
	OnGraphCreated(ctx context.Context, g Graph)
}

// NoopWriteHandler embeds zero-value methods for WriteHandler so types that
// only care about a subset of write events can compose it instead of having
// to implement every method.
type NoopWriteHandler struct{}

func (NoopWriteHandler) OnNodeWritten(context.Context, Node)   {}
func (NoopWriteHandler) OnEdgeWritten(context.Context, Edge)   {}
func (NoopWriteHandler) OnGraphCreated(context.Context, Graph) {}

// Unsubscribe removes a previously registered WriteHandler.
type Unsubscribe func()
