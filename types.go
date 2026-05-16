package memgraph

import "time"

// ConflictPolicy controls how concurrent writes to the same lineage are
// resolved within a graph.
type ConflictPolicy string

const (
	ConflictPolicyLWW    ConflictPolicy = "lww"
	ConflictPolicyManual ConflictPolicy = "manual"
)

// Graph is the unit of isolation. A deployment hosts N graphs.
type Graph struct {
	ID             GraphID
	Name           string
	ConflictPolicy ConflictPolicy
	KindWhitelist  []string
	Metadata       map[string]any
	CreatedAt      time.Time
}

// GraphInput is the create payload for a new Graph. Empty ConflictPolicy
// defaults to ConflictPolicyLWW.
type GraphInput struct {
	Name           string
	ConflictPolicy ConflictPolicy
	KindWhitelist  []string
	Metadata       map[string]any
}

// GraphConfigPatch updates mutable graph configuration. Nil pointers leave
// the field unchanged; explicit empty slices/maps clear the value.
type GraphConfigPatch struct {
	Name           *string
	ConflictPolicy *ConflictPolicy
	KindWhitelist  []string
	Metadata       map[string]any
}

// Node is an atomic, immutable, versioned unit of information. Nodes are
// minimal containers — clients define their own ontology via Kind, Tags,
// and Metadata.
type Node struct {
	ID           NodeID
	GraphID      GraphID
	LineageID    LineageID
	Version      int
	Kind         string
	Content      string
	Summary      string // optional; clients fall back to Content if empty
	Tags         []string
	Metadata     map[string]any
	FreshnessAt  *time.Time
	CreatedAt    time.Time
	CreatedBy    string // opaque provenance; memgraph never interprets
	SupersededBy *NodeID
	// Conflicts lists sibling versions of this node that are not superseded
	// by anyone. Empty in the normal case. Populated when a lineage has
	// multiple concurrent heads under ConflictPolicyManual; the head with
	// the highest version is returned with the other heads listed here.
	Conflicts []NodeID
}

// NodeInput is the put payload. Omit LineageID to create a new lineage;
// supply an existing LineageID to append a new version.
type NodeInput struct {
	GraphID     GraphID
	LineageID   LineageID
	Kind        string
	Content     string
	Summary     string
	Tags        []string
	Metadata    map[string]any
	FreshnessAt *time.Time
	CreatedBy   string
	// BasedOnVersion is an optional optimistic-concurrency hint. If nil,
	// the put follows last-writer-wins semantics: the new version supersedes
	// whatever the current head is. If non-nil and the current head version
	// matches *BasedOnVersion, the put is non-conflicting. If non-nil and
	// the current head is ahead of *BasedOnVersion, the put is a concurrent
	// write — behavior then depends on the graph's ConflictPolicy.
	BasedOnVersion *int
}

// Edge is a directed, typed relationship between two lineages. If ToGraph
// differs from GraphID, the edge is a symlink across graph boundaries.
type Edge struct {
	ID          EdgeID
	GraphID     GraphID
	FromLineage LineageID
	ToGraph     GraphID
	ToLineage   LineageID
	Kind        string
	Metadata    map[string]any
	Ordinal     *int
	CreatedAt   time.Time
	CreatedBy   string
}

// EdgeInput is the create payload for an edge. If ToGraph is empty it
// defaults to GraphID (intra-graph edge).
type EdgeInput struct {
	GraphID     GraphID
	FromLineage LineageID
	ToGraph     GraphID
	ToLineage   LineageID
	Kind        string
	Metadata    map[string]any
	Ordinal     *int
	CreatedBy   string
}

// ReadOpts controls version selection when reading by lineage.
type ReadOpts struct {
	AtTime    *time.Time
	AtVersion *int
}

// NodeFilter narrows a ListNodes query.
type NodeFilter struct {
	Kinds  []string
	Tags   []string
	Limit  int
	Offset int
}

// TraverseOpts controls edge walks. FollowSymlinks is opt-in — by default
// traversal stops at graph boundaries.
type TraverseOpts struct {
	MaxDepth       int
	EdgeKinds      []string
	FollowSymlinks bool
	MaxNodes       int
}

// TraversalResult is the materialized output of a traversal.
type TraversalResult struct {
	Nodes []Node
	Edges []Edge
}

// SearchQuery is the search request payload. Search is graph-scoped;
// cross-graph search is a client orchestration concern.
type SearchQuery struct {
	Text      string
	Kinds     []string
	Tags      []string
	FreshOnly bool
	Limit     int
}

// SearchHit is one ranked result of a search.
type SearchHit struct {
	Node    Node
	Snippet string
	Score   float64
}

// SymlinkManifest summarizes a graph's cross-graph references.
type SymlinkManifest struct {
	Outbound []GraphRef
	Inbound  []GraphRef
}

// GraphRef is a reference to a graph, with a denormalized edge count.
type GraphRef struct {
	GraphID   GraphID
	EdgeCount int
}
