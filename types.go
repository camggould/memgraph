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
	// Compact requests a sparse projection: callers that only need to render
	// a graph view (canvas labels, colors, shape, filters) can skip the heavy
	// fields. When true, the Store returns Nodes with Content, Metadata,
	// FreshnessAt, CreatedBy, SupersededBy, and Conflicts cleared. The
	// remaining fields (ID, GraphID, LineageID, Version, Kind, Summary, Tags,
	// CreatedAt) plus the computed is_current / is_stale flags downstream
	// adapters expose are enough to draw the graph. Default false preserves
	// the v0.5 behavior.
	Compact bool
}

// TraverseDirection controls which way edges are followed during a traversal.
// "" defaults to outgoing for backward compatibility.
type TraverseDirection string

const (
	TraverseOutgoing TraverseDirection = "outgoing"
	TraverseIncoming TraverseDirection = "incoming"
	TraverseBoth     TraverseDirection = "both"
)

// TraverseOpts controls edge walks. FollowSymlinks is opt-in — by default
// traversal stops at graph boundaries.
type TraverseOpts struct {
	MaxDepth       int
	EdgeKinds      []string
	FollowSymlinks bool
	MaxNodes       int
	// Direction controls which edges are followed. Default ("" or
	// TraverseOutgoing) is outgoing-only — backward-compatible with v0.1.
	Direction TraverseDirection
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

// SchemaDescription summarizes the kind and tag vocabulary currently in
// use within a graph. It is intended as a one-shot snapshot an agent can
// fetch at session start to learn the user's existing conventions and
// avoid inventing parallel ones.
type SchemaDescription struct {
	GraphID     GraphID         `json:"graph_id"`
	NodeCount   int             `json:"node_count"`
	Kinds       []KindFreq      `json:"kinds"`
	TagPrefixes []TagPrefixFreq `json:"tag_prefixes"`
	Tags        []TagFreq       `json:"tags"`
}

// KindFreq is a kind label with the number of current-version nodes using
// it and up to three example summaries to hint at the kind's semantic
// role.
type KindFreq struct {
	Kind     string   `json:"kind"`
	Count    int      `json:"count"`
	Examples []string `json:"examples,omitempty"` // up to 3 short summaries
}

// TagPrefixFreq groups together tags sharing a "prefix:value" namespace.
// Count is the total number of node-tag uses with this prefix; Values is
// up to ten distinct trailing values seen.
type TagPrefixFreq struct {
	Prefix string   `json:"prefix"` // e.g. "protein"
	Count  int      `json:"count"`  // total node-tag uses with this prefix
	Values []string `json:"values"` // up to 10 distinct values
}

// TagFreq is a single tag and its node-use count.
type TagFreq struct {
	Tag   string `json:"tag"`
	Count int    `json:"count"`
}

// Batch-search tuning constants. Exported so adapter packages (the MCP server,
// REST server, and downstream tenant-scoped wrappers like memorysvc) can share
// a single source of truth for limits and the Reciprocal Rank Fusion k value.
const (
	// SearchBatchRRFK is the standard RRF k constant; 60 is the canonical
	// value from Cormack et al. (2009).
	SearchBatchRRFK = 60.0
	// SearchBatchMaxQueries caps the number of variant queries an agent may
	// submit in one batch.
	SearchBatchMaxQueries = 8
	// SearchBatchDefaultPerQueryLimit is the default per-variant Limit when
	// the caller does not specify one.
	SearchBatchDefaultPerQueryLimit = 20
	// SearchBatchMaxPerQueryLimit caps each variant's Limit regardless of
	// what the caller asks for.
	SearchBatchMaxPerQueryLimit = 50
	// SearchBatchDefaultTotalLimit is the default total-result cap applied
	// after merging across variants.
	SearchBatchDefaultTotalLimit = 20
	// SearchBatchMaxTotalLimit caps the total returned hits regardless of
	// what the caller asks for.
	SearchBatchMaxTotalLimit = 100
)

// SearchBatchHit is one element of an RRF-fused, deduped batch-search result.
// Score is the summed Reciprocal Rank Fusion score across the queries that
// surfaced this lineage; QueriesMatched lists the input-query indexes that
// contributed.
type SearchBatchHit struct {
	Node           Node
	RRFScore       float64
	QueriesMatched []int
}

// SearchBatchResult is the full output of a batch-search fan-out.
type SearchBatchResult struct {
	// Hits is the merged, deduped, top-limit slice ranked by RRFScore.
	Hits []SearchBatchHit
	// QueryCount is len(input queries).
	QueryCount int
	// UniqueHits is the count of distinct lineages observed across all
	// queries, before the total-limit truncation is applied.
	UniqueHits int
	// PerQueryHits[i] is the number of hits returned by input query i.
	PerQueryHits []int
}
