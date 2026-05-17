package mcp

import (
	"context"
	"errors"
	"fmt"
	"time"

	memgraph "github.com/camggould/memgraph"
	sdkmcp "github.com/modelcontextprotocol/go-sdk/mcp"
)

// --- DTOs (JSON-serialized forms of memgraph types) ---

type nodeOut struct {
	ID           string         `json:"id"`
	GraphID      string         `json:"graph_id"`
	LineageID    string         `json:"lineage_id"`
	Version      int            `json:"version"`
	Kind         string         `json:"kind"`
	Content      string         `json:"content"`
	Summary      string         `json:"summary,omitempty"`
	Tags         []string       `json:"tags,omitempty"`
	Metadata     map[string]any `json:"metadata,omitempty"`
	FreshnessAt  *time.Time     `json:"freshness_at,omitempty"`
	CreatedAt    time.Time      `json:"created_at"`
	CreatedBy    string         `json:"created_by"`
	SupersededBy *string        `json:"superseded_by,omitempty"`
	IsCurrent    bool           `json:"is_current"`
	IsStale      bool           `json:"is_stale"`
	Conflicts    []string       `json:"conflicts,omitempty"`
}

func toNodeOut(n memgraph.Node, requestedVersion bool) nodeOut {
	out := nodeOut{
		ID:          string(n.ID),
		GraphID:     string(n.GraphID),
		LineageID:   string(n.LineageID),
		Version:     n.Version,
		Kind:        n.Kind,
		Content:     n.Content,
		Summary:     n.Summary,
		Tags:        n.Tags,
		Metadata:    n.Metadata,
		FreshnessAt: n.FreshnessAt,
		CreatedAt:   n.CreatedAt,
		CreatedBy:   n.CreatedBy,
	}
	if n.SupersededBy != nil {
		s := string(*n.SupersededBy)
		out.SupersededBy = &s
	}
	if len(n.Conflicts) > 0 {
		out.Conflicts = make([]string, 0, len(n.Conflicts))
		for _, c := range n.Conflicts {
			out.Conflicts = append(out.Conflicts, string(c))
		}
	}
	// is_current: a node is current if it has no successor. If the caller
	// asked for a specific version (requestedVersion=true), the node may be
	// historical even when superseded_by is nil only at the head.
	out.IsCurrent = n.SupersededBy == nil
	if requestedVersion {
		// When pinned by version/time, the result is "current" only if it
		// happens to coincide with the head — same condition.
		out.IsCurrent = n.SupersededBy == nil
	}
	out.IsStale = out.IsCurrent && n.FreshnessAt != nil && n.FreshnessAt.Before(time.Now())
	return out
}

type edgeOut struct {
	ID          string         `json:"id"`
	GraphID     string         `json:"graph_id"`
	FromLineage string         `json:"from_lineage"`
	ToGraph     string         `json:"to_graph"`
	ToLineage   string         `json:"to_lineage"`
	Kind        string         `json:"kind"`
	Metadata    map[string]any `json:"metadata,omitempty"`
	Ordinal     *int           `json:"ordinal,omitempty"`
	CreatedAt   time.Time      `json:"created_at"`
	CreatedBy   string         `json:"created_by"`
}

func toEdgeOut(e memgraph.Edge) edgeOut {
	return edgeOut{
		ID:          string(e.ID),
		GraphID:     string(e.GraphID),
		FromLineage: string(e.FromLineage),
		ToGraph:     string(e.ToGraph),
		ToLineage:   string(e.ToLineage),
		Kind:        e.Kind,
		Metadata:    e.Metadata,
		Ordinal:     e.Ordinal,
		CreatedAt:   e.CreatedAt,
		CreatedBy:   e.CreatedBy,
	}
}

type graphOut struct {
	ID             string         `json:"id"`
	Name           string         `json:"name"`
	ConflictPolicy string         `json:"conflict_policy"`
	KindWhitelist  []string       `json:"kind_whitelist,omitempty"`
	Metadata       map[string]any `json:"metadata,omitempty"`
	CreatedAt      time.Time      `json:"created_at"`
	OutboundCount  int            `json:"outbound_count"`
	InboundCount   int            `json:"inbound_count"`
}

func toGraphOut(g memgraph.Graph, outbound, inbound int) graphOut {
	return graphOut{
		ID:             string(g.ID),
		Name:           g.Name,
		ConflictPolicy: string(g.ConflictPolicy),
		KindWhitelist:  g.KindWhitelist,
		Metadata:       g.Metadata,
		CreatedAt:      g.CreatedAt,
		OutboundCount:  outbound,
		InboundCount:   inbound,
	}
}

type graphRefOut struct {
	GraphID   string `json:"graph_id"`
	EdgeCount int    `json:"edge_count"`
}

type symlinkManifestOut struct {
	Outbound []graphRefOut `json:"outbound"`
	Inbound  []graphRefOut `json:"inbound"`
}

func toSymlinkManifestOut(m memgraph.SymlinkManifest) symlinkManifestOut {
	out := symlinkManifestOut{
		Outbound: make([]graphRefOut, 0, len(m.Outbound)),
		Inbound:  make([]graphRefOut, 0, len(m.Inbound)),
	}
	for _, r := range m.Outbound {
		out.Outbound = append(out.Outbound, graphRefOut{GraphID: string(r.GraphID), EdgeCount: r.EdgeCount})
	}
	for _, r := range m.Inbound {
		out.Inbound = append(out.Inbound, graphRefOut{GraphID: string(r.GraphID), EdgeCount: r.EdgeCount})
	}
	return out
}

// --- Tool input/output shapes ---

type listGraphsOut struct {
	Graphs []graphOut `json:"graphs"`
}

type getNodeIn struct {
	NodeID    string `json:"node_id,omitempty" jsonschema:"specific node version id"`
	LineageID string `json:"lineage_id,omitempty" jsonschema:"lineage id; resolves to current version"`
	AtVersion *int   `json:"at_version,omitempty" jsonschema:"pin to a specific version (with lineage_id)"`
	AtTime    string `json:"at_time,omitempty" jsonschema:"RFC3339 point-in-time (with lineage_id)"`
}

type historyIn struct {
	LineageID string `json:"lineage_id" jsonschema:"lineage id to walk"`
}

type historyOut struct {
	Versions []nodeOut `json:"versions"`
}

type traverseIn struct {
	FromLineage    string   `json:"from_lineage" jsonschema:"lineage to start from"`
	MaxDepth       int      `json:"max_depth,omitempty" jsonschema:"max edge hops; default 2"`
	EdgeKinds      []string `json:"edge_kinds,omitempty"`
	FollowSymlinks bool     `json:"follow_symlinks,omitempty"`
	MaxNodes       int      `json:"max_nodes,omitempty" jsonschema:"output budget; default 50"`
	Direction      string   `json:"direction,omitempty" jsonschema:"one of: outgoing (default), incoming, both"`
}

type traverseOut struct {
	Nodes []nodeOut `json:"nodes"`
	Edges []edgeOut `json:"edges"`
}

type searchIn struct {
	GraphID   string   `json:"graph_id"`
	Text      string   `json:"text"`
	Kinds     []string `json:"kinds,omitempty"`
	Tags      []string `json:"tags,omitempty"`
	FreshOnly bool     `json:"fresh_only,omitempty"`
	Limit     int      `json:"limit,omitempty" jsonschema:"default 20"`
}

type searchHitOut struct {
	Node    nodeOut `json:"node"`
	Snippet string  `json:"snippet,omitempty"`
	Score   float64 `json:"score"`
}

type searchOut struct {
	Hits []searchHitOut `json:"hits"`
}

type symlinkManifestIn struct {
	GraphID string `json:"graph_id"`
}

type createGraphIn struct {
	Name           string         `json:"name"`
	ConflictPolicy string         `json:"conflict_policy,omitempty" jsonschema:"lww|manual; default lww"`
	KindWhitelist  []string       `json:"kind_whitelist,omitempty"`
	Metadata       map[string]any `json:"metadata,omitempty"`
}

type putNodeIn struct {
	GraphID        string         `json:"graph_id"`
	Kind           string         `json:"kind"`
	Content        string         `json:"content"`
	LineageID      string         `json:"lineage_id,omitempty" jsonschema:"omit to start a new lineage"`
	Summary        string         `json:"summary,omitempty"`
	Tags           []string       `json:"tags,omitempty"`
	Metadata       map[string]any `json:"metadata,omitempty"`
	FreshnessAt    string         `json:"freshness_at,omitempty" jsonschema:"RFC3339"`
	CreatedBy      string         `json:"created_by,omitempty" jsonschema:"opaque provenance; default \"unknown\""`
	BasedOnVersion *int           `json:"based_on_version,omitempty" jsonschema:"optimistic-concurrency hint; the version the writer believed was current. Mismatch under manual conflict policy records a sibling."`
}

type putEdgeIn struct {
	GraphID     string         `json:"graph_id"`
	FromLineage string         `json:"from_lineage"`
	ToLineage   string         `json:"to_lineage"`
	Kind        string         `json:"kind"`
	ToGraph     string         `json:"to_graph,omitempty" jsonschema:"defaults to graph_id"`
	Metadata    map[string]any `json:"metadata,omitempty"`
	Ordinal     *int           `json:"ordinal,omitempty"`
	CreatedBy   string         `json:"created_by,omitempty"`
}

type deleteEdgeIn struct {
	EdgeID string `json:"edge_id"`
}

type okOut struct {
	OK bool `json:"ok"`
}

// --- Handler registration ---

func (s *Server) registerTools(srv *sdkmcp.Server) {
	sdkmcp.AddTool(srv, &sdkmcp.Tool{
		Name:        "memgraph_list_graphs",
		Description: "List graphs with id, name, conflict policy, and symlink-manifest counts.",
	}, s.handleListGraphs)

	sdkmcp.AddTool(srv, &sdkmcp.Tool{
		Name:        "memgraph_get_node",
		Description: "Fetch a node by node_id or lineage_id. Returns is_current and is_stale flags.",
	}, s.handleGetNode)

	sdkmcp.AddTool(srv, &sdkmcp.Tool{
		Name:        "memgraph_history",
		Description: "Return all versions of a lineage, newest first.",
	}, s.handleHistory)

	sdkmcp.AddTool(srv, &sdkmcp.Tool{
		Name:        "memgraph_traverse",
		Description: "Walk edges from a lineage. Default depth 2, max 50 nodes. Default direction is outgoing; use direction='incoming' for backlinks or 'both' for undirected reachability.",
	}, s.handleTraverse)

	sdkmcp.AddTool(srv, &sdkmcp.Tool{
		Name:        "memgraph_search",
		Description: "Full-text search within a graph with kind/tag/freshness filters.",
	}, s.handleSearch)

	sdkmcp.AddTool(srv, &sdkmcp.Tool{
		Name:        "memgraph_symlink_manifest",
		Description: "List cross-graph references for a graph.",
	}, s.handleSymlinkManifest)

	sdkmcp.AddTool(srv, &sdkmcp.Tool{
		Name:        "memgraph_create_graph",
		Description: "Create a new graph.",
	}, s.handleCreateGraph)

	sdkmcp.AddTool(srv, &sdkmcp.Tool{
		Name:        "memgraph_put_node",
		Description: "Create a new node lineage or append a new version. Omit lineage_id to start fresh.",
	}, s.handlePutNode)

	sdkmcp.AddTool(srv, &sdkmcp.Tool{
		Name:        "memgraph_put_edge",
		Description: "Create a typed edge between two lineages (possibly cross-graph).",
	}, s.handlePutEdge)

	sdkmcp.AddTool(srv, &sdkmcp.Tool{
		Name:        "memgraph_delete_edge",
		Description: "Remove an edge by id.",
	}, s.handleDeleteEdge)
}

// --- Handlers ---

func (s *Server) handleListGraphs(ctx context.Context, _ *sdkmcp.CallToolRequest, _ struct{}) (*sdkmcp.CallToolResult, listGraphsOut, error) {
	gs, err := s.store.ListGraphs(ctx)
	if err != nil {
		return nil, listGraphsOut{}, err
	}
	out := listGraphsOut{Graphs: make([]graphOut, 0, len(gs))}
	for _, g := range gs {
		m, err := s.store.SymlinkManifest(ctx, g.ID)
		if err != nil {
			return nil, listGraphsOut{}, err
		}
		out.Graphs = append(out.Graphs, toGraphOut(g, len(m.Outbound), len(m.Inbound)))
	}
	return nil, out, nil
}

func (s *Server) handleGetNode(ctx context.Context, _ *sdkmcp.CallToolRequest, in getNodeIn) (*sdkmcp.CallToolResult, nodeOut, error) {
	if (in.NodeID == "") == (in.LineageID == "") {
		return nil, nodeOut{}, fmt.Errorf("%w: provide exactly one of node_id or lineage_id", memgraph.ErrInvalidInput)
	}
	if in.NodeID != "" {
		n, err := s.store.GetNodeByID(ctx, memgraph.NodeID(in.NodeID))
		if err != nil {
			return nil, nodeOut{}, err
		}
		return nil, toNodeOut(n, true), nil
	}
	var opts memgraph.ReadOpts
	if in.AtVersion != nil {
		opts.AtVersion = in.AtVersion
	}
	if in.AtTime != "" {
		t, err := time.Parse(time.RFC3339, in.AtTime)
		if err != nil {
			return nil, nodeOut{}, fmt.Errorf("%w: at_time: %v", memgraph.ErrInvalidInput, err)
		}
		opts.AtTime = &t
	}
	n, err := s.store.GetNodeByLineage(ctx, memgraph.LineageID(in.LineageID), opts)
	if err != nil {
		return nil, nodeOut{}, err
	}
	return nil, toNodeOut(n, opts.AtVersion != nil || opts.AtTime != nil), nil
}

func (s *Server) handleHistory(ctx context.Context, _ *sdkmcp.CallToolRequest, in historyIn) (*sdkmcp.CallToolResult, historyOut, error) {
	if in.LineageID == "" {
		return nil, historyOut{}, fmt.Errorf("%w: lineage_id required", memgraph.ErrInvalidInput)
	}
	ns, err := s.store.History(ctx, memgraph.LineageID(in.LineageID))
	if err != nil {
		return nil, historyOut{}, err
	}
	out := historyOut{Versions: make([]nodeOut, 0, len(ns))}
	for _, n := range ns {
		out.Versions = append(out.Versions, toNodeOut(n, true))
	}
	return nil, out, nil
}

func (s *Server) handleTraverse(ctx context.Context, _ *sdkmcp.CallToolRequest, in traverseIn) (*sdkmcp.CallToolResult, traverseOut, error) {
	if in.FromLineage == "" {
		return nil, traverseOut{}, fmt.Errorf("%w: from_lineage required", memgraph.ErrInvalidInput)
	}
	depth := in.MaxDepth
	if depth <= 0 {
		depth = 2
	}
	maxN := in.MaxNodes
	if maxN <= 0 {
		maxN = 50
	}
	dir := memgraph.TraverseDirection(in.Direction)
	switch dir {
	case "", memgraph.TraverseOutgoing, memgraph.TraverseIncoming, memgraph.TraverseBoth:
		// ok
	default:
		return nil, traverseOut{}, fmt.Errorf("%w: direction must be one of outgoing, incoming, both", memgraph.ErrInvalidInput)
	}
	res, err := s.store.Traverse(ctx, memgraph.LineageID(in.FromLineage), memgraph.TraverseOpts{
		MaxDepth:       depth,
		EdgeKinds:      in.EdgeKinds,
		FollowSymlinks: in.FollowSymlinks,
		MaxNodes:       maxN,
		Direction:      dir,
	})
	if err != nil {
		return nil, traverseOut{}, err
	}
	out := traverseOut{
		Nodes: make([]nodeOut, 0, len(res.Nodes)),
		Edges: make([]edgeOut, 0, len(res.Edges)),
	}
	for _, n := range res.Nodes {
		out.Nodes = append(out.Nodes, toNodeOut(n, false))
	}
	for _, e := range res.Edges {
		out.Edges = append(out.Edges, toEdgeOut(e))
	}
	return nil, out, nil
}

func (s *Server) handleSearch(ctx context.Context, _ *sdkmcp.CallToolRequest, in searchIn) (*sdkmcp.CallToolResult, searchOut, error) {
	if in.GraphID == "" {
		return nil, searchOut{}, fmt.Errorf("%w: graph_id required", memgraph.ErrInvalidInput)
	}
	limit := in.Limit
	if limit <= 0 {
		limit = 20
	}
	hits, err := s.store.Search(ctx, memgraph.GraphID(in.GraphID), memgraph.SearchQuery{
		Text:      in.Text,
		Kinds:     in.Kinds,
		Tags:      in.Tags,
		FreshOnly: in.FreshOnly,
		Limit:     limit,
	})
	if err != nil {
		return nil, searchOut{}, err
	}
	out := searchOut{Hits: make([]searchHitOut, 0, len(hits))}
	for _, h := range hits {
		out.Hits = append(out.Hits, searchHitOut{
			Node:    toNodeOut(h.Node, false),
			Snippet: h.Snippet,
			Score:   h.Score,
		})
	}
	return nil, out, nil
}

func (s *Server) handleSymlinkManifest(ctx context.Context, _ *sdkmcp.CallToolRequest, in symlinkManifestIn) (*sdkmcp.CallToolResult, symlinkManifestOut, error) {
	if in.GraphID == "" {
		return nil, symlinkManifestOut{}, fmt.Errorf("%w: graph_id required", memgraph.ErrInvalidInput)
	}
	m, err := s.store.SymlinkManifest(ctx, memgraph.GraphID(in.GraphID))
	if err != nil {
		return nil, symlinkManifestOut{}, err
	}
	return nil, toSymlinkManifestOut(m), nil
}

func (s *Server) handleCreateGraph(ctx context.Context, _ *sdkmcp.CallToolRequest, in createGraphIn) (*sdkmcp.CallToolResult, graphOut, error) {
	if in.Name == "" {
		return nil, graphOut{}, fmt.Errorf("%w: name required", memgraph.ErrInvalidInput)
	}
	policy := memgraph.ConflictPolicy(in.ConflictPolicy)
	g, err := s.store.CreateGraph(ctx, memgraph.GraphInput{
		Name:           in.Name,
		ConflictPolicy: policy,
		KindWhitelist:  in.KindWhitelist,
		Metadata:       in.Metadata,
	})
	if err != nil {
		return nil, graphOut{}, err
	}
	return nil, toGraphOut(g, 0, 0), nil
}

func (s *Server) handlePutNode(ctx context.Context, _ *sdkmcp.CallToolRequest, in putNodeIn) (*sdkmcp.CallToolResult, nodeOut, error) {
	var fresh *time.Time
	if in.FreshnessAt != "" {
		t, err := time.Parse(time.RFC3339, in.FreshnessAt)
		if err != nil {
			return nil, nodeOut{}, fmt.Errorf("%w: freshness_at: %v", memgraph.ErrInvalidInput, err)
		}
		fresh = &t
	}
	createdBy := in.CreatedBy
	if createdBy == "" {
		createdBy = "unknown"
	}
	n, err := s.store.PutNode(ctx, memgraph.NodeInput{
		GraphID:        memgraph.GraphID(in.GraphID),
		LineageID:      memgraph.LineageID(in.LineageID),
		Kind:           in.Kind,
		Content:        in.Content,
		Summary:        in.Summary,
		Tags:           in.Tags,
		Metadata:       in.Metadata,
		FreshnessAt:    fresh,
		CreatedBy:      createdBy,
		BasedOnVersion: in.BasedOnVersion,
	})
	if err != nil {
		// Manual conflict is a successful write that flags a conflict. We
		// still return the node (with Conflicts populated) so the client
		// can act on it, but mark the result as an error so the surfaces
		// that check IsError can branch on it.
		if errors.Is(err, memgraph.ErrConflictManual) {
			res := &sdkmcp.CallToolResult{
				IsError: true,
				Content: []sdkmcp.Content{
					&sdkmcp.TextContent{Text: err.Error()},
				},
			}
			return res, toNodeOut(n, false), nil
		}
		return nil, nodeOut{}, err
	}
	return nil, toNodeOut(n, false), nil
}

func (s *Server) handlePutEdge(ctx context.Context, _ *sdkmcp.CallToolRequest, in putEdgeIn) (*sdkmcp.CallToolResult, edgeOut, error) {
	e, err := s.store.PutEdge(ctx, memgraph.EdgeInput{
		GraphID:     memgraph.GraphID(in.GraphID),
		FromLineage: memgraph.LineageID(in.FromLineage),
		ToGraph:     memgraph.GraphID(in.ToGraph),
		ToLineage:   memgraph.LineageID(in.ToLineage),
		Kind:        in.Kind,
		Metadata:    in.Metadata,
		Ordinal:     in.Ordinal,
		CreatedBy:   in.CreatedBy,
	})
	if err != nil {
		return nil, edgeOut{}, err
	}
	return nil, toEdgeOut(e), nil
}

func (s *Server) handleDeleteEdge(ctx context.Context, _ *sdkmcp.CallToolRequest, in deleteEdgeIn) (*sdkmcp.CallToolResult, okOut, error) {
	if in.EdgeID == "" {
		return nil, okOut{}, fmt.Errorf("%w: edge_id required", memgraph.ErrInvalidInput)
	}
	if err := s.store.DeleteEdge(ctx, memgraph.EdgeID(in.EdgeID)); err != nil {
		return nil, okOut{}, err
	}
	return nil, okOut{OK: true}, nil
}
