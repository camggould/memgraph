// Package mcp exposes a memgraph.Store as a Model Context Protocol server.
//
// The MCP tool surface mirrors §8 of PRD.md: read tools (list_graphs,
// get_node, history, traverse, search, symlink_manifest) and write tools
// (create_graph, put_node, put_edge, delete_edge).
//
// TODO(v1): wire up the chosen Go MCP SDK (official modelcontextprotocol/go-sdk
// vs. mark3labs/mcp-go — decided during scaffolding).
package mcp

import (
	"context"
	"fmt"

	memgraph "github.com/camggould/memgraph"
)

// Server adapts a memgraph.Store to the MCP transport. Constructed with the
// Store to expose; lifecycle is managed by the caller (typically the CLI's
// `memgraph serve` command).
type Server struct {
	store memgraph.Store
}

// New returns a new MCP server bound to the given store.
func New(store memgraph.Store) *Server {
	return &Server{store: store}
}

// Serve starts the MCP server on stdio. Blocks until ctx is cancelled or the
// transport closes.
//
// TODO(v1): implement using the chosen MCP SDK.
func (s *Server) Serve(ctx context.Context) error {
	return fmt.Errorf("memgraph/mcp: %w", memgraph.ErrNotImplemented)
}
