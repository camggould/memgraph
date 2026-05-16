// Package mcp exposes a memgraph.Store as a Model Context Protocol server.
//
// Tool surface mirrors §8 of PRD.md: read tools (list_graphs, get_node,
// history, traverse, search, symlink_manifest) and write tools (create_graph,
// put_node, put_edge, delete_edge). Graphs and lineages are also exposed as
// MCP resources under the memgraph:// scheme.
package mcp

import (
	"context"
	"errors"

	memgraph "github.com/camggould/memgraph"
	sdkmcp "github.com/modelcontextprotocol/go-sdk/mcp"
)

// Server adapts a memgraph.Store to the MCP transport.
type Server struct {
	store memgraph.Store
	name  string
	ver   string
	// unsubResources releases the Store subscription installed by
	// registerResources so the live-resource handler stops running after
	// the SDK server shuts down. Set by build via registerResources.
	unsubResources memgraph.Unsubscribe
}

// Option configures a Server.
type Option func(*Server)

// WithName overrides the MCP server name advertised to clients.
func WithName(name string) Option { return func(s *Server) { s.name = name } }

// WithVersion overrides the MCP server version advertised to clients.
func WithVersion(version string) Option { return func(s *Server) { s.ver = version } }

// New returns a new MCP server bound to the given store.
func New(store memgraph.Store, opts ...Option) *Server {
	s := &Server{store: store, name: "memgraph", ver: "0.1.0"}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Serve runs the MCP server on stdio. Blocks until ctx is cancelled or the
// transport closes.
func (s *Server) Serve(ctx context.Context) error {
	srv := s.build()
	defer func() {
		if s.unsubResources != nil {
			s.unsubResources()
			s.unsubResources = nil
		}
	}()
	err := srv.Run(ctx, &sdkmcp.StdioTransport{})
	// ctx cancellation is a clean shutdown.
	if err != nil && (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
		return nil
	}
	return err
}

// build constructs and registers tools/resources on a fresh SDK server. Split
// out so tests can connect via an in-memory transport.
func (s *Server) build() *sdkmcp.Server {
	srv := sdkmcp.NewServer(&sdkmcp.Implementation{Name: s.name, Version: s.ver}, nil)
	s.registerTools(srv)
	s.registerResources(srv)
	return srv
}
