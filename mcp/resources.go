package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"

	memgraph "github.com/camggould/memgraph"
	sdkmcp "github.com/modelcontextprotocol/go-sdk/mcp"
)

// registerResources wires graphs and lineages as MCP resources.
//
//   memgraph://<graph_id>              -> graph summary JSON
//   memgraph://<graph_id>/<lineage_id> -> current node JSON
//
// Existing graphs are enumerated once at build time. New graphs that appear
// after the server starts are picked up via a Store subscription installed
// here — its OnGraphCreated handler calls AddResource on the SDK server, and
// the SDK automatically sends notifications/resources/list_changed to
// connected clients. The returned unsubscribe is stored on Server so Serve
// can release it on shutdown.
//
// The lineage form is registered as a resource template — clients can
// ReadResource any URI that matches.
func (s *Server) registerResources(srv *sdkmcp.Server) {
	// Best-effort: enumerate graphs so clients see them via ListResources.
	// Failures here shouldn't block server startup.
	if gs, err := s.store.ListGraphs(context.Background()); err == nil {
		for _, g := range gs {
			s.addGraphResource(srv, g)
		}
	}
	// Lineage URIs are unbounded; expose as a template.
	srv.AddResourceTemplate(&sdkmcp.ResourceTemplate{
		URITemplate: "memgraph://{graph_id}/{lineage_id}",
		Name:        "memgraph_lineage",
		Description: "Current version of a lineage, JSON-encoded.",
		MIMEType:    "application/json",
	}, s.handleReadResource)

	// Subscribe so graphs created (or updated) post-startup show up in
	// ListResources without a server restart. AddResource replaces an entry
	// with the same URI, so UpdateGraphConfig safely refreshes the Name.
	// The SDK fires notifications/resources/list_changed for us.
	if unsub, err := s.store.Subscribe(&liveResourceHandler{srv: srv, s: s}); err == nil {
		s.unsubResources = unsub
	}
}

// liveResourceHandler reflects store-side graph creation into the MCP
// server's resource list. Node and edge writes are no-ops here.
type liveResourceHandler struct {
	memgraph.NoopWriteHandler
	srv *sdkmcp.Server
	s   *Server
}

func (h *liveResourceHandler) OnGraphCreated(_ context.Context, g memgraph.Graph) {
	h.s.addGraphResource(h.srv, g)
}

func (s *Server) addGraphResource(srv *sdkmcp.Server, g memgraph.Graph) {
	srv.AddResource(&sdkmcp.Resource{
		URI:         graphURI(g.ID),
		Name:        g.Name,
		Description: "memgraph graph " + string(g.ID),
		MIMEType:    "application/json",
	}, s.handleReadResource)
}

func graphURI(id memgraph.GraphID) string { return "memgraph://" + string(id) }

// handleReadResource serves both graph and lineage URIs.
func (s *Server) handleReadResource(ctx context.Context, req *sdkmcp.ReadResourceRequest) (*sdkmcp.ReadResourceResult, error) {
	u, err := url.Parse(req.Params.URI)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", memgraph.ErrInvalidInput, err)
	}
	if u.Scheme != "memgraph" {
		return nil, fmt.Errorf("%w: scheme %q", memgraph.ErrInvalidInput, u.Scheme)
	}
	// memgraph://<graph_id>[/<lineage_id>]: Host is graph_id; Path may carry
	// the lineage segment.
	graphID := memgraph.GraphID(u.Host)
	rest := strings.Trim(u.Path, "/")
	if rest == "" {
		g, err := s.store.GetGraph(ctx, graphID)
		if err != nil {
			return nil, err
		}
		m, err := s.store.SymlinkManifest(ctx, graphID)
		if err != nil {
			return nil, err
		}
		body, err := json.Marshal(toGraphOut(g, len(m.Outbound), len(m.Inbound)))
		if err != nil {
			return nil, err
		}
		return &sdkmcp.ReadResourceResult{
			Contents: []*sdkmcp.ResourceContents{{
				URI: req.Params.URI, MIMEType: "application/json", Text: string(body),
			}},
		}, nil
	}
	lineageID := memgraph.LineageID(rest)
	n, err := s.store.GetNodeByLineage(ctx, lineageID, memgraph.ReadOpts{})
	if err != nil {
		return nil, err
	}
	body, err := json.Marshal(toNodeOut(n, false))
	if err != nil {
		return nil, err
	}
	return &sdkmcp.ReadResourceResult{
		Contents: []*sdkmcp.ResourceContents{{
			URI: req.Params.URI, MIMEType: "application/json", Text: string(body),
		}},
	}, nil
}
