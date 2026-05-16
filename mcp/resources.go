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
// The SDK requires resources to be added at registration time, so we list
// graphs at build time and register each one. The lineage form is registered
// as a resource template — clients can ReadResource any URI that matches.
func (s *Server) registerResources(srv *sdkmcp.Server) {
	// Best-effort: enumerate graphs so clients see them via ListResources.
	// Failures here shouldn't block server startup.
	if gs, err := s.store.ListGraphs(context.Background()); err == nil {
		for _, g := range gs {
			uri := graphURI(g.ID)
			srv.AddResource(&sdkmcp.Resource{
				URI:         uri,
				Name:        g.Name,
				Description: "memgraph graph " + string(g.ID),
				MIMEType:    "application/json",
			}, s.handleReadResource)
		}
	}
	// Lineage URIs are unbounded; expose as a template.
	srv.AddResourceTemplate(&sdkmcp.ResourceTemplate{
		URITemplate: "memgraph://{graph_id}/{lineage_id}",
		Name:        "memgraph_lineage",
		Description: "Current version of a lineage, JSON-encoded.",
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
