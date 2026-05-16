// Package memgraph defines the core data model and storage contract for the
// memgraph knowledge substrate: nodes, edges, graphs, lineages, versions,
// freshness, and symlinks. See PRD.md for the design rationale.
//
// memgraph itself is the persistence layer. Authentication, authorization,
// UI, and ingestion live in clients above this package.
package memgraph
