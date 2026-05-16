# memgraph

A versioned, multi-graph knowledge substrate for agents and teams.

> **Status:** Pre-alpha. Design phase. See [PRD.md](PRD.md) for the full vision and v1 scope.

memgraph is an open-source agentic persistence layer: a durable, queryable graph store designed to be the long-term memory and shared knowledge substrate for teams of humans and agents. It is the successor to [`camggould/kb`](https://github.com/camggould/kb), generalizing its single-table KB into a real graph with first-class edges, versioning, freshness, cross-graph symlinks, and a pluggable storage layer.

memgraph is **a persistence layer, not an application** — it's opinionated about the data model (graph + versioned lineage + symlinks + provenance) and agnostic about who uses it and how. Authentication, authorization, UI, and ingestion live in clients above this layer.

## Why

LLM agents need memory that survives sessions, scales beyond a context window, and is shareable across teammates and tools. Existing options force a choice between toy local stores (no concurrency, no audit) and heavyweight graph databases (no MCP, no agent-native API, no versioning model for evolving facts). memgraph aims to be the small, sharp tool in between: easy to embed, easy to deploy, hard to corrupt.

## Status

This repository currently contains the [PRD](PRD.md) and a Go module scaffold. Implementation is in progress.

## License

MIT
