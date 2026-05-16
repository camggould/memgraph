# memgraph — PRD

**Status:** Draft v0.1
**Author:** Cam Gould
**Last updated:** 2026-05-16

---

## 1. Vision

memgraph is an **open-source agentic persistence layer**: a durable, queryable, versioned graph store designed to be the long-term memory and shared knowledge substrate for teams of humans and agents.

It is the successor to [`camggould/kb`](https://github.com/camggould/kb), which proved out the "atomic notes + MCP + LLM" loop on a single SQLite-backed store. memgraph generalizes that idea into a real graph substrate with first-class relationships, versioning, freshness, multi-graph topology with cross-graph symlinks, and a pluggable storage layer — deployable locally or in the cloud, and exposed primarily via MCP.

memgraph is **a persistence layer, not an application.** It is opinionated about the *data model* (graph + versioned lineage + symlinks + provenance) and agnostic about *who uses it and how*. Authentication, authorization, UI, document rendering, ingestion pipelines, and team workflows all live in clients above this layer.

### One-line pitch

> A versioned, multi-graph knowledge substrate for agents and teams — minimal nodes, rich edges, cross-graph symlinks, durable history, no built-in auth. Bring your own application.

---

## 2. Non-goals

- **Not an authentication or authorization system.** memgraph stores opaque provenance strings (`created_by`) but never authenticates, authorizes, or filters. Consumers wrap memgraph in their own access layer.
- **Not a UI.** No web app, no document editor, no admin console in v1. Reference clients are explicitly *separate projects*.
- **Not a general-purpose graph database.** memgraph is shaped specifically for knowledge representation and agent memory. Workloads like analytics over billion-edge graphs are out of scope; use Neo4j or a real graph DB.
- **Not a vector database.** v1.1 will integrate a vector index as a *derived* index over graph nodes, but memgraph's identity is the graph, not the vectors.
- **Not a CRDT framework.** Conflict resolution is intentionally simple (configurable per-graph; default LWW). Sophisticated mergeable types are a future concern.
- **Not a document store.** The L2 document-authoring plugin (see §11) is a separate tool that *uses* memgraph; memgraph itself doesn't know what a "document" is.

---

## 3. Personas & primary jobs-to-be-done

### P1. Solo developer + agent

A single user runs memgraph locally. Their coding agent uses memgraph as long-term memory across sessions: facts about the codebase, the user's preferences, prior decisions, ongoing projects. Latency must feel instantaneous.

- *Add fact:* "User prefers tabs over spaces in Go projects."
- *Retrieve:* "What does the user prefer for Go formatting?"
- *Update:* "Actually, they switched to spaces last Tuesday." (lineage update)

### P2. Small team (3–20 people)

A team deploys memgraph as shared infrastructure. Each member's agent reads and writes to a shared graph (or graphs). The team uses it as durable tribal-knowledge storage: onboarding facts, architectural decisions, postmortem learnings, undocumented gotchas.

- *Concurrent writes:* two agents add facts about the same subject at the same time — memgraph resolves the conflict per the graph's configured policy.
- *Cross-team boundary:* the platform team's graph symlinks into the security team's graph for shared compliance facts.

### P3. Plugin/client author

A developer builds an application (doc editor, agent IDE plugin, ingestion bot) on top of memgraph. They need a stable MCP surface and a clear data model. They do not want memgraph to dictate their UX, auth, or schema.

---

## 4. Conceptual model

### 4.1 Node

The atomic unit. Intentionally minimal — a container with metadata, not a domain object.

```
Node {
  id:           ULID                 // stable, opaque, deployment-unique
  graph_id:     ULID                 // which graph owns this node
  lineage_id:   ULID                 // the conceptual "thing"; multiple versions share one
  version:      monotonic int        // 1, 2, 3...
  kind:         string               // extensible: "fact", "segment", "image_ref", ...
  content:      string               // primary payload, kept small (target: < 1KB)
  summary:      string?              // optional short blurb; falls back to `content` if absent
  tags:         string[]             // freeform categorization, indexed
  metadata:     json                 // freeform structured data, opaque to memgraph core
  freshness_at: timestamp?           // optional declared "valid until" date
  created_at:   timestamp            // immutable
  created_by:   string               // opaque provenance string; memgraph never interprets it
  superseded_by: node_id?            // back-reference set when a newer version exists
}
```

**Design notes:**

- Nodes are **content-addressed and immutable** once written. An "edit" creates a new version with the same `lineage_id`.
- `graph_id` is the unit of isolation (see §4.3). A node lives in exactly one graph.
- `kind` is a free string. memgraph treats it as opaque except for indexing; clients define their own ontology (`fact`, `segment`, `entity:person`, `decision`, etc.).
- `summary` exists only to optimize progressive disclosure. If `content` is already short enough to be a summary, leave `summary` empty and clients fall back to `content`.
- `freshness_at` is **declarative, not enforced.** memgraph surfaces it on read; clients decide whether to warn the user.
- `metadata` is a free JSON blob for client-specific data (rendering hints, source URLs, custom fields). memgraph stores and returns it untouched.

### 4.2 Edge

First-class typed relationships between nodes.

```
Edge {
  id:               ULID
  graph_id:         ULID                       // graph this edge lives in
  from_lineage:     ULID                       // source lineage_id (NOT version_id)
  to_lineage:       (graph_id, lineage_id)     // target, possibly in another graph (symlink)
  kind:             string                     // "child_of", "cites", "contradicts", ...
  metadata:         json
  ordinal:          int?                       // optional ordering hint among sibling edges
  created_at:       timestamp
  created_by:       string                     // opaque provenance
}
```

**Design notes:**

- Edges target `lineage_id`, not `version_id`. This is what makes references survive versioning: when a node is updated, every edge pointing at it automatically resolves to the current version. Old versions are only reachable through explicit history queries.
- `kind` is free-form. memgraph ships with **no** reserved edge kinds. Clients pick their own vocabulary.
- `ordinal` exists because ordered children are a recurring need (document segments, list items, ranked alternatives). Cheap to support; expensive to bolt on later.
- A symlink is **not a special edge type** — it's an ordinary edge whose `to_lineage` references a different `graph_id` than the edge itself. This keeps the API minimal.

### 4.3 Graph

The unit of isolation. A deployment hosts N graphs.

```
Graph {
  id:                 ULID
  name:               string                 // human-readable; not necessarily unique
  conflict_policy:    "lww" | "manual"       // default: "lww"
  kind_whitelist:     string[]?              // optional allowed node/edge kinds
  metadata:           json                   // freeform graph-level config
  created_at:         timestamp
}
```

- Graphs do not nest. Hierarchy among graphs, if needed, is modeled by clients using a designated registry graph and symlinks.
- A node and its edges live in exactly one graph. The *target* of an edge may live in another graph (a symlink); this is the only cross-graph link.
- Per-graph configuration covers conflict policy and optional kind whitelisting. Most settings stay client-side.

### 4.4 Symlinks (cross-graph edges)

A symlink is simply an edge whose target lives in a different graph than the edge itself. Properties:

- Symlinks are stored in the **source graph** (where the edge originates).
- Each graph maintains a denormalized **manifest** of its outbound and inbound symlinks for inventory and cheap traversal-cost estimation: "this graph references 14 other graphs; this graph is referenced by 3."
- Resolution at traversal time:
  - If the target graph is in the same deployment, memgraph resolves transparently.
  - If the target graph is on a remote deployment (deferred to v2), memgraph returns a **symlink stub** containing `(graph_id, lineage_id, last_known_summary)` and lets the client decide whether to dereference.
- Symlinks survive renames (graphs are addressed by ULID) and content edits (edges target lineage IDs).

### 4.5 Lineage & versions

Every node has a stable `lineage_id`. The first time a fact is asserted, `lineage_id == node.id` and `version == 1`. Each subsequent edit:

1. Creates a new node with the same `lineage_id` and `version + 1`.
2. Sets the previous version's `superseded_by` to point at the new node.
3. Atomically updates the lineage-current index so future reads resolve to the new version by default.

**Read semantics:**

- `get(lineage_id)` → returns the current version. This is the default.
- `get(node_id)` → returns that specific version. Used for time-travel/audit.
- `history(lineage_id)` → returns all versions, newest first.
- `traverse(from_lineage)` → walks edges; each hop resolves to current versions unless `at_time` or `at_version` is passed.

This means agents never accidentally read stale data during normal traversal, and history is still preserved for explicit queries.

### 4.6 Freshness

Independent of lineage. A node may be the *current* version of its lineage and *also* be past its declared freshness horizon (no one has updated it in time). Both signals surface on read:

```
NodeRead {
  ...node fields...,
  is_current: bool,            // false only when explicitly reading an old version
  is_stale: bool,              // true if freshness_at < now and is_current
  superseded_at: timestamp?,   // when a newer version replaced this one (if not current)
}
```

memgraph never refuses to return stale data; it only flags it. Clients decide whether to warn the user or proactively prompt for an update.

---

## 5. Conflict resolution

When two writers concurrently create new versions for the same `lineage_id`, the conflict policy on the graph determines the outcome:

- **`lww` (last-writer-wins, default):** highest `created_at` wins as the current version. The loser is still written as a versioned record (audit preserved) but immediately marked `superseded_by` the winner.
- **`manual`:** both versions are written; the lineage-current index is left ambiguous (returns the most recent + a `conflicts: [...]` field). Resolving the conflict means writing a third version that explicitly supersedes both.

Conflict policy is per-graph and configurable at graph creation; it can be changed later by an admin client (memgraph just exposes the setting; the application layer enforces who can change it).

Future: an `agent_merge` policy where a designated client/agent is responsible for proposing a merged version. Out of scope for v1.

---

## 6. Provenance (not authentication)

memgraph stores `created_by` on every node and edge as an **opaque string**. memgraph never:

- Authenticates the writer
- Verifies the string matches any user identity
- Authorizes operations based on it
- Filters reads based on it

Clients put whatever they want there: a user ID, an OIDC subject, "agent:claude-opus-4-7", "ingestion-bot:v2", "anonymous". The string is searchable and queryable; it has no other semantics in memgraph itself.

This is deliberate. Building an application requires authn/authz; building a *persistence layer* does not. Pushing identity upstream keeps memgraph small, neutral, and easy to embed in any access model a consumer wants.

---

## 7. Storage abstraction

memgraph defines a `Store` interface that all reads and writes go through. Reference implementations ship with v1:

- **`SQLiteStore`** — single-file, embedded, ideal for local and single-process deployments. Uses `modernc.org/sqlite` (pure-Go, cgo-free — preserves single-binary cross-compilation) with FTS5 enabled. `mattn/go-sqlite3` is a tunable fallback if the cgo build becomes worthwhile for perf, but the default ships without cgo.
- **`PostgresStore`** — cloud, multi-process, multi-writer. Uses `pgx` with standard Postgres + `pg_trgm` for full-text. Drop-in target for managed Postgres (RDS, Supabase, Neon, etc.).

The interface exposes graph and index primitives — never SQL, never engine-specific types:

```go
type Store interface {
    // Graphs
    CreateGraph(ctx context.Context, g GraphInput) (Graph, error)
    GetGraph(ctx context.Context, id GraphID) (Graph, error)
    ListGraphs(ctx context.Context) ([]Graph, error)
    UpdateGraphConfig(ctx context.Context, id GraphID, patch GraphConfigPatch) (Graph, error)

    // Nodes (versioned)
    PutNode(ctx context.Context, in NodeInput) (Node, error)                       // creates v1 or v+1 of lineage
    GetNodeByLineage(ctx context.Context, id LineageID, opts ReadOpts) (Node, error)
    GetNodeByID(ctx context.Context, id NodeID) (Node, error)                      // exact version
    History(ctx context.Context, id LineageID) ([]Node, error)
    ListNodes(ctx context.Context, graphID GraphID, f NodeFilter) ([]Node, error)

    // Edges
    PutEdge(ctx context.Context, in EdgeInput) (Edge, error)
    DeleteEdge(ctx context.Context, id EdgeID) error
    Outgoing(ctx context.Context, from LineageID, opts TraverseOpts) ([]Edge, error)
    Incoming(ctx context.Context, to LineageID, opts TraverseOpts) ([]Edge, error)
    Traverse(ctx context.Context, from LineageID, opts TraverseOpts) (TraversalResult, error)

    // Indexes
    Search(ctx context.Context, graphID GraphID, q SearchQuery) ([]SearchHit, error)
    SymlinkManifest(ctx context.Context, graphID GraphID) (SymlinkManifest, error)

    // Hooks (for derived indexes; see §9)
    Subscribe(h WriteHandler) (Unsubscribe, error)
}

type WriteHandler interface {
    OnNodeWritten(context.Context, Node)
    OnEdgeWritten(context.Context, Edge)
}
```

Third parties can implement `Store` against Neo4j, DuckDB, FoundationDB, or anything else without touching the rest of the codebase.

**Anti-goal:** memgraph does NOT ship a bespoke embedded engine. Off-the-shelf databases handle this workload well.

---

## 8. MCP surface (v1)

memgraph's primary external interface is an MCP server. Tools are scoped tightly so agents can reason about them.

### Read tools

- `memgraph_list_graphs` — list graphs in the deployment, with size and symlink-manifest summary.
- `memgraph_get_node` — fetch by `node_id` (specific version) or `lineage_id` (current). Returns full node + freshness/staleness flags.
- `memgraph_history` — version history for a lineage.
- `memgraph_traverse` — walk edges from a starting lineage. Parameters: depth limit, edge-kind filter, include/exclude symlinks, output budget (max nodes returned).
- `memgraph_search` — full-text + tag + kind + freshness filters. Scoped to a single graph; cross-graph search is a client orchestration concern.
- `memgraph_symlink_manifest` — list cross-graph references for a graph.

### Write tools

- `memgraph_create_graph` — create a new graph with config.
- `memgraph_put_node` — create or update a lineage. Includes `created_by` (opaque), optional `lineage_id` (omit to create new lineage), `kind`, `content`, `summary`, `tags`, `metadata`, `freshness_at`.
- `memgraph_put_edge` — create an edge (intra- or cross-graph).
- `memgraph_delete_edge` — remove an edge. Nodes are not deletable in v1 (immutability); they can be superseded with a `kind: "tombstone"` node if a client wants soft-deletion semantics.

### Resources

memgraph exposes graphs and lineages as MCP resources so clients can browse and reference them with URIs like `memgraph://<graph_id>/<lineage_id>`.

### Prompts

Out of scope for v1. The reference client (or the L2 doc plugin) will ship prompts.

---

## 9. Extensibility seams

memgraph is built to be extended without forking. The seams:

### 9.1 Derived indexes (for v1.1 vector support)

The `Store` interface emits `onNodeWritten` / `onEdgeWritten` events. A derived-index module subscribes, computes whatever it wants (embedding vectors, custom inverted indexes, summary caches), and exposes a query method that returns lineage IDs back to the core.

**v1.1 vector index plan:** ship a `VectorIndex` module that listens for node writes, computes embeddings via a pluggable provider (OpenAI, local model, etc.), stores them in a separate table (or external vector DB), and exposes `vector_search(graph_id, query, k)` returning lineage IDs. The graph remains canonical; the vector index is rebuildable from the graph at any time.

### 9.2 Custom node/edge kinds

`kind` is a free string with no reserved values in memgraph core. Clients invent their own. Optional `kind_whitelist` per graph lets administrators enforce a schema if they want; the default is permissive.

### 9.3 Conflict policies

`lww` and `manual` ship in v1. Future policies (`agent_merge`, `crdt_set`, etc.) plug in via a `ConflictResolver` interface.

### 9.4 Auth wrappers

Because memgraph itself doesn't authenticate, the natural extension is a middleware MCP server that wraps memgraph's MCP, validates tokens, and rewrites tool calls to enforce permissions. We may ship a reference `memgraph-auth` middleware separately (not in v1).

---

## 10. Scope cuts

### v1.0 (L1 — substrate)

- Node + edge + graph data model
- Lineage + versioning + freshness
- Symlinks (cross-graph edges within a single deployment)
- Conflict resolution: `lww` and `manual`, configurable per graph
- `Store` interface + `SqliteStore` (local) + `PostgresStore` (cloud) reference impls
- MCP server with the tools listed in §8
- CLI for graph admin (create graph, dump, restore, migrate)
- Migration utility from `kb` SQLite DBs into a memgraph graph

### v1.1

- Vector index module (derived-index pattern from §9.1)
- Cross-deployment symlink stubs (remote graph references)
- `agent_merge` conflict policy

### L2 (separate tool, after v1.1)

- **`memgraph-docs`** — a CLI/plugin that ingests markdown into a memgraph graph as ordered `segment` nodes (with `child_of` edges and `ordinal` for order), and reconstructs documents by traversing from a root segment. This validates the "atoms compose into renderable docs" thesis.

### Out of scope indefinitely

- Built-in authentication, authorization, or user management
- Web UI / admin console
- Multi-region replication (delegate to the underlying store)
- Encryption at rest (delegate to the underlying store)

---

## 11. Relationship to `kb`

`kb` is a TypeScript single-table SQLite + FTS5 + MCP knowledge base with atomic markdown notes, tags, and free-form links. memgraph reimplements the surface in Go and generalizes the model:

| `kb` concept                  | memgraph equivalent                                              |
| ----------------------------- | ---------------------------------------------------------------- |
| Note                          | Node with `kind: "fact"`                                         |
| `title`, `body`               | `summary`, `content`                                             |
| `tags`                        | `tags` (unchanged)                                               |
| `source`, `context`, `workspace` | `metadata` JSON (or first-class tags depending on client)     |
| `links` (JSON field on note)  | First-class `Edge` rows; `kb_backlinks` becomes `incoming()`     |
| Multiple KBs (`kb_name`)      | Multiple **graphs** with cross-graph **symlinks**                |
| `workspace`                   | Graph-level isolation                                            |
| `kb_distill` (markdown → atoms) | The L2 `memgraph-docs` plugin (a separate tool)               |
| FTS5 search                   | `memgraph_search` (FTS5 in `SqliteStore`, `pg_trgm` in `PostgresStore`) |
| Versioning                    | Net-new (lineage + version chain)                                |
| Freshness                     | Net-new                                                          |
| Edges as first-class          | Net-new                                                          |

A migration tool in v1 (a `memgraph migrate kb` subcommand) reads a `kb` SQLite DB directly and emits one memgraph graph: notes become `fact` nodes (with `content = body`, `summary = title`), the `links` field becomes real `cites`-kind edges, and `workspace` becomes a single graph per workspace value (so a `kb` with three workspaces yields three memgraph graphs). Since the source is just SQLite, the cross-language boundary is trivial.

---

## 12. Open questions

1. **Language/runtime.** ✅ Resolved: **Go**. Single static binary, trivial cross-compile (`GOOS=linux GOARCH=arm64 go build`), no runtime to ship with releases. Sub-choices to make during scaffolding:
   - **SQLite driver:** `modernc.org/sqlite` (pure Go, cgo-free, slower) vs. `mattn/go-sqlite3` (cgo, faster). Default to `modernc.org/sqlite` for distribution friendliness; expose a build tag for users who want the cgo build.
   - **Postgres driver:** `pgx` (preferred for Postgres-specific features and perf) vs. `database/sql` + `lib/pq`. **Lean: `pgx`.**
   - **MCP SDK:** the official Go MCP SDK (when stable) vs. `mark3labs/mcp-go`. Pick during scaffolding based on current state.
   - **CLI framework:** `cobra` for the `memgraph` command. Standard, well-known.
2. **Symlink resolution depth.** Should `traverse()` automatically follow symlinks across graphs, or require an explicit `follow_symlinks: true`? Tradeoff: convenience vs. predictable cost. **Lean: explicit opt-in, default false.**
3. **Tombstones.** v1 says nodes aren't deletable; clients use a `tombstone` kind to soft-delete. Is that good enough, or do we need a real `purge` operation for compliance/GDPR? Probably need `purge` eventually but not v1.
4. **Symlink permissions.** With no auth in memgraph, can any graph in the deployment symlink to any other graph? Yes — and that's the consumer's problem to police. Worth documenting prominently so people don't expect isolation.
5. **`metadata` schema enforcement.** Free JSON is flexible but a recipe for mess at scale. Do we want optional per-graph JSON schemas (validated at write)? Probably v1.1+.
6. **Snapshots and backups.** Delegate to the backing store (sqlite copy, pg_dump) or ship a memgraph-native export? **Lean: delegate in v1, add native export in v1.1.**
7. **Telemetry.** memgraph should be quiet by default — no phoning home. Optional, opt-in operator-side telemetry for self-hosters who want it. Out of v1 scope.

---

## 13. Glossary

- **Node:** Atomic unit of information. Immutable once written.
- **Edge:** Directed, typed relationship between two lineages. May cross graph boundaries (symlink).
- **Graph:** Unit of isolation. A deployment hosts N graphs.
- **Lineage:** The conceptual "thing" — a stable identifier shared by all versions of a node.
- **Version:** A specific revision of a lineage. Immutable.
- **Symlink:** An edge whose target lives in a different graph than the edge itself.
- **Freshness:** A node's declared "valid until" timestamp. Independent of versioning.
- **Provenance:** Opaque `created_by` string. memgraph stores but does not interpret.
- **Vault (informal):** A consumer-side concept — typically maps to a memgraph graph plus client-side ACLs. Not a memgraph primitive.
