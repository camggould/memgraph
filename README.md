# memgraph

A versioned, multi-graph knowledge substrate for agents and teams. Exposed as a Model Context Protocol (MCP) server. Runs locally or in the cloud.

> **Status:** Alpha (v0.1.0). The data model and MCP surface are stable enough to build against, but expect breaking changes before v1.0. See [PRD.md](PRD.md) for the full design and roadmap.

memgraph is an open-source agentic persistence layer. It generalizes the single-table KB pattern of [`camggould/kb`](https://github.com/camggould/kb) into a real graph: first-class typed edges, versioned lineages, freshness, cross-graph symlinks, and a pluggable storage layer.

It is **a persistence layer, not an application**. memgraph is opinionated about the data model and agnostic about who uses it — authentication, authorization, UI, and ingestion all live in clients above this layer.

## Why

LLM agents need memory that survives sessions, scales beyond a context window, and is shareable across teammates and tools. Existing options force a choice between toy local stores (no concurrency, no audit) and heavyweight graph databases (no MCP, no agent-native API, no versioning model for evolving facts). memgraph aims to be the small, sharp tool in between.

## Features (v0.1.0)

- **Graphs as units of isolation** — a deployment hosts N graphs; cross-graph references are first-class symlinks.
- **Versioned lineages** — every node has a stable `lineage_id`; edits append immutable versions. Default reads always return the current version; history is preserved for audit.
- **Configurable conflict policy** — `lww` (default) or `manual` (concurrent writes produce sibling heads that require explicit resolution). Per-graph.
- **Freshness flags** — nodes carry an optional `freshness_at` date; reads surface `is_stale` so clients can warn users about decay.
- **FTS5 (SQLite) and pg_trgm (Postgres) search** — graph-scoped, ranked, with snippet output.
- **MCP server** with 10 tools and `memgraph://` resources; live `resources/list_changed` notifications when graphs are created.
- **`memgraph migrate kb`** — idempotent migration from a `camggould/kb` SQLite database.
- **Two reference storage backends** — `SQLiteStore` (cgo-free, single-binary, local) and `PostgresStore` (cloud, multi-process). Pluggable: implement `memgraph.Store` against any backend.

## Install

### `go install` (easiest, works today)

```sh
go install github.com/camggould/memgraph/cmd/memgraph@latest
```

Requires Go 1.25+. The binary will land in `$(go env GOBIN)` (or `$GOPATH/bin`); add that to your `PATH`.

### Build from source

```sh
git clone https://github.com/camggould/memgraph.git
cd memgraph
go build -o memgraph ./cmd/memgraph
```

The default build is fully pure-Go (no cgo). Cross-compile for any supported target:

```sh
GOOS=linux GOARCH=amd64 go build -o memgraph-linux-amd64 ./cmd/memgraph
GOOS=darwin GOARCH=arm64 go build -o memgraph-darwin-arm64 ./cmd/memgraph
GOOS=windows GOARCH=amd64 go build -o memgraph-windows-amd64.exe ./cmd/memgraph
```

### Pre-built binaries

Prebuilt releases will appear on the [Releases page](https://github.com/camggould/memgraph/releases) once cut. A `.goreleaser.yaml` config is in the repo; run `goreleaser release --clean` from a maintainer machine with a `GITHUB_TOKEN`.

## Quickstart

memgraph runs as an MCP server over stdio. The most common pattern: wire it into a Claude Code (or any MCP-compatible) client and let an agent read and write memory.

### 1. Initialize a store and run the server

The SQLite file is created on first run; no separate init step needed.

```sh
memgraph serve --sqlite ~/.memgraph/store.db
```

Press Ctrl-C to stop. The store persists across runs.

### 2. Wire it into an MCP client

Add memgraph to your MCP client config. The exact format depends on the client; the generic block looks like:

```json
{
  "mcpServers": {
    "memgraph": {
      "command": "memgraph",
      "args": ["serve", "--sqlite", "/Users/you/.memgraph/store.db"]
    }
  }
}
```

For Claude Code specifically:

```sh
claude mcp add memgraph -- memgraph serve --sqlite ~/.memgraph/store.db
```

### 3. Have your agent use it

Once connected, the agent can call any of the [MCP tools](#mcp-tools). A typical first interaction:

```
> Remember that I prefer tabs over spaces in Go projects.

[agent calls memgraph_create_graph(name="prefs") then memgraph_put_node(...)]

> What do I prefer for Go formatting?

[agent calls memgraph_search(graph_id="...", text="go formatting") and recalls the fact]
```

## CLI reference

```
memgraph --help

Commands:
  serve         Run the memgraph MCP server
  graph         Manage graphs in a memgraph deployment
    create      Create a new graph
    list        List graphs
  migrate       Migrate data from other systems
    kb          Import a camggould/kb SQLite database
  completion    Generate shell autocompletion
  help          Help about any command
```

### `memgraph serve`

Runs the MCP server on stdio.

```
--sqlite string   Path to SQLite store file (default "memgraph.db")
```

### `memgraph migrate kb <kb-db-path>`

Imports a `camggould/kb` SQLite database into a memgraph SQLite store. Idempotent: re-running only migrates net-new content.

```
--sqlite string   Path to target memgraph SQLite store (default "memgraph.db")
--dry-run         Validate source and report what would be migrated without writing
```

Mapping (see [PRD §11](PRD.md#11-relationship-to-kb)):

- Each kb note → memgraph node with `kind=fact` (`content=body`, `summary=title`).
- kb tags → memgraph `Tags`.
- kb `source`, `context`, `workspace`, `file_path`, `created`, `modified` → preserved in metadata under `kb_*` keys.
- kb workspaces → memgraph graphs (one per distinct value; NULL/empty → `default`).
- kb `links` → first-class `cites` edges; cross-workspace links become cross-graph symlinks.

## Data model in one screen

- **Node** — atomic, immutable, versioned. Fields: `id`, `lineage_id`, `version`, `kind`, `content`, `summary`, `tags`, `metadata`, `freshness_at`, `created_at`, `created_by`, `superseded_by`, `conflicts`. Clients pick their own `kind` ontology.
- **Edge** — directed, typed, ordered. Targets `lineage_id` (not `version`), so references survive edits. If `to_graph != graph_id`, the edge is a symlink across graph boundaries.
- **Graph** — unit of isolation. A deployment hosts N graphs; cross-graph symlinks are explicit edges.
- **Lineage** — the conceptual "thing". Multiple versions share one `lineage_id`. Default reads resolve to the current (non-superseded) version.

See [PRD §4](PRD.md#4-conceptual-model) for the full data model.

## MCP tools

**Read:**

| Tool | Purpose |
|---|---|
| `memgraph_list_graphs` | List all graphs with symlink-manifest counts |
| `memgraph_get_node` | Fetch by `node_id` or `lineage_id`; flags `is_current`, `is_stale` |
| `memgraph_history` | All versions of a lineage, newest first |
| `memgraph_traverse` | BFS from a lineage, with depth/kind filters and optional symlink-follow |
| `memgraph_search` | FTS over a graph; filters by kind / tag / freshness |
| `memgraph_symlink_manifest` | Cross-graph reference inventory |

**Write:**

| Tool | Purpose |
|---|---|
| `memgraph_create_graph` | Create a graph with optional conflict policy / kind whitelist |
| `memgraph_put_node` | Create a new lineage or append a version (optimistic via `based_on_version`) |
| `memgraph_put_edge` | Create an intra-graph edge or cross-graph symlink |
| `memgraph_delete_edge` | Remove an edge |

**Resources:**

- `memgraph://<graph_id>` — graph summary
- `memgraph://<graph_id>/<lineage_id>` — current node payload

## Storage backends

### SQLite (default, local)

Single file, pure-Go (`modernc.org/sqlite`), cgo-free. Ideal for single-process / single-user deployments.

```sh
memgraph serve --sqlite ~/.memgraph/store.db
```

### Postgres (cloud, multi-process)

For team deployments and concurrent writers. memgraph uses `pgx/v5` with `pg_trgm` (with `tsvector` fallback) for full-text. Cross-process serialization is provided by `SELECT ... FOR UPDATE` on lineage heads inside `PutNode`.

The CLI doesn't yet expose Postgres directly; for now, embed memgraph as a Go library:

```go
import (
    "github.com/camggould/memgraph/mcp"
    "github.com/camggould/memgraph/store/postgres"
)

store, err := postgres.OpenContext(ctx, "postgres://user:pw@host:5432/db")
if err != nil { /* ... */ }
defer store.Close()

if err := mcp.New(store).Serve(ctx); err != nil { /* ... */ }
```

A `--postgres` flag on `memgraph serve` is planned for the next release.

### Custom backends

Implement `memgraph.Store` and you have a backend. The compile-time interface assertion (`var _ memgraph.Store = (*MyStore)(nil)`) catches drift. The reference SQLite and Postgres implementations are good models — both are ~1000 LoC.

## Development

```sh
git clone https://github.com/camggould/memgraph.git
cd memgraph
go test -race ./...
```

### Running Postgres tests

The Postgres tests require a reachable Postgres instance. Set `MEMGRAPH_POSTGRES_DSN`:

```sh
MEMGRAPH_POSTGRES_DSN='postgres://you@localhost:5432/postgres' go test ./store/postgres/
```

If unset, the tests fall back to libpq defaults (`PGHOST`, `PGUSER`, socket search). If neither resolves a connection, the tests `t.Skip`.

Quick local setup:

```sh
brew services start postgresql@16
createdb postgres
go test ./store/postgres/  # uses defaults
```

### Layout

```
memgraph/
├── PRD.md                  # full product spec
├── doc.go, types.go, store.go, ids.go, errors.go   # public package
├── cmd/memgraph/           # CLI entrypoint (cobra)
├── mcp/                    # MCP server (modelcontextprotocol/go-sdk)
├── store/
│   ├── sqlite/             # local backend (modernc.org/sqlite)
│   └── postgres/           # cloud backend (pgx/v5)
└── internal/kbmigrate/     # kb -> memgraph migration
```

## License

MIT. See [LICENSE](LICENSE).
