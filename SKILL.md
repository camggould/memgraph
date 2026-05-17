---
name: memgraph
description: Use this skill whenever interacting with a memgraph MCP server — the agentic persistence layer that stores knowledge as versioned graph nodes. Covers when to create/read/write nodes and edges, how to structure them well (atomicity, kinds, tags, metadata, freshness), how to link nodes via typed edges, when to start a new graph vs reuse one, how cross-graph symlinks work, optimistic-concurrency writes with based_on_version, conflict policies, and the decision between memgraph_search and memgraph_traverse. Invoke any time tools named memgraph_* are available and you're being asked to remember, recall, link, version, or organize knowledge.
---

# memgraph — agent guide

memgraph is a versioned multi-graph knowledge substrate. You write atomic facts as **nodes**, link them with typed **edges**, isolate them into **graphs**, and rely on **lineages** to evolve facts over time without breaking references.

This guide tells you how to use it well. The first principle is: **structure matters more than volume**. A small graph of well-shaped nodes is more useful than a large pile of dump-everything content.

## The mental model in 60 seconds

- **Node** — one atomic unit of information. Has a `kind` (you pick the ontology), `content` (the payload), optional `summary`, `tags`, `metadata`, and `freshness_at`.
- **Lineage** — the *thing* a node is about, identified by `lineage_id`. When you "update" a node, you append a new version with the same lineage_id; default reads always resolve to the current version.
- **Edge** — directed, typed relationship between two lineages. Edges target lineages (not specific versions), so links survive edits.
- **Graph** — a unit of isolation. A deployment hosts many graphs; edges between graphs are first-class **symlinks**.

Available tools: `memgraph_list_graphs`, `memgraph_create_graph`, `memgraph_get_node`, `memgraph_history`, `memgraph_traverse`, `memgraph_search`, `memgraph_symlink_manifest`, `memgraph_put_node`, `memgraph_put_edge`, `memgraph_delete_edge`. Plus `memgraph://` resources for browsing.

---

## Structuring nodes well

### Be atomic

**One fact per node.** A node should express a single, self-contained idea that someone could quote standalone. If you'd put a paragraph break in the middle of your content, it's two nodes.

Good:
- `content`: "JWT tokens expire after 1 hour by default in production."
- `content`: "The token expiration is configured via env var `JWT_TTL_SECONDS`."

Bad:
- `content`: "JWTs expire after 1 hour. This is set via JWT_TTL_SECONDS. The team agreed to this in Q2 2025 because of session-hijack concerns. See ADR-014 for the full context. Note: this doesn't apply to refresh tokens..."

Why: atomic nodes can be linked, versioned, and retrieved independently. A wall of text can only be retrieved whole. If you have a wall of text, ingest it through `memgraph-docs` (which splits it into structured nodes) rather than as a single fact.

### Pick a `kind` deliberately

`kind` is a free string; memgraph doesn't enforce values. **Use a consistent, semantic ontology across writes** so future search and filtering work. Examples:

- `fact` — declarative knowledge ("X is Y")
- `decision` — a choice that was made and why
- `preference` — a user/team preference
- `event` — something that happened at a time
- `entity:person`, `entity:project`, `entity:service` — first-class named things
- `observation` — empirical signal (metric, log, anecdote)
- `procedure` — how-to / runbook step

If you're inventing a kind on the fly, check `memgraph_list_graphs` for existing graphs and use `memgraph_search` with no text + a `kinds` filter to see what's already in use. Match the local vocabulary.

### Use `summary` only when content is long

`summary` exists for progressive disclosure during traversal. Clients show it first; full `content` is fetched on demand.

- If `content` is short (under ~80 chars), **leave summary empty.** Clients fall back to content.
- If `content` is long, write a tight one-line `summary` (no period, no preamble).

Don't duplicate content into summary verbatim. That bloats the store with no benefit.

### Tag for filtering, not for prose

Tags are flat keywords used for filtering (`memgraph_search` accepts a `tags` filter). They are exact-match — `bar` does not match `barb`. Conventions that work:

- Lowercase, hyphen-separated for multi-word: `auth-service`, `q2-roadmap`
- Limit to ~5 tags per node; more dilutes signal
- Reuse existing tags rather than inventing parallel ones

### Use `metadata` for structured side data

`metadata` is freeform JSON that memgraph stores but never interprets. Use it for things that aren't searchable text:

- Source attribution: `{"url": "...", "doc_id": "..."}`
- Numeric or structured facts: `{"latency_ms": 230, "p99": 410}`
- Client-specific fields: `{"author_email": "...", "ticket": "JIRA-123"}`

Don't put searchable prose in metadata — it won't appear in `memgraph_search` results.

### Use `freshness_at` for time-sensitive facts

If a fact has a known expiration ("this is the deploy schedule for Q2"), set `freshness_at` to when the fact should be re-validated. On read, memgraph surfaces an `is_stale` flag; you can warn the user.

Skip `freshness_at` for evergreen facts ("our database is Postgres").

### Use `created_by` to attribute writes

`created_by` is an opaque string memgraph stores but never interprets. Stuff it with whatever makes sense for your context:

- `"agent:claude-opus-4-7"` — agent provenance
- `"user:alice"` — user-driven writes
- `"ingest:slack-bot"` — automated ingestion

Setting this consistently makes audits (via `memgraph_history`) meaningful.

---

## Linking nodes with edges

Edges are how knowledge becomes a graph rather than a pile. **Whenever you write a node, ask: what other nodes does this relate to?**

### Pick a meaningful edge `kind`

Edge `kind` is also a free string. Use semantic verbs. Examples:

| Edge kind | When to use |
|---|---|
| `cites` | This node references another (e.g. fact based on document) |
| `supports` | This node provides evidence for another |
| `contradicts` | This node disagrees with another (preserve both!) |
| `supersedes` | This node deprecates an old fact (don't use — use lineage versioning instead) |
| `depends_on` | Causal / requirement relationship |
| `part_of` | Compositional (paragraph part_of doc, service part_of system) |
| `mentions` | This node refers to a named entity |
| `derived_from` | This fact was computed/inferred from another |
| `related_to` | Soft association when nothing more specific fits |

Prefer specific kinds over `related_to`. A graph full of `related_to` edges is hard to query meaningfully.

### Edges target lineages, not versions

When you create an edge with `memgraph_put_edge`, the `to_lineage` you pass is a `lineage_id`. The edge automatically tracks the current version. Edits to either end don't break the link.

### Use `ordinal` for ordered relationships

`ordinal` is an integer on the edge. Use it when sibling order matters: list items, document children, ranked alternatives, timeline events. Leave it empty when order doesn't matter.

If you're appending children, assign `ordinal = (max existing ordinal) + 1` so order stays append-friendly.

### Don't use edges as a fact substitute

A common mistake: writing a node "Alice" and a node "Bob" with a `married_to` edge between them, expecting search to find "Alice and Bob are married". Search doesn't traverse edges. Either:

- Write an explicit fact node: `kind: "fact"`, `content: "Alice and Bob are married since 2020."`, plus `mentions` edges to Alice and Bob — search hits the fact, traverse reveals the relationships.
- Or accept that the relationship is a structure-only signal, discoverable via `memgraph_traverse` but not `memgraph_search`.

Use both patterns together: facts say *what*, edges express *how things connect*.

---

## Graphs: when to create vs reuse

Graphs are isolation boundaries. Symlinks connect them, but the default is no leakage.

### Create a new graph when

- It's a logically separate domain (`personal-notes`, `work-projects`, `customer-feedback`)
- It has different stakeholders or sensitivity
- It needs a different conflict policy (e.g. `manual` for sensitive collaborative facts, `lww` for personal scratch)
- The size justifies isolation (a million-node graph is slower to enumerate than a partitioned one)

### Reuse a graph when

- Content is conceptually part of the same domain
- You'd want search results to span the new content with existing content
- Sharing edges to existing nodes is valuable

**Before creating a new graph, always call `memgraph_list_graphs` to see what exists.** Names aren't unique but matching an existing name is a signal you want the existing graph.

### Use cross-graph symlinks for hand-offs

When a node in graph A logically references something in graph B, create an edge with `to_graph=B`. memgraph treats this as a symlink and tracks it in the manifest. Use cases:

- A `customer-feedback` graph references a `roadmap` graph item
- A personal note links to a team-shared decision

Symlinks don't bypass isolation; both graphs must be queried separately to see the relationship from each side.

---

## Versioning: when to update vs write a new node

memgraph has two distinct concepts here:

- **A new version of an existing lineage** — pass the existing `lineage_id` to `memgraph_put_node`. Old version is superseded; default reads return the new one. History is preserved.
- **A new lineage** — omit `lineage_id`. memgraph generates a fresh one. The new node has no relationship to anything else unless you add an edge.

### Update an existing lineage when

- The content is *the same fact* but the wording, detail, or accuracy improved
- A typo, date update, or factual correction
- The freshness expired and you have updated info

### Write a new lineage when

- It's a different fact, even if related (use a `supersedes` or `derived_from` edge to link them if you want)
- The original fact is still true at a point in time and you want both to coexist as history (use edges for "as of X" semantics if needed)

### Use optimistic concurrency with `based_on_version`

If you fetched a node, plan to update it, and want to detect that someone else updated it in the meantime, pass `based_on_version` to `memgraph_put_node` set to the version you read.

- Under `lww` policy (default): your write wins regardless. `based_on_version` is informational.
- Under `manual` policy: if the version doesn't match, the write succeeds but is recorded as a sibling head; the response includes the conflicting sibling IDs in `conflicts`. Resolution = a third write that explicitly supersedes both siblings.

For agent flows, set `based_on_version` whenever you're updating a node you just read — it gives the user a chance to see conflicts.

---

## Search vs traverse: pick the right tool

These are different access patterns. Picking wrong wastes context.

### Use `memgraph_search` when

- You don't know the lineage ID; you have keywords
- You want ranked relevance over a graph
- You want full-text matching across content and tags
- The answer might be in many places

Search is graph-scoped. Cross-graph search means parallel queries, then merge.

### Use `memgraph_traverse` when

- You know a starting `lineage_id` and want its neighborhood
- You want to follow specific edge kinds (`cites`, `part_of`, etc.)
- You want hierarchical structure (e.g. all children of a section)
- You want to follow cross-graph symlinks (`follow_symlinks: true`)

Traverse is breadth-first; bound it with `max_depth` (default 2) and `max_nodes` (default 50) to keep context manageable.

**Direction matters.** As of memgraph v0.2, `memgraph_traverse` takes a `direction` parameter:

| `direction` | Walks | Use for |
|---|---|---|
| `outgoing` (default) | from-→to edges only | "What does this node cite / depend on / contain?" |
| `incoming` | to-→from edges only (backlinks) | "Who cites / depends on / mentions this node?" |
| `both` | edges in either direction | "Is this node connected to anything?" / ego-graph audits |

Most natural questions are outgoing-shaped. Use `incoming` for backlinks. Use `both` for connectivity audits or undirected ego graphs.

### Use `memgraph_get_node` when

- You have an exact lineage_id and want the full payload
- You want to fetch a specific historical version (`at_version` or `at_time`)

---

## Anti-patterns to avoid

- **Dumping prose into one node.** If it has multiple ideas, it's multiple nodes. Use `memgraph-docs` for structured documents.
- **Writing nodes without edges.** A graph without edges is a list. Always ask "what does this connect to?"
- **Inventing parallel `kind` values.** `Fact`, `fact`, `Atomic fact`, `note` are all the same thing — pick one and use it.
- **Putting big binary blobs in `content`.** Use a URL reference and `kind: media` (or use `memgraph-docs`).
- **Skipping `created_by`.** Provenance is cheap; audits are expensive without it.
- **Creating a new graph per session.** Graphs are durable; sessions aren't. New graphs should be reserved for new *domains*.
- **Re-running `memgraph migrate kb` after changing the kb file.** Migration is idempotent for *new* notes; it doesn't update changed notes. Manual updates for changes.

---

## Quick reference: writing a fact end-to-end

```
1. memgraph_list_graphs                     → confirm target graph or pick one to create
2. memgraph_search(graph_id, text=...)      → check if the fact already exists
3a. If exists with different content:
    memgraph_put_node(lineage_id=existing, content=updated, based_on_version=...)
3b. If exists exactly: do nothing
3c. If new:
    memgraph_put_node(graph_id, kind=..., content=..., summary=..., tags=[...], metadata={...}, created_by=...)
4. memgraph_put_edge(graph_id, from=new, to=existing-related-node, kind=cites|supports|...)
   — link the new fact to whatever it relates to
```

When in doubt: **read before writing**. Search the existing graph first. Be atomic. Always add at least one edge if a related node exists.
