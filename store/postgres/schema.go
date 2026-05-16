package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// schemaStatements is applied idempotently on Open. Order matters: tables
// before their dependent indexes.
var schemaStatements = []string{
	`CREATE TABLE IF NOT EXISTS graphs (
		id              TEXT PRIMARY KEY,
		name            TEXT NOT NULL,
		conflict_policy TEXT NOT NULL DEFAULT 'lww',
		kind_whitelist  JSONB,
		metadata        JSONB,
		created_at      TIMESTAMPTZ NOT NULL
	)`,
	`CREATE TABLE IF NOT EXISTS nodes (
		id            TEXT PRIMARY KEY,
		graph_id      TEXT NOT NULL REFERENCES graphs(id),
		lineage_id    TEXT NOT NULL,
		version       INTEGER NOT NULL,
		kind          TEXT NOT NULL,
		content       TEXT NOT NULL,
		summary       TEXT,
		tags          JSONB,
		metadata      JSONB,
		freshness_at  TIMESTAMPTZ,
		created_at    TIMESTAMPTZ NOT NULL,
		created_by    TEXT NOT NULL,
		superseded_by TEXT,
		UNIQUE(lineage_id, version)
	)`,
	`CREATE INDEX IF NOT EXISTS idx_nodes_lineage_current ON nodes(lineage_id) WHERE superseded_by IS NULL`,
	`CREATE INDEX IF NOT EXISTS idx_nodes_graph_kind ON nodes(graph_id, kind)`,
	`CREATE INDEX IF NOT EXISTS idx_nodes_lineage ON nodes(lineage_id)`,
	`CREATE TABLE IF NOT EXISTS edges (
		id           TEXT PRIMARY KEY,
		graph_id     TEXT NOT NULL REFERENCES graphs(id),
		from_lineage TEXT NOT NULL,
		to_graph     TEXT NOT NULL,
		to_lineage   TEXT NOT NULL,
		kind         TEXT NOT NULL,
		metadata     JSONB,
		ordinal      INTEGER,
		created_at   TIMESTAMPTZ NOT NULL,
		created_by   TEXT NOT NULL
	)`,
	`CREATE INDEX IF NOT EXISTS idx_edges_from ON edges(from_lineage, kind)`,
	`CREATE INDEX IF NOT EXISTS idx_edges_to ON edges(to_lineage, kind)`,
	`CREATE INDEX IF NOT EXISTS idx_edges_graph_to ON edges(graph_id, to_graph)`,
}

// trgmIndexStatements are applied only when pg_trgm is available. They use
// the trigram operator class for fast similarity-based search.
var trgmIndexStatements = []string{
	`CREATE INDEX IF NOT EXISTS idx_nodes_content_trgm ON nodes USING GIN (content gin_trgm_ops)`,
	`CREATE INDEX IF NOT EXISTS idx_nodes_summary_trgm ON nodes USING GIN (summary gin_trgm_ops)`,
}

// tsvFallbackStatements create a tsvector-based fallback when pg_trgm is
// unavailable. Stored generated column keeps the index always up to date.
var tsvFallbackStatements = []string{
	`ALTER TABLE nodes ADD COLUMN IF NOT EXISTS search_tsv tsvector
		GENERATED ALWAYS AS (to_tsvector('simple', coalesce(content,'') || ' ' || coalesce(summary,''))) STORED`,
	`CREATE INDEX IF NOT EXISTS idx_nodes_search_tsv ON nodes USING GIN (search_tsv)`,
}

func applySchema(ctx context.Context, pool *pgxpool.Pool) (useTrgm bool, err error) {
	for _, stmt := range schemaStatements {
		if _, err := pool.Exec(ctx, stmt); err != nil {
			return false, fmt.Errorf("apply schema: %w", err)
		}
	}
	// Try pg_trgm; if available, prefer it.
	if _, err := pool.Exec(ctx, `CREATE EXTENSION IF NOT EXISTS pg_trgm`); err == nil {
		for _, stmt := range trgmIndexStatements {
			if _, err := pool.Exec(ctx, stmt); err != nil {
				return false, fmt.Errorf("apply trgm index: %w", err)
			}
		}
		return true, nil
	}
	// Fallback: tsvector.
	for _, stmt := range tsvFallbackStatements {
		if _, err := pool.Exec(ctx, stmt); err != nil {
			return false, fmt.Errorf("apply tsv fallback: %w", err)
		}
	}
	return false, nil
}
