package sqlite

import "database/sql"

// schemaSQL is applied idempotently on Open. Keep statements
// individually executable; modernc.org/sqlite tolerates multi-stmt
// Exec but splitting helps when triggers grow complex.
var schemaStatements = []string{
	`CREATE TABLE IF NOT EXISTS graphs (
		id              TEXT PRIMARY KEY,
		name            TEXT NOT NULL,
		conflict_policy TEXT NOT NULL DEFAULT 'lww',
		kind_whitelist  TEXT,
		metadata        TEXT,
		created_at      TEXT NOT NULL
	)`,
	`CREATE TABLE IF NOT EXISTS nodes (
		id            TEXT PRIMARY KEY,
		graph_id      TEXT NOT NULL REFERENCES graphs(id),
		lineage_id    TEXT NOT NULL,
		version       INTEGER NOT NULL,
		kind          TEXT NOT NULL,
		content       TEXT NOT NULL,
		summary       TEXT,
		tags          TEXT,
		metadata      TEXT,
		freshness_at  TEXT,
		created_at    TEXT NOT NULL,
		created_by    TEXT NOT NULL,
		superseded_by TEXT,
		UNIQUE(lineage_id, version)
	)`,
	`CREATE INDEX IF NOT EXISTS idx_nodes_lineage_current ON nodes(lineage_id) WHERE superseded_by IS NULL`,
	`CREATE INDEX IF NOT EXISTS idx_nodes_graph_kind ON nodes(graph_id, kind)`,
	`CREATE TABLE IF NOT EXISTS edges (
		id           TEXT PRIMARY KEY,
		graph_id     TEXT NOT NULL REFERENCES graphs(id),
		from_lineage TEXT NOT NULL,
		to_graph     TEXT NOT NULL,
		to_lineage   TEXT NOT NULL,
		kind         TEXT NOT NULL,
		metadata     TEXT,
		ordinal      INTEGER,
		created_at   TEXT NOT NULL,
		created_by   TEXT NOT NULL
	)`,
	`CREATE INDEX IF NOT EXISTS idx_edges_from ON edges(from_lineage, kind)`,
	`CREATE INDEX IF NOT EXISTS idx_edges_to ON edges(to_lineage, kind)`,
	`CREATE INDEX IF NOT EXISTS idx_edges_graph_to ON edges(graph_id, to_graph)`,
	// FTS5 external-content table over nodes. We do NOT use automatic
	// triggers because we only want CURRENT versions indexed; the Store
	// manages inserts/deletes inline with PutNode in the same tx.
	`CREATE VIRTUAL TABLE IF NOT EXISTS nodes_fts USING fts5(
		content, summary, tags,
		node_id UNINDEXED,
		tokenize = 'porter unicode61'
	)`,
}

func applySchema(db *sql.DB) error {
	if _, err := db.Exec(`PRAGMA foreign_keys = ON`); err != nil {
		return err
	}
	if _, err := db.Exec(`PRAGMA journal_mode = WAL`); err != nil {
		return err
	}
	for _, stmt := range schemaStatements {
		if _, err := db.Exec(stmt); err != nil {
			return err
		}
	}
	return nil
}
