// Command memgraph is the CLI entrypoint for the memgraph persistence layer.
//
// Subcommands (in scope for v1):
//
//	memgraph serve        — run the MCP server against a configured store
//	memgraph graph ...    — create / list / describe graphs
//	memgraph migrate kb   — import a camggould/kb SQLite DB into a memgraph graph
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/spf13/cobra"

	memgraph "github.com/camggould/memgraph"
	"github.com/camggould/memgraph/internal/kbmigrate"
	"github.com/camggould/memgraph/mcp"
	"github.com/camggould/memgraph/store/sqlite"
)

// Build-time injectable version. Override with:
//
//	go build -ldflags="-X main.version=$(git describe --tags --always)"
var version = "dev"

func main() {
	if err := newRootCmd().Execute(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func newRootCmd() *cobra.Command {
	root := &cobra.Command{
		Use:           "memgraph",
		Short:         "Versioned multi-graph knowledge substrate for agents and teams",
		SilenceUsage:  true,
		SilenceErrors: true,
		Version:       version,
	}

	root.AddCommand(
		newServeCmd(),
		newGraphCmd(),
		newMigrateCmd(),
	)
	return root
}

// --- serve ---

func newServeCmd() *cobra.Command {
	var sqlitePath string

	cmd := &cobra.Command{
		Use:   "serve",
		Short: "Run the memgraph MCP server",
		RunE: func(cmd *cobra.Command, args []string) error {
			store, err := openSqlite(sqlitePath)
			if err != nil {
				return err
			}
			defer store.Close()

			ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
			defer cancel()

			srv := mcp.New(store)
			return srv.Serve(ctx)
		},
	}
	cmd.Flags().StringVar(&sqlitePath, "sqlite", "memgraph.db", "Path to SQLite store file")
	return cmd
}

// --- graph ---

func newGraphCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "graph",
		Short: "Manage graphs in a memgraph deployment",
	}
	cmd.AddCommand(
		&cobra.Command{
			Use:   "create [name]",
			Short: "Create a new graph",
			Args:  cobra.ExactArgs(1),
			RunE: func(cmd *cobra.Command, args []string) error {
				return fmt.Errorf("graph create: %w", memgraph.ErrNotImplemented)
			},
		},
		&cobra.Command{
			Use:   "list",
			Short: "List graphs in the deployment",
			RunE: func(cmd *cobra.Command, args []string) error {
				return fmt.Errorf("graph list: %w", memgraph.ErrNotImplemented)
			},
		},
	)
	return cmd
}

// --- migrate ---

func newMigrateCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "migrate",
		Short: "Migrate data from other systems into memgraph",
	}
	cmd.AddCommand(newMigrateKBCmd())
	return cmd
}

func newMigrateKBCmd() *cobra.Command {
	var (
		sqlitePath string
		dryRun     bool
	)

	cmd := &cobra.Command{
		Use:   "kb <kb-db-path>",
		Short: "Import a camggould/kb SQLite database into memgraph",
		Long: `Import a camggould/kb SQLite database into memgraph.

Each kb note becomes a memgraph node with kind="fact" (content = body,
summary = title). kb workspaces become memgraph graphs (one per distinct
value; NULL/empty/whitespace workspaces fall into a "default" graph).
The note's links field is promoted to first-class "cites" edges; links
that cross workspaces become cross-graph symlinks.

Migration is idempotent: re-running picks up net-new notes/links and
leaves existing migrated content untouched. Content updates on existing
notes are NOT detected (planned for v1.2).`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			kbPath := args[0]

			ctx, cancel := signal.NotifyContext(cmd.Context(), os.Interrupt, syscall.SIGTERM)
			defer cancel()

			store, err := openSqlite(sqlitePath)
			if err != nil {
				return err
			}
			defer store.Close()

			report, err := kbmigrate.Migrate(ctx, kbPath, store, kbmigrate.Options{
				DryRun: dryRun,
			})
			if err != nil {
				return fmt.Errorf("migrate kb: %w", err)
			}
			absTarget, absErr := filepath.Abs(sqlitePath)
			if absErr != nil {
				absTarget = sqlitePath
			}
			report.TargetPath = absTarget

			printMigrateReport(cmd.OutOrStdout(), report)
			return nil
		},
	}
	cmd.Flags().StringVar(&sqlitePath, "sqlite", "memgraph.db", "Path to target memgraph SQLite store")
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "Validate source and report what would be migrated without writing")
	return cmd
}

func printMigrateReport(w interface{ Write(p []byte) (int, error) }, r kbmigrate.Report) {
	prefix := ""
	verb := "Migrated"
	if r.DryRun {
		prefix = "(dry-run) "
		verb = "Would migrate"
	}
	fmt.Fprintf(w, "%s%s kb -> memgraph\n", prefix, verb)
	fmt.Fprintf(w, "  source: %s\n", r.SourcePath)
	fmt.Fprintf(w, "  target: %s\n", r.TargetPath)

	reused := 0
	for _, g := range r.Graphs {
		if g.Reused {
			reused++
		}
	}
	newG := len(r.Graphs) - reused
	if r.DryRun {
		fmt.Fprintf(w, "  graphs: %d\n", len(r.Graphs))
	} else {
		fmt.Fprintf(w, "  graphs: %d (%d reused, %d new)\n", len(r.Graphs), reused, newG)
	}

	// Right-pad names for a stable visual column.
	maxName := 0
	for _, g := range r.Graphs {
		if len(g.Name) > maxName {
			maxName = len(g.Name)
		}
	}
	for _, g := range r.Graphs {
		pad := ""
		if diff := maxName - len(g.Name); diff > 0 {
			pad = padding(diff)
		}
		switch {
		case r.DryRun:
			fmt.Fprintf(w, "    - %s%s (%d nodes)\n", g.Name, pad, g.NodeCount)
		case g.NodesSkipped == 0:
			fmt.Fprintf(w, "    - %s%s (%d nodes: %d new)\n",
				g.Name, pad, g.NodeCount, g.NodesCreated)
		default:
			fmt.Fprintf(w, "    - %s%s (%d nodes: %d new, %d existing)\n",
				g.Name, pad, g.NodeCount, g.NodesCreated, g.NodesSkipped)
		}
	}
	if r.DryRun {
		fmt.Fprintf(w, "  edges would create: %d (cites)\n", r.EdgesCreated)
	} else {
		fmt.Fprintf(w, "  edges: %d created, %d skipped (already present)\n",
			r.EdgesCreated, r.EdgesSkipped)
	}
	fmt.Fprintf(w, "  skipped links: %d (missing targets)\n", len(r.SkippedLinks))
}

func padding(n int) string {
	if n <= 0 {
		return ""
	}
	b := make([]byte, n)
	for i := range b {
		b[i] = ' '
	}
	return string(b)
}

// --- helpers ---

func openSqlite(path string) (memgraph.Store, error) {
	store, err := sqlite.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open sqlite store at %s: %w", path, err)
	}
	return store, nil
}
