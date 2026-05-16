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
	"syscall"

	"github.com/spf13/cobra"

	memgraph "github.com/camggould/memgraph"
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
	cmd.AddCommand(&cobra.Command{
		Use:   "kb [path]",
		Short: "Import a camggould/kb SQLite database into a new graph",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return fmt.Errorf("migrate kb: %w", memgraph.ErrNotImplemented)
		},
	})
	return cmd
}

// --- helpers ---

func openSqlite(path string) (memgraph.Store, error) {
	store, err := sqlite.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open sqlite store at %s: %w", path, err)
	}
	return store, nil
}
