package pg_mini

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path"
	"time"

	"github.com/jackc/pgx/v5"
)

type Export struct {
	DB        *pgx.Conn
	RootTable string
	Filter    string
	RawQuery  string
	OutDir    string

	DryRun       bool
	Verbose      bool
	NoAnimations bool
}

// Run the export
//   - Runs queries to understand your database schema
//   - Build a dependency graph of tables based on foreign key relationships (including transitive dependencies!)
//   - Provided with a root table an execution sequence is calculated to traverse the tree
//   - A set of queries are generated that copy data into temporary tables
//   - In the correct sequence (starting with the root table)
//   - Only including rows that are required to fulfil the foreign key relationships
//   - Queries are executed within a transaction for internal consistency
//   - COPY from commands are used to export these temp tables to CSV
func (e *Export) Run(ctx context.Context) error {
	t0 := time.Now()

	// Runs queries to understand your database schema
	schema, err := queryDBSchema(ctx, e.DB)
	if err != nil {
		return fmt.Errorf("get schema: %w", err)
	}
	err = SaveAsJSONFile(schema, path.Join(e.OutDir, "schema.json"))
	if err != nil {
		return fmt.Errorf("save graph: %w", err)
	}
	slog.Debug("Extracted schema from database, saved to: schema.json")

	// Build a dependency graph of tables based on foreign key relationships (including transitive dependencies!)
	// Provided with a root table an execution sequence is calculated to traverse the tree
	graph, err := buildGraph(schema, e.RootTable)
	if err != nil {
		return fmt.Errorf("build graph: %w", err)
	}
	err = SaveAsJSONFile(graph, path.Join(e.OutDir, "export_graph.json"))
	if err != nil {
		return fmt.Errorf("save graph: %w", err)
	}
	slog.Debug("Export graph calculated, saved to: export_graph.json")

	graphPrinter := &GraphPrinter{
		g: graph,
	}
	if !e.Verbose && !e.NoAnimations {
		graphPrinter.Init(os.Stdout)
		graphPrinter.Render()
	} else {
		graph.Print()
	}

	if e.DryRun {
		slog.Info("Dry run, not executing queries")

		fmt.Println()
		for _, tbl := range graph.ExportOrder {
			for _, query := range tempCopyQueries(graph, tbl, e.Filter, e.RawQuery) {
				fmt.Println(query)
			}
		}
		fmt.Println()
		for _, tbl := range graph.ExportOrder {
			fmt.Println(copyToCSVQuery(tbl))
		}
		fmt.Println()

		slog.Info("Dry run complete")
		return nil
	}

	// A set of queries are generated that copy data into temporary tables
	// In the correct sequence (starting with the root table)
	// Only including rows that are required to fulfil the foreign key relationships
	// run temp copy queries in transaction for consistency
	if e.Verbose || e.NoAnimations {
		slog.Info("Begin transaction, copying data into temporary tables...")
	}
	tx, err := e.DB.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)
	for _, tbl := range graph.ExportOrder {
		graph.Tables[tbl].Status = statusCopyStarted
		graphPrinter.Render()

		t0 := time.Now()
		queries := tempCopyQueries(graph, tbl, e.Filter, e.RawQuery)
		var rows int64
		for _, query := range queries {
			slog.Debug(query)
			r, err := tx.Exec(ctx, query)
			if err != nil {
				return fmt.Errorf("execute query %s: %w", query, err)
			}
			slog.Debug(r.String())
			rows += r.RowsAffected()
		}

		graph.Tables[tbl].Status = statusCopyDone
		graph.Tables[tbl].Rows = rows
		graph.Tables[tbl].CopyDuration = time.Since(t0)
		if e.NoAnimations || e.Verbose {
			slog.Info("Copied temp table: "+tbl, "rows", prettyCount(rows),
				"duration", prettyDuration(graph.Tables[tbl].CopyDuration),
			)
		}
		graphPrinter.Render()
	}
	if e.Verbose || e.NoAnimations {
		slog.Info("Commit transaction. Copying complete")
	}
	err = tx.Commit(ctx)
	if err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}

	// COPY from commands are used to export these temp tables to CSV
	for _, tbl := range graph.ExportOrder {
		t0 := time.Now()

		graph.Tables[tbl].Status = statusCSVStarted
		graphPrinter.Render()

		query := copyToCSVQuery(tbl)
		slog.Debug(query)

		res, err := copyToCSV(ctx, e.DB, tbl, query, e.OutDir)
		if err != nil {
			return fmt.Errorf("copy out files: %w", err)
		}

		graph.Tables[tbl].Status = statusCSVDone
		graph.Tables[tbl].CSVSize = res.FileSize
		graph.Tables[tbl].CSVDuration = time.Since(t0)
		graphPrinter.Render()

		if e.NoAnimations || e.Verbose {
			slog.Info("Exported table: "+tbl,
				"file", res.FileName,
				"rows", prettyCount(res.Rows),
				"duration", prettyDuration(res.Duration),
				"file size", prettyFileSize(res.FileSize),
			)
		}
	}

	slog.Info("Export complete", "dir", e.OutDir, "total duration", prettyDuration(time.Since(t0)))

	return nil
}
