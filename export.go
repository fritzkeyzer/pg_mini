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
	err = SaveAsJSONFile(graph, path.Join(e.OutDir, "graph.json"))
	if err != nil {
		return fmt.Errorf("save graph: %w", err)
	}
	slog.Debug("Export graph calculated, saved to: graph.json")

	graphPrinter := &GraphPrinter{
		g: graph,
	}
	if !e.Verbose && !e.NoAnimations {
		graphPrinter.Init(os.Stdout)
		graphPrinter.Render()
	} else {
		graph.Print()
	}

	queries := generateExportQueries(graph, e.Filter, e.RawQuery)

	err = SaveAsJSONFile(queries, path.Join(e.OutDir, "export_queries.json"))
	if err != nil {
		return fmt.Errorf("save queries: %w", err)
	}

	if e.DryRun {
		slog.Info("Dry run, not executing queries")

		fmt.Println()
		for _, tq := range queries {
			fmt.Println(tq.CreateTmp)
			if tq.CreateIndex != "" {
				fmt.Println(tq.CreateIndex)
			}
		}
		fmt.Println()
		for _, tq := range queries {
			fmt.Println(tq.CopyToCSV)
		}
		fmt.Println()

		slog.Info("Dry run complete")
		return nil
	}

	// Execute temp copy queries in transaction for consistency
	if e.Verbose || e.NoAnimations {
		slog.Info("Begin transaction, copying data into temporary tables...")
	}
	tx, err := e.DB.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	for _, tq := range queries {
		graph.Tables[tq.Table].status = statusCopyStarted
		graphPrinter.Render()

		tblStart := time.Now()
		var rows int64

		slog.Debug(tq.CreateTmp)
		r, err := tx.Exec(ctx, tq.CreateTmp)
		if err != nil {
			return fmt.Errorf("execute query %s: %w", tq.CreateTmp, err)
		}
		slog.Debug(r.String())
		rows += r.RowsAffected()

		if tq.CreateIndex != "" {
			slog.Debug(tq.CreateIndex)
			r, err = tx.Exec(ctx, tq.CreateIndex)
			if err != nil {
				return fmt.Errorf("execute query %s: %w", tq.CreateIndex, err)
			}
			slog.Debug(r.String())
		}

		graph.Tables[tq.Table].status = statusCopyDone
		graph.Tables[tq.Table].rows = rows
		graph.Tables[tq.Table].copyDuration = time.Since(tblStart)
		if e.NoAnimations || e.Verbose {
			slog.Info("Copied temp table: "+tq.Table, "rows", prettyCount(rows),
				"duration", prettyDuration(graph.Tables[tq.Table].copyDuration),
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
	for _, tq := range queries {
		tblStart := time.Now()

		graph.Tables[tq.Table].status = statusCSVStarted
		graphPrinter.Render()

		slog.Debug(tq.CopyToCSV)

		res, err := copyToCSV(ctx, e.DB, tq.Table, tq.CopyToCSV, e.OutDir)
		if err != nil {
			return fmt.Errorf("copy out files: %w", err)
		}

		graph.Tables[tq.Table].status = statusCSVDone
		graph.Tables[tq.Table].csvSize = res.FileSize
		graph.Tables[tq.Table].csvDuration = time.Since(tblStart)
		graphPrinter.Render()

		if e.NoAnimations || e.Verbose {
			slog.Info("Exported table: "+tq.Table,
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
