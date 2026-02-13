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

type Import struct {
	DB        *pgx.Conn
	RootTable string
	Truncate  bool
	Upsert    bool
	OutDir    string

	DryRun       bool
	Verbose      bool
	NoAnimations bool
}

// Run the import
//   - Loads schema from a previous export
//   - Builds a dependency graph to determine import order
//   - Optionally truncates tables before importing
//   - Uses COPY FROM to import CSV files in the correct order
func (i *Import) Run(ctx context.Context) error {
	t0 := time.Now()

	schema := &Schema{}
	err := FromJSONFile(path.Join(i.OutDir, "schema.json"), schema)
	if err != nil {
		return fmt.Errorf("load graph: %w", err)
	}
	slog.Debug("Loaded schema from json, saved to: schema.json")

	graph, err := buildGraph(schema, i.RootTable)
	if err != nil {
		return fmt.Errorf("build graph: %w", err)
	}
	err = SaveAsJSONFile(graph, path.Join(i.OutDir, "graph.json"))
	if err != nil {
		return fmt.Errorf("save graph: %w", err)
	}
	slog.Debug("Import graph calculated, saved to: graph.json")

	graphPrinter := &GraphPrinter{
		g: graph,
	}
	if !i.Verbose && !i.NoAnimations {
		graphPrinter.Init(os.Stdout)
	} else {
		graph.Print()
	}

	queries := generateImportQueries(graph, schema)

	err = SaveAsJSONFile(queries, path.Join(i.OutDir, "import_queries.json"))
	if err != nil {
		return fmt.Errorf("save queries: %w", err)
	}

	// Validate upsert: all tables must have primary keys
	if i.Upsert {
		for _, tq := range queries {
			if tq.Upsert == "" {
				return fmt.Errorf("upsert requested but table %s has no primary key or unique constraint", tq.Table)
			}
		}
	}

	if i.DryRun {
		slog.Info("Dry run, not executing queries")

		fmt.Println()
		for _, tq := range queries {
			if i.Truncate {
				fmt.Println(tq.Truncate)
			}
			if i.Upsert {
				fmt.Println(tq.CreateTemp)
				fmt.Println(tq.CopyTemp)
				fmt.Println(tq.Upsert)
				fmt.Println(tq.DropTemp)
			} else {
				fmt.Println(tq.Copy)
			}
		}
		fmt.Println()

		slog.Info("Dry run complete")
		return nil
	}

	slog.Info("Importing...")

	for _, tq := range queries {
		if i.Truncate {
			slog.Debug(tq.Truncate)
			_, err := i.DB.Exec(ctx, tq.Truncate)
			if err != nil {
				return fmt.Errorf("truncate table: %w", err)
			}
			if i.Verbose || i.NoAnimations {
				slog.Info("Truncated table: " + tq.Table)
			}
		}

		if i.Upsert {
			// Create temp table
			slog.Debug(tq.CreateTemp)
			_, err := i.DB.Exec(ctx, tq.CreateTemp)
			if err != nil {
				return fmt.Errorf("create temp table for %s: %w", tq.Table, err)
			}

			// COPY into temp table
			slog.Debug(tq.CopyTemp)
			res, err := copyFromCSV(ctx, i.DB, tq.Table, tq.CopyTemp, i.OutDir)
			if err != nil {
				return fmt.Errorf("copy from csv into temp table: %w", err)
			}

			// Upsert from temp into target
			slog.Debug(tq.Upsert)
			_, err = i.DB.Exec(ctx, tq.Upsert)
			if err != nil {
				return fmt.Errorf("upsert from temp table for %s: %w", tq.Table, err)
			}

			// Drop temp table
			slog.Debug(tq.DropTemp)
			_, err = i.DB.Exec(ctx, tq.DropTemp)
			if err != nil {
				return fmt.Errorf("drop temp table for %s: %w", tq.Table, err)
			}

			if i.Verbose || i.NoAnimations {
				slog.Info("Upserted: "+tq.Table,
					"rows", prettyCount(res.Rows),
					"duration", prettyDuration(res.Duration),
					"file size", prettyFileSize(res.FileSize),
				)
			}
		} else {
			slog.Debug(tq.Copy)

			res, err := copyFromCSV(ctx, i.DB, tq.Table, tq.Copy, i.OutDir)
			if err != nil {
				return fmt.Errorf("copy from csv: %w", err)
			}

			if i.Verbose || i.NoAnimations {
				slog.Info("Imported CSV: "+tq.Table,
					"rows", prettyCount(res.Rows),
					"duration", prettyDuration(res.Duration),
					"file size", prettyFileSize(res.FileSize),
				)
			}
		}
	}

	slog.Info("Import complete", "duration", prettyDuration(time.Since(t0)))

	return nil
}
