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
	err = SaveAsJSONFile(graph, path.Join(i.OutDir, "import_graph.json"))
	if err != nil {
		return fmt.Errorf("save graph: %w", err)
	}
	slog.Debug("Import graph calculated, saved to: import_graph.json")

	graphPrinter := &GraphPrinter{
		g: graph,
	}
	if !i.Verbose && !i.NoAnimations {
		graphPrinter.Init(os.Stdout)
	} else {
		graph.Print()
	}

	queries := generateImportQueries(graph)

	if i.DryRun {
		slog.Info("Dry run, not executing queries")

		err = SaveAsJSONFile(queries, path.Join(i.OutDir, "queries.json"))
		if err != nil {
			return fmt.Errorf("save queries: %w", err)
		}

		fmt.Println()
		for _, tq := range queries {
			if i.Truncate {
				fmt.Println(tq.Truncate)
			}
			fmt.Println(tq.CopyFromCSV)
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

		slog.Debug(tq.CopyFromCSV)

		res, err := copyFromCSV(ctx, i.DB, tq.Table, tq.CopyFromCSV, i.OutDir)
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

	slog.Info("Import complete", "duration", prettyDuration(time.Since(t0)))

	return nil
}
