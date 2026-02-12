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
//   - Runs queries to understand your database schema
//   - Build a dependency graph of tables based on foreign key relationships (including transitive dependencies!)
//   - Provided with a root table an execution sequence is calculated to traverse the tree
//   - A set of queries are generated that copy data into temporary tables
//   - In the correct sequence (starting with the root table)
//   - Only including rows that are required to fulfil the foreign key relationships
//   - Queries are executed within a transaction for internal consistency
//   - COPY from commands are used to export these temp tables to CSV
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

	if i.DryRun {
		slog.Info("Dry run, not executing queries")

		fmt.Println()
		for _, tbl := range graph.ImportOrder {
			if i.Truncate {
				fmt.Println(truncateTblQuery(tbl))
			}

			fmt.Println(copyFromCSVQuery(tbl))
		}
		fmt.Println()

		slog.Info("Dry run complete")
		return nil
	}

	slog.Info("Importing...")

	for _, tbl := range graph.ImportOrder {
		if i.Truncate {
			truncateQuery := truncateTblQuery(tbl)
			slog.Debug(truncateQuery)
			_, err := i.DB.Exec(ctx, truncateQuery)
			if err != nil {
				return fmt.Errorf("truncate table: %w", err)
			}
			if i.Verbose || i.NoAnimations {
				slog.Info("Truncated table: " + tbl)
			}
		}

		query := copyFromCSVQuery(tbl)
		slog.Debug(query)

		res, err := copyFromCSV(ctx, i.DB, tbl, query, i.OutDir)
		if err != nil {
			return fmt.Errorf("copy from csv: %w", err)
		}

		if i.Verbose || i.NoAnimations {
			slog.Info("Imported CSV: "+tbl,
				"rows", prettyCount(res.Rows),
				"duration", prettyDuration(res.Duration),
				"file size", prettyFileSize(res.FileSize),
			)
		}
	}

	slog.Info("Import complete", "duration", prettyDuration(time.Since(t0)))

	return nil
}
