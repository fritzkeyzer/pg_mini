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
	DB         *pgx.Conn
	RootTable  string
	Truncate   bool
	Upsert     bool
	SoftInsert bool
	SkipErrors bool
	MaxErrors  int
	OutDir     string

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

	if i.MaxErrors < -1 {
		return fmt.Errorf("--max-errors must be -1 or >= 0")
	}

	if i.DryRun {
		slog.Info("Dry run, not executing queries")

		fmt.Println()
		for _, tq := range queries {
			if i.Truncate {
				fmt.Println(tq.Truncate)
			}
			if i.SkipErrors {
				if i.Upsert {
					if tq.RowUpsert != "" {
						fmt.Println(tq.RowUpsert)
					} else {
						fmt.Println(tq.Insert)
					}
				} else if i.SoftInsert {
					if tq.RowSoftInsert != "" {
						fmt.Println(tq.RowSoftInsert)
					} else {
						fmt.Println(tq.Insert)
					}
				} else {
					fmt.Println(tq.Insert)
				}
			} else if i.Upsert {
				fmt.Println(tq.CreateTemp)
				fmt.Println(tq.CopyTemp)
				fmt.Println(tq.Upsert)
				fmt.Println(tq.DropTemp)
			} else if i.SoftInsert {
				fmt.Println(tq.CreateTemp)
				fmt.Println(tq.CopyTemp)
				fmt.Println(tq.SoftInsert)
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
	tableStats := map[string]*rowImportRes{}
	nullableColsByTable := map[string]map[string]bool{}

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

		if i.SkipErrors {
			nullableCols, ok := nullableColsByTable[tq.Table]
			if !ok {
				nullableCols, err = getNullableColumns(ctx, i.DB, tq.Table)
				if err != nil {
					return fmt.Errorf("load nullable columns for %s: %w", tq.Table, err)
				}
				nullableColsByTable[tq.Table] = nullableCols
			}

			mode := "insert"
			query := tq.Insert
			usedFallback := false

			if i.Upsert {
				if tq.RowUpsert != "" {
					query = tq.RowUpsert
					mode = "upsert"
				} else {
					usedFallback = true
					slog.Info("No primary key or unique constraint for table, falling back to row inserts", "table", tq.Table)
				}
			} else if i.SoftInsert {
				if tq.RowSoftInsert != "" {
					query = tq.RowSoftInsert
					mode = "soft-insert"
				} else {
					usedFallback = true
					slog.Info("No primary key or unique constraint for table, falling back to row inserts", "table", tq.Table)
				}
			}

			res, err := insertRowsFromCSV(
				ctx,
				i.DB,
				tq.Table,
				tq.Columns,
				nullableCols,
				query,
				i.OutDir,
				i.MaxErrors,
				i.SoftInsert,
			)
			if err != nil {
				return fmt.Errorf("row import for %s: %w", tq.Table, err)
			}
			res.Mode = mode
			res.UsedFallback = usedFallback
			tableStats[tq.Table] = res

			if i.Verbose || i.NoAnimations {
				slog.Info("Imported rows: "+tq.Table,
					"mode", mode,
					"processed", prettyCount(res.Processed),
					"inserted", prettyCount(res.Inserted),
					"skipped", prettyCount(res.Skipped),
					"failed", prettyCount(res.Failed),
					"duration", prettyDuration(res.Duration),
					"file size", prettyFileSize(res.FileSize),
				)
			}
		} else if i.Upsert {
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
		} else if i.SoftInsert {
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

			// Soft insert from temp into target (skip conflicts)
			slog.Debug(tq.SoftInsert)
			_, err = i.DB.Exec(ctx, tq.SoftInsert)
			if err != nil {
				return fmt.Errorf("soft insert from temp table for %s: %w", tq.Table, err)
			}

			// Drop temp table
			slog.Debug(tq.DropTemp)
			_, err = i.DB.Exec(ctx, tq.DropTemp)
			if err != nil {
				return fmt.Errorf("drop temp table for %s: %w", tq.Table, err)
			}

			if i.Verbose || i.NoAnimations {
				slog.Info("Soft inserted: "+tq.Table,
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

	if i.SkipErrors {
		var totalProcessed int64
		var totalInserted int64
		var totalSkipped int64
		var totalFailed int64

		for _, tq := range queries {
			res, ok := tableStats[tq.Table]
			if !ok {
				continue
			}

			totalProcessed += res.Processed
			totalInserted += res.Inserted
			totalSkipped += res.Skipped
			totalFailed += res.Failed

			slog.Info("Table import stats",
				"table", tq.Table,
				"mode", res.Mode,
				"fallback", res.UsedFallback,
				"processed", res.Processed,
				"inserted", res.Inserted,
				"skipped", res.Skipped,
				"failed", res.Failed,
			)
		}

		slog.Info("Import row stats",
			"processed", totalProcessed,
			"inserted", totalInserted,
			"skipped", totalSkipped,
			"failed", totalFailed,
		)
	}

	slog.Info("Import complete", "duration", prettyDuration(time.Since(t0)))

	return nil
}
