package pg_mini

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/k0kubun/pp/v3"
)

type ExportCmd struct {
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
func (c *ExportCmd) Run(ctx context.Context) error {
	t0 := time.Now()

	// Runs queries to understand your database schema
	schema, err := queryDBSchema(ctx, c.DB)
	if err != nil {
		return fmt.Errorf("get schema: %w", err)
	}

	// Build a dependency graph of tables based on foreign key relationships (including transitive dependencies!)
	// Provided with a root table an execution sequence is calculated to traverse the tree
	graph, err := buildGraph(schema, c.RootTable)
	if err != nil {
		return fmt.Errorf("build graph: %w", err)
	}
	if c.Verbose {
		pp.Println(graph)
	}
	err = graph.SaveAsJSON(c.OutDir)
	if err != nil {
		return fmt.Errorf("save graph: %w", err)
	}

	graph.Print(false)

	if c.DryRun {
		fmt.Println()
		for _, tbl := range graph.Order {
			for _, query := range tempCopyQueries(graph, tbl, c.Filter, c.RawQuery) {
				fmt.Println(query)
			}
		}
		fmt.Println()
		for _, tbl := range graph.Order {
			fmt.Println(copyToCSVQuery(tbl))
		}
		fmt.Println()
		return nil
	}

	// A set of queries are generated that copy data into temporary tables
	// In the correct sequence (starting with the root table)
	// Only including rows that are required to fulfil the foreign key relationships
	// run temp copy queries in transaction for consistency
	if c.Verbose || c.NoAnimations {
		log.Println("Begin transaction, copying data into temporary tables...")
	}
	tx, err := c.DB.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)
	for _, tbl := range graph.Order {
		graph.Tables[tbl].Status = statusCopyStarted
		graph.Print(true)

		t0 := time.Now()
		queries := tempCopyQueries(graph, tbl, c.Filter, c.RawQuery)
		var rows int64
		for _, query := range queries {
			if c.Verbose {
				log.Println(query)
			}
			r, err := tx.Exec(ctx, query)
			if err != nil {
				return fmt.Errorf("execute query %s: %w", query, err)
			}
			if c.Verbose {
				log.Println(r.String())
			}
			rows += r.RowsAffected()
		}

		graph.Tables[tbl].Status = statusCopyDone
		graph.Tables[tbl].Rows = rows
		graph.Tables[tbl].CopyDuration = time.Since(t0)
		graph.Print(true)
	}
	if c.Verbose || c.NoAnimations {
		log.Println("Commit transaction. Copying complete")
	}
	err = tx.Commit(ctx)
	if err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}

	// COPY from commands are used to export these temp tables to CSV
	for _, tbl := range graph.Order {
		t0 := time.Now()

		graph.Tables[tbl].Status = statusCSVStarted
		graph.Print(true)

		query := copyToCSVQuery(tbl)
		if c.Verbose {
			log.Println(query)
		}

		res, err := copyToCSV(ctx, c.DB, tbl, query, c.OutDir)
		if err != nil {
			return fmt.Errorf("copy out files: %w", err)
		}

		graph.Tables[tbl].Status = statusCSVDone
		graph.Tables[tbl].CSVSize = res.FileSize
		graph.Tables[tbl].CSVDuration = time.Since(t0)
		graph.Print(true)

		if c.NoAnimations || c.Verbose {
			log.Printf("Exported: %s (%s rows, %s, %s)", res.FileName, prettyCount(res.Rows), res.Duration.String(), prettyFileSize(res.FileSize))
		}
	}

	log.Printf("Done in %s", prettyDuration(time.Since(t0)))
	log.Println("Exported to", c.OutDir)

	return nil
}
