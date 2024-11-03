package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/fritzkeyzer/clite"
	"github.com/fritzkeyzer/pg_mini"
	"github.com/jackc/pgx/v5"
)

func main() {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		<-sigChan
		fmt.Println()
		log.Println("Aborting...")
		cancel()
	}()

	app := clite.App{
		Name:        "pg_mini",
		Description: "Create and restore consistent partial Postgres backups",
		Cmds: []clite.Cmd{
			exportCmd,
			importCmd,
		},
	}

	if err := app.Run(ctx); err != nil {
		log.Fatal("ERROR: ", err)
	}
}

var exportParams struct {
	ConnURI   string `flag:"--conn" comment:"required, database connection string"`
	RootTable string `flag:"--table" comment:"required, the top-level table you want to base this export on"`
	Filter    string `flag:"--filter" comment:"optional where clause (raw sql), eg: country_code = 'DE' order by random() limit 10000"`
	RawQuery  string `flag:"--raw-query" comment:"use the raw query instead of the filter, allows for more advanced queries"`
	OutDir    string `flag:"--out" comment:"required, the directory to write the exported files to"`

	// logging options
	DryRun       bool `flag:"--dry" comment:"skip execution of queries"`
	Verbose      bool `flag:"--verbose"`
	NoAnimations bool `flag:"--no-animations" comment:"disables animations"`
}

var exportCmd = clite.Cmd{
	Name:  "export",
	Flags: &exportParams,
	Func: func(ctx context.Context) error {
		if exportParams.ConnURI == "" {
			return fmt.Errorf("must provide a connection string")
		}
		if exportParams.RootTable == "" {
			return fmt.Errorf("must provide a root table name")
		}

		db, err := pgx.Connect(ctx, exportParams.ConnURI)
		if err != nil {
			return fmt.Errorf("connecting to database: %w", err)
		}

		export := &pg_mini.ExportCmd{
			DB:           db,
			RootTable:    exportParams.RootTable,
			Filter:       exportParams.Filter,
			RawQuery:     exportParams.RawQuery,
			OutDir:       exportParams.OutDir,
			DryRun:       exportParams.DryRun,
			Verbose:      exportParams.Verbose,
			NoAnimations: exportParams.NoAnimations,
		}

		return export.Run(ctx)
	},
}

var importCmd = clite.Cmd{
	Name: "import",
}
