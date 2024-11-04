package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/fritzkeyzer/clite"
	"github.com/fritzkeyzer/pg_mini"
	"github.com/golang-cz/devslog"
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
		slog.Error(err.Error())
		os.Exit(1)
	}
}

func initLogger(debug bool) {
	level := slog.LevelInfo
	if debug {
		level = slog.LevelDebug
	}

	logger := slog.New(devslog.NewHandler(os.Stdout, &devslog.Options{
		HandlerOptions: &slog.HandlerOptions{
			Level: level,
		},
		TimeFormat:        time.Kitchen,
		StringIndentation: true,
	}))

	slog.SetDefault(logger)
}

var exportParams struct {
	ConnURI   string `flag:"--conn" comment:"required, database connection string"`
	RootTable string `flag:"--table" comment:"required, the top-level table you want to base this export on"`
	Filter    string `flag:"--filter" comment:"optional where clause (raw sql), eg: country_code = 'DE' order by random() limit 10000"`
	RawQuery  string `flag:"--raw" comment:"use the raw query instead of the filter, allows for more advanced queries"`
	OutDir    string `flag:"--out" comment:"required, the directory to write the exported files to"`

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
		if exportParams.OutDir == "" && !exportParams.DryRun {
			return fmt.Errorf("must provide an output directory")
		}
		if exportParams.RawQuery != "" && exportParams.Filter != "" {
			return fmt.Errorf("cannot provide both --raw-query and --filter")
		}

		initLogger(exportParams.Verbose)

		db, err := pgx.Connect(ctx, exportParams.ConnURI)
		if err != nil {
			return fmt.Errorf("connecting to database: %w", err)
		}

		export := &pg_mini.Export{
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

var importParams struct {
	ConnURI   string `flag:"--conn" comment:"required, database connection string"`
	RootTable string `flag:"--table" comment:"required, the top-level table used for the export"`
	Truncate  bool   `flag:"--truncate" comment:"truncate the target table before importing"`
	OutDir    string `flag:"--out" comment:"required, the directory to write the exported files to"`

	DryRun       bool `flag:"--dry" comment:"skip execution of queries"`
	Verbose      bool `flag:"--verbose"`
	NoAnimations bool `flag:"--no-animations" comment:"disables animations"`
}

var importCmd = clite.Cmd{
	Name:  "import",
	Flags: &importParams,
	Func: func(ctx context.Context) error {
		if importParams.ConnURI == "" {
			return fmt.Errorf("must provide a connection string")
		}
		if importParams.OutDir == "" {
			return fmt.Errorf("must provide an output directory")
		}

		initLogger(importParams.Verbose)

		db, err := pgx.Connect(ctx, importParams.ConnURI)
		if err != nil {
			return fmt.Errorf("connecting to database: %w", err)
		}

		importCmd := &pg_mini.Import{
			DB:           db,
			RootTable:    importParams.RootTable,
			Truncate:     importParams.Truncate,
			OutDir:       importParams.OutDir,
			DryRun:       importParams.DryRun,
			Verbose:      importParams.Verbose,
			NoAnimations: importParams.NoAnimations,
		}

		return importCmd.Run(ctx)
	},
}
