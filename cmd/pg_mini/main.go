package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/fritzkeyzer/pg_mini"
	"github.com/jackc/pgx/v5"
	"github.com/lmittmann/tint"
	"github.com/urfave/cli/v3"
)

func main() {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		<-sigChan
		fmt.Println()
		slog.Info("Aborting...")
		cancel()
	}()

	var logLevel slog.LevelVar
	slog.SetDefault(slog.New(tint.NewHandler(os.Stdout, &tint.Options{Level: &logLevel, TimeFormat: time.Kitchen})))

	verboseFlag := cli.BoolFlag{
		Name:  "verbose",
		Usage: "enable verbose/debug logging",
	}

	app := &cli.Command{
		Name:  "pg_mini",
		Usage: "Create and restore consistent partial Postgres backups",
		Commands: []*cli.Command{
			{
				Name: "export",
				Flags: []cli.Flag{
					&cli.StringFlag{Name: "conn", Usage: "required, database connection string"},
					&cli.StringFlag{Name: "table", Usage: "required, the top-level table you want to base this export on"},
					&cli.StringFlag{Name: "filter", Usage: "optional where clause (raw sql)"},
					&cli.StringFlag{Name: "raw", Usage: "use the raw query instead of the filter"},
					&cli.StringFlag{Name: "out", Usage: "required, the directory to write the exported files to"},
					&cli.BoolFlag{Name: "dry", Usage: "skip execution of queries"},
					&verboseFlag,
					&cli.BoolFlag{Name: "no-animations", Usage: "disables animations"},
				},
				Action: func(ctx context.Context, cmd *cli.Command) error {
					if cmd.Bool("verbose") {
						logLevel.Set(slog.LevelDebug)
					}

					connURI := cmd.String("conn")
					rootTable := cmd.String("table")
					filter := cmd.String("filter")
					rawQuery := cmd.String("raw")
					outDir := cmd.String("out")
					dryRun := cmd.Bool("dry")
					noAnimations := cmd.Bool("no-animations")

					if connURI == "" {
						return fmt.Errorf("must provide a connection string")
					}
					if rootTable == "" {
						return fmt.Errorf("must provide a root table name")
					}
					if outDir == "" && !dryRun {
						return fmt.Errorf("must provide an output directory")
					}
					if rawQuery != "" && filter != "" {
						return fmt.Errorf("cannot provide both --raw and --filter")
					}

					db, err := pgx.Connect(ctx, connURI)
					if err != nil {
						return fmt.Errorf("connecting to database: %w", err)
					}

					export := &pg_mini.Export{
						DB:           db,
						RootTable:    rootTable,
						Filter:       filter,
						RawQuery:     rawQuery,
						OutDir:       outDir,
						DryRun:       dryRun,
						Verbose:      cmd.Bool("verbose"),
						NoAnimations: noAnimations,
					}

					return export.Run(ctx)
				},
			},
			{
				Name: "import",
				Flags: []cli.Flag{
					&cli.StringFlag{Name: "conn", Usage: "required, database connection string"},
					&cli.StringFlag{Name: "table", Usage: "required, the top-level table used for the export"},
					&cli.BoolFlag{Name: "truncate", Usage: "truncate the target table before importing"},
					&cli.BoolFlag{Name: "upsert", Usage: "use INSERT ... ON CONFLICT DO UPDATE instead of plain COPY (requires primary keys)"},
					&cli.StringFlag{Name: "out", Usage: "required, the directory to read the exported files from"},
					&cli.BoolFlag{Name: "dry", Usage: "skip execution of queries"},
					&verboseFlag,
					&cli.BoolFlag{Name: "no-animations", Usage: "disables animations"},
				},
				Action: func(ctx context.Context, cmd *cli.Command) error {
					if cmd.Bool("verbose") {
						logLevel.Set(slog.LevelDebug)
					}

					connURI := cmd.String("conn")
					outDir := cmd.String("out")

					truncate := cmd.Bool("truncate")
					upsert := cmd.Bool("upsert")

					if connURI == "" {
						return fmt.Errorf("must provide a connection string")
					}
					if outDir == "" {
						return fmt.Errorf("must provide an output directory")
					}
					if truncate && upsert {
						return fmt.Errorf("--truncate and --upsert are mutually exclusive")
					}

					db, err := pgx.Connect(ctx, connURI)
					if err != nil {
						return fmt.Errorf("connecting to database: %w", err)
					}

					importCmd := &pg_mini.Import{
						DB:           db,
						RootTable:    cmd.String("table"),
						Truncate:     truncate,
						Upsert:       upsert,
						OutDir:       outDir,
						DryRun:       cmd.Bool("dry"),
						Verbose:      cmd.Bool("verbose"),
						NoAnimations: cmd.Bool("no-animations"),
					}

					return importCmd.Run(ctx)
				},
			},
		},
	}

	if err := app.Run(ctx, os.Args); err != nil {
		slog.Error("Fatal", "error", err)
		os.Exit(1)
	}
}
