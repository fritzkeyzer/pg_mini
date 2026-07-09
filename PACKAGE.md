# pg_mini (Go package)

```go
import "github.com/fritzkeyzer/pg_mini"
```

`pg_mini` creates consistent, partial backups of PostgreSQL databases. Given a
root table and a filter, it walks foreign key relationships to export only the
rows you need — plus every row required to keep those references valid — and
imports them back in the correct order.

The CLI (`cmd/pg_mini`) is a thin wrapper around the two entry points in this
package: [`Export`](#export) and [`Import`](#import).

## Quick start

```go
ctx := context.Background()

conn, err := pgx.Connect(ctx, "postgresql://user:pass@localhost:5432/mydb")
if err != nil {
	log.Fatal(err)
}
defer conn.Close(ctx)

// Export a subset of products (and their dependencies) to ./backup
exp := pg_mini.Export{
	DB:        conn,
	RootTable: "product",
	Filter:    "where country_code = 'DE' order by random() limit 10000",
	Storage:   pg_mini.DirStorage("backup"),
}
if err := exp.Run(ctx); err != nil {
	log.Fatal(err)
}

// Import it back into another database
imp := pg_mini.Import{
	DB:        otherConn,
	RootTable: "product",
	Storage:   pg_mini.DirStorage("backup"),
}
if err := imp.Run(ctx); err != nil {
	log.Fatal(err)
}
```

## Export

```go
type Export struct {
	DB        *pgx.Conn // required
	RootTable string    // required
	Filter    string    // WHERE/ORDER BY/LIMIT clause applied to the root table
	RawQuery  string    // full SELECT for the root table (alternative to Filter)
	Storage   Storage   // required — where artifacts are written

	DryRun       bool // print generated SQL, execute nothing
	GraphOnly    bool // write graph.json and stop
	Verbose      bool // log each step instead of the animated view
	NoAnimations bool // plain logging, no live-rendered graph
}

func (e *Export) Run(ctx context.Context) error
```

`Run` discovers the schema, builds a dependency graph from the root table,
copies the referenced rows into session-scoped temporary tables inside a single
transaction, and writes one CSV per table via `COPY TO`.

Because it only reads (`SELECT` + `COPY TO`) through `TEMP` tables, export works
with an unprivileged user — no schema-modification rights are needed, just
`SELECT` on the tables and the standard `TEMP` privilege on the database.

Use either `Filter` (appended to a `SELECT * FROM <root>`) or `RawQuery` (a
complete statement) to scope the root table. Downstream tables are filtered
automatically to satisfy foreign keys.

## Import

```go
type Import struct {
	DB        *pgx.Conn // required
	RootTable string    // required
	Storage   Storage   // required — where artifacts are read from

	// Mode (mutually exclusive; default is plain COPY FROM):
	Truncate   bool // truncate targets in reverse dependency order first
	Upsert     bool // INSERT ... ON CONFLICT DO UPDATE (needs PK/unique)
	SoftInsert bool // INSERT ... ON CONFLICT DO NOTHING (needs PK/unique)

	// Error handling:
	SkipErrors bool // row-by-row insert; log and skip failing rows
	MaxErrors  int  // abort after this many failures (-1 = no limit)

	DryRun       bool
	Verbose      bool
	NoAnimations bool
}

func (i *Import) Run(ctx context.Context) error
```

`Run` loads the exported schema, recomputes the import order, and loads each CSV
back with `COPY FROM` (or via temp tables for upsert / soft-insert).

By default the import is a fast bulk `COPY FROM` that fails on any conflict.
`SkipErrors` switches to row-by-row inserts that log failures via `slog` and
continue, reporting per-table counters (`processed`, `inserted`, `skipped`,
`failed`) at the end — useful for best-effort partial imports.

## Storage

`Storage` is required. Use the built-in `DirStorage` for the local filesystem,
or supply your own implementation to back an export/import with something else
(S3, GCS, in-memory, …).

```go
type Storage interface {
	// Create opens name for writing, truncating any existing entry.
	Create(name string) (io.WriteCloser, error)
	// Open opens name for reading.
	Open(name string) (io.ReadCloser, error)
}

// DirStorage is the built-in local-filesystem backend.
func DirStorage(dir string) Storage
```

Names are simple relative keys such as `"schema.json"`, `"graph.json"`, the
`*_queries.json` files, and one `<table>.csv` per table. Backends that finalize
on `Close` (e.g. an S3 upload) are supported — write errors are surfaced from
`Close`.

## Logging

`pg_mini` logs through the standard library `log/slog`. Set the default logger
(or a handler level) to control verbosity; `Verbose` / `NoAnimations` on
`Export`/`Import` switch the live-rendered graph off in favour of plain logs.
