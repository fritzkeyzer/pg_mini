# pg_mini

[![Tests](https://github.com/fritzkeyzer/pg_mini/actions/workflows/test.yml/badge.svg)](https://github.com/fritzkeyzer/pg_mini/actions/workflows/test.yml)

Create consistent, partial backups of PostgreSQL databases.

- Install the CLI with `brew` or `go install`
- Or import the Go library `github.com/fritzkeyzer/pg_mini`

## How it works

Given a root table and a filter, pg_mini follows foreign key relationships to export only the rows you
need — plus every row required to keep those references valid. Exports run in a single transaction and
land as one CSV file per table. Import loads them back in dependency order.

## Why?

- **Seed dev/staging databases** with realistic production data without copying the entire database
- **Fast** — only exports the rows you need, plus their dependencies
- **Consistent** — exports run in a single transaction, so foreign keys are always satisfied
- **Simple** — one command to export, one to import
- **Debuggable** — one CSV file per table, is easy to grep

## CLI

- Homebrew (macOS/Linux) `brew install fritzkeyzer/tap/pg_mini`
- Go toolchain (cross platform) `go install github.com/fritzkeyzer/pg_mini/cmd/pg_mini@latest`
- Or download a prebuilt binary from [Releases](https://github.com/fritzkeyzer/pg_mini/releases).

```sh
# Export with random 10k German products
pg_mini export \
  --conn="postgresql://user:pass@localhost:5432/mydb" \
  --table="product" \
  --filter="where country_code='DE' order by random() limit 10000" \
  --out="backups/mini/products_de_10k"


# Import
pg_mini import \
  --conn="postgresql://user:pass@localhost:5432/mydb" \
  --table="product" \
  --out="backups/mini/products_de_10k"
```

### S3 compatible storage

`--out` also accepts an `s3://bucket/prefix` URL (AWS S3, MinIO, R2, B2, etc.) for
both `export` and `import`. The bucket must already exist. Credentials come from
`PG_MINI_S3_KEY_ID` / `PG_MINI_S3_ACCESS_KEY`, falling back to the IAM chain.

| Flag            | Behavior                                                    |
|-----------------|-------------------------------------------------------------|
| `--s3-endpoint` | S3 host (default `s3.amazonaws.com`; e.g. `localhost:9000`) |
| `--s3-region`   | Region, required by some endpoints                          |
| `--s3-insecure` | Use plain HTTP instead of HTTPS (e.g. local MinIO)          |

```sh
export PG_MINI_S3_KEY_ID=... 
export PG_MINI_S3_ACCESS_KEY=...

pg_mini export --conn="postgres://..." --table=products \
  --out="s3://my-bucket/backups/products" --s3-region=us-east-1
```

### Dry mode

- Both `export` and `import` support `--dry` and `--graph-only`
- `--dry` and `--graph-only` only execute introspection queries (strictly read-only)
- `--dry` emits all the queries that it would have executed - via stdout
- `--graph-only` saves the schema instropection result `graph.json`

### Import modes

| Flag            | Behavior                                                                                             |
|-----------------|------------------------------------------------------------------------------------------------------|
| *(default)*     | `COPY FROM` — fast bulk insert, fails on conflicts                                                   |
| `--truncate`    | Truncates target tables (in reverse dependency order) before importing                               |
| `--upsert`      | Merges data, updating existing rows on conflict. Requires primary keys or unique constraints.        |
| `--soft-insert` | Inserts only new rows, skipping any that already exist. Requires primary keys or unique constraints. |
| `--skip-errors` | Best-effort partial import: inserts row-by-row, logs failures, and keeps going.                      |
| `--max-errors`  | Used with `--skip-errors`; aborts once failures exceed this limit. Default `-1` (no limit).          |

`--truncate`, `--upsert`, and `--soft-insert` are mutually exclusive.

## Embedded use

`pg_mini` is also an importable Go package — the CLI is a thin wrapper around it.

```go
conn, _ := pgx.Connect(ctx, connStr)

exp := pg_mini.Export{
    DB:        conn,
    RootTable: "company",
    Store:   pg_mini.DirStore("backup"),
}
if err := exp.Run(ctx); err != nil {
    // ...
}
```

See [PACKAGE.md](./PACKAGE.md) for the full API, including custom storage backends (S3, GCS, in-memory, …).

## License

[MIT](LICENSE)
