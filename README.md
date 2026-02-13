# pg_mini

[![Tests](https://github.com/fritzkeyzer/pg_mini/actions/workflows/test.yml/badge.svg)](https://github.com/fritzkeyzer/pg_mini/actions/workflows/test.yml)

Create consistent, partial backups of PostgreSQL databases.

Export a subset of rows from a root table and `pg_mini` automatically follows foreign key relationships to include all dependent data. Import it back with full referential integrity preserved.

## Why?

- **Seed dev/staging databases** with realistic production data without copying the entire database
- **Fast** — only exports the rows you need, plus their dependencies
- **Consistent** — exports run in a single transaction, so foreign keys are always satisfied
- **Simple** — one command to export, one to import

## Install

```sh
go install github.com/fritzkeyzer/pg_mini/cmd/pg_mini@latest
```

## Export

Pick a root table, optionally filter it, and `pg_mini` exports that table plus all related tables (via foreign keys) to CSV files.

```sh
pg_mini export \
  --conn="postgresql://user:pass@localhost:5432/mydb" \
  --table="product" \
  --filter="where country_code='DE' order by random() limit 10000" \
  --out="backups/mini"
```

### Filter options

| Flag | Description |
|------|-------------|
| `--filter` | A SQL `WHERE` clause (and optional `ORDER BY` / `LIMIT`) appended to the root table query |
| `--raw` | A complete SQL query to use instead of `--filter` (mutually exclusive with `--filter`) |

Only the root table is filtered. All dependent tables are automatically included based on the foreign key relationships to the filtered root rows.

### Dry run

Use `--dry` to preview the generated SQL without executing anything:

```sh
pg_mini export --conn="..." --table="product" --filter="limit 100" --dry
```

### Example output

```
product (10k rows, 732kB, copy 521ms, csv 63ms)
├── product_tag (121k rows, 3MB, copy 124ms, csv 320ms)
├── user_saved (5k rows, 1MB, copy 33ms, csv 16ms)
├── user_cart (0 rows, 118B, copy 1ms, csv 1ms)
├── vendor (2k rows, 2MB, copy 39ms, csv 13ms)
│   └── vendor_tag (17k rows, 381kB, copy 48ms, csv 43ms)
└── website (10k rows, 4MB, copy 73ms, csv 43ms)
    ├── website_tag (139k rows, 3MB, copy 144ms, csv 317ms)
    ├── website_task (40k rows, 3MB, copy 127ms, csv 114ms)
    └── website_url (11k rows, 483kB, copy 53ms, csv 31ms)
```

## Import

Import a previously exported backup into a database:

```sh
pg_mini import \
  --conn="postgresql://user:pass@localhost:5432/mydb" \
  --table="product" \
  --out="backups/mini"
```

### Import modes

| Flag | Behavior |
|------|----------|
| *(default)* | `COPY FROM` — fast bulk insert, fails on conflicts |
| `--truncate` | Truncates target tables (in reverse dependency order) before importing |
| `--upsert` | Loads into temp tables, then `INSERT ... ON CONFLICT DO UPDATE` — merges data without deleting existing rows. Requires primary keys or unique constraints. |

`--truncate` and `--upsert` are mutually exclusive.

## How it works

1. Queries the database to discover the schema (tables, columns, foreign keys, primary keys)
2. Builds a dependency graph from foreign key relationships, including transitive dependencies
3. Generates queries to copy data into temporary tables in the correct order — starting from the filtered root table and following foreign keys so only referenced rows are included
4. Executes all copy queries within a single transaction for consistency
5. Exports the temporary tables to CSV using `COPY TO`

On import, the process is reversed: CSV files are loaded back in dependency order using `COPY FROM` (or upserted via temp tables).
