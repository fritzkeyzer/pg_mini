# pg_mini
Is a command line tool and package for creating and restoring consistent partial backups for PostgreSQL.


eg: `pg_mini export --conn="..." --table="product" --filter="order by random() limit 1000" --out="backups/mini/product"`

outputs:
```
Table depencies:

product ──► vendor ───────► vendor_tag ──► tag
     ▲                                      ▲
     ├───── user_cart ──┬─► user            │
     ├───── user_saved ─┘                   │
     └───── product_tag ────────────────────┘

Sequence: 
product, vendor, vendor_tag, user_cart, user_saved, product_tag, tag, user

Copying to temp tables...
product (1000 rows, 1s)
 ├── vendor (123 rows, 123ms)
 │    └── vendor_tag  (234 rows, 234ms)
 ├── user_cart (12 rows, 12ms)
 ├── user_saved (23 rows, 23ms)
 └── product_tag (3456 rows, 3456ms)
tag (345 rows, 345ms)
user (25 rows, 25ms)

Exporting to CSV...
product (1000 rows, 1MB, 1s)
 ├── vendor (123 rows, 123kB, 123ms)
 │    └── vendor_tag  (234 rows, 234kB, 234ms)
 ├── user_cart (12 rows, 12kB, 12ms)
 ├── user_saved (23 rows, 23kB, 23ms)
 └── product_tag (3456 rows, 3456kB, 3456ms)
tag (345 rows, 345kB, 345ms)
user (25 rows, 25kB, 25ms)

Done. 
Exported to backups/mini/product (2MB, 2s)
```

## How it works

Steps
- Runs queries to understand your database schema
- Build a dependency graph of tables based on foreign key relationships (including transitive dependencies!)
- Provided with a root table an execution sequence is calculated to traverse the tree
- A set of queries are generated that copy data into temporary tables 
  - In the correct sequence (starting with the root table)
  - Only including rows that are required to fulfil the foreign key relationships
- Queries are executed within a transaction for internal consistency
- COPY from commands are used to export these temp tables to CSV