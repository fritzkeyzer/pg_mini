# pg_mini
Is a command line tool and package for creating and restoring consistent partial backups for PostgreSQL.

eg: `pg_mini export --conn="..." --table="product" --filter="order by random() limit 10000" --out="backups/mini"`

outputs:
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

tag (63k rows, 200MB, copy 348ms, csv 1.09s)
├── product_tag (121k rows, 3MB, copy 124ms, csv 320ms)
└── vendor_tag (17k rows, 381kB, copy 48ms, csv 43ms)

user (5k rows, 6.2MB, copy 148ms, csv 534ms)
├── user_saved (5k rows, 1MB, copy 33ms, csv 16ms)
└── user_cart (0 rows, 118B, copy 1ms, csv 1ms)

2024/11/03 02:53:30 Done in 3.83s
2024/11/03 02:53:30 Exported to backups/mini
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