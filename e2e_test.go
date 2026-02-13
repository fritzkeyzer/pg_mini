package pg_mini

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

func startPostgres(t *testing.T) string {
	t.Helper()
	ctx := context.Background()

	pgContainer, err := postgres.Run(ctx, "postgres:16-alpine",
		postgres.WithDatabase("testdb"),
		postgres.WithUsername("test"),
		postgres.WithPassword("test"),
		postgres.BasicWaitStrategies(),
	)
	if err != nil {
		t.Fatalf("start postgres container: %v", err)
	}
	t.Cleanup(func() {
		if err := pgContainer.Terminate(ctx); err != nil {
			t.Logf("terminate postgres container: %v", err)
		}
	})

	connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("get connection string: %v", err)
	}
	return connStr
}

func connect(t *testing.T, connStr string) *pgx.Conn {
	t.Helper()
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		t.Fatalf("connect to postgres: %v", err)
	}
	t.Cleanup(func() { conn.Close(ctx) })
	return conn
}

func execSQLFile(t *testing.T, conn *pgx.Conn, path string) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read sql file %s: %v", path, err)
	}
	_, err = conn.Exec(context.Background(), string(data))
	if err != nil {
		t.Fatalf("exec sql file %s: %v", path, err)
	}
}

// snapshotDB returns all rows from all public tables as map[tableName][]row.
// Each row is a slice of string representations. Rows are sorted by all columns for determinism.
func snapshotDB(t *testing.T, conn *pgx.Conn) map[string][][]string {
	t.Helper()
	ctx := context.Background()

	rows, err := conn.Query(ctx, "SELECT tablename FROM pg_tables WHERE schemaname = 'public' ORDER BY tablename")
	if err != nil {
		t.Fatalf("query tables: %v", err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan table name: %v", err)
		}
		tables = append(tables, name)
	}
	rows.Close()

	snapshot := make(map[string][][]string)
	for _, table := range tables {
		tblRows, err := conn.Query(ctx, fmt.Sprintf("SELECT * FROM %s", table))
		if err != nil {
			t.Fatalf("query table %s: %v", table, err)
		}

		var allRows [][]string
		cols := tblRows.FieldDescriptions()
		for tblRows.Next() {
			vals, err := tblRows.Values()
			if err != nil {
				t.Fatalf("get values for %s: %v", table, err)
			}
			row := make([]string, len(vals))
			for i, v := range vals {
				row[i] = fmt.Sprintf("%v", v)
			}
			_ = cols
			allRows = append(allRows, row)
		}
		tblRows.Close()

		// Sort rows for determinism
		sort.Slice(allRows, func(i, j int) bool {
			for k := range allRows[i] {
				if allRows[i][k] != allRows[j][k] {
					return allRows[i][k] < allRows[j][k]
				}
			}
			return false
		})

		snapshot[table] = allRows
	}
	return snapshot
}

func compareSnapshots(t *testing.T, want, got map[string][][]string) {
	t.Helper()

	allTables := make(map[string]bool)
	for k := range want {
		allTables[k] = true
	}
	for k := range got {
		allTables[k] = true
	}

	for table := range allTables {
		wantRows := want[table]
		gotRows := got[table]

		if len(wantRows) != len(gotRows) {
			t.Errorf("table %s: want %d rows, got %d rows", table, len(wantRows), len(gotRows))
			continue
		}

		for i := range wantRows {
			if strings.Join(wantRows[i], "|") != strings.Join(gotRows[i], "|") {
				t.Errorf("table %s row %d:\n  want: %v\n  got:  %v", table, i, wantRows[i], gotRows[i])
			}
		}
	}
}

func truncateAll(t *testing.T, conn *pgx.Conn) {
	t.Helper()
	ctx := context.Background()

	rows, err := conn.Query(ctx, "SELECT tablename FROM pg_tables WHERE schemaname = 'public' ORDER BY tablename")
	if err != nil {
		t.Fatalf("query tables: %v", err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan table name: %v", err)
		}
		tables = append(tables, name)
	}
	rows.Close()

	if len(tables) > 0 {
		_, err = conn.Exec(ctx, fmt.Sprintf("TRUNCATE %s CASCADE", strings.Join(tables, ", ")))
		if err != nil {
			t.Fatalf("truncate tables: %v", err)
		}
	}
}

func TestE2E_RoundTrip(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	connStr := startPostgres(t)
	ctx := context.Background()

	// Setup schema and seed data
	setupConn := connect(t, connStr)
	execSQLFile(t, setupConn, "testdata/e2e/company/setup.sql")

	// Snapshot the original state
	original := snapshotDB(t, setupConn)

	// Export
	outDir := "testdata/e2e/company/backup"
	exportConn := connect(t, connStr)
	exp := &Export{
		DB:           exportConn,
		RootTable:    "company",
		OutDir:       outDir,
		NoAnimations: true,
	}
	if err := exp.Run(ctx); err != nil {
		t.Fatalf("export: %v", err)
	}

	// Truncate all tables
	truncConn := connect(t, connStr)
	truncateAll(t, truncConn)

	// Verify tables are empty
	emptySnap := snapshotDB(t, truncConn)
	for table, rows := range emptySnap {
		if len(rows) != 0 {
			t.Fatalf("table %s should be empty after truncate, has %d rows", table, len(rows))
		}
	}

	// Import (truncate mode — tables already empty, but tests the code path)
	importConn := connect(t, connStr)
	imp := &Import{
		DB:           importConn,
		RootTable:    "company",
		Truncate:     true,
		OutDir:       outDir,
		NoAnimations: true,
	}
	if err := imp.Run(ctx); err != nil {
		t.Fatalf("import: %v", err)
	}

	// Compare
	restored := snapshotDB(t, connect(t, connStr))
	compareSnapshots(t, original, restored)
}

func TestE2E_Upsert(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	connStr := startPostgres(t)
	ctx := context.Background()

	// Setup schema and seed data
	setupConn := connect(t, connStr)
	execSQLFile(t, setupConn, "testdata/e2e/company/setup.sql")

	// Snapshot the original state
	original := snapshotDB(t, setupConn)

	// Export
	outDir := "testdata/e2e/company/backup"
	exportConn := connect(t, connStr)
	exp := &Export{
		DB:           exportConn,
		RootTable:    "company",
		OutDir:       outDir,
		NoAnimations: true,
	}
	if err := exp.Run(ctx); err != nil {
		t.Fatalf("export: %v", err)
	}

	// Mutate a row and insert an extra row
	mutateConn := connect(t, connStr)
	_, err := mutateConn.Exec(ctx, "UPDATE company SET name = 'MUTATED' WHERE id = 1")
	if err != nil {
		t.Fatalf("mutate: %v", err)
	}
	_, err = mutateConn.Exec(ctx, "INSERT INTO company (id, name, created_at) VALUES (99, 'Extra Corp', '2024-06-01 00:00:00')")
	if err != nil {
		t.Fatalf("insert extra row: %v", err)
	}

	// Verify changes took effect
	var name string
	if err := mutateConn.QueryRow(ctx, "SELECT name FROM company WHERE id = 1").Scan(&name); err != nil {
		t.Fatalf("verify mutation: %v", err)
	}
	if name != "MUTATED" {
		t.Fatalf("expected MUTATED, got %s", name)
	}

	// Import with upsert (should restore mutated row but keep the extra row)
	importConn := connect(t, connStr)
	imp := &Import{
		DB:           importConn,
		RootTable:    "company",
		Upsert:       true,
		OutDir:       outDir,
		NoAnimations: true,
	}
	if err := imp.Run(ctx); err != nil {
		t.Fatalf("import upsert: %v", err)
	}

	// Build expected: original data + the extra row
	expected := make(map[string][][]string)
	for k, v := range original {
		expected[k] = v
	}
	// Add the extra company row
	extraRow := snapshotDB(t, connect(t, connStr))
	expected["company"] = extraRow["company"] // use actual snapshot since it includes both original + extra

	// Verify mutated row was restored
	verifyConn := connect(t, connStr)
	if err := verifyConn.QueryRow(ctx, "SELECT name FROM company WHERE id = 1").Scan(&name); err != nil {
		t.Fatalf("verify restored: %v", err)
	}
	if name != "Acme Corp" {
		t.Errorf("expected mutated row restored to 'Acme Corp', got %s", name)
	}

	// Verify extra row still exists
	if err := verifyConn.QueryRow(ctx, "SELECT name FROM company WHERE id = 99").Scan(&name); err != nil {
		t.Fatalf("verify extra row: %v", err)
	}
	if name != "Extra Corp" {
		t.Errorf("expected extra row 'Extra Corp', got %s", name)
	}

	// Verify all original tables still match (non-company tables unchanged)
	restored := snapshotDB(t, connect(t, connStr))
	for table, wantRows := range original {
		if table == "company" {
			// company table should have original 3 + extra row
			if len(restored[table]) != 4 {
				t.Errorf("table company: want 4 rows, got %d", len(restored[table]))
			}
			continue
		}
		compareSnapshots(t, map[string][][]string{table: wantRows}, map[string][][]string{table: restored[table]})
	}
}
