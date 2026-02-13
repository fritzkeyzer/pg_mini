package pg_mini

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/jackc/pgx/v5"
)

type Schema struct {
	Tables    map[string]tableSchema
	Relations []foreignKeyRelation
}

type tableSchema struct {
	Name string
	Cols []columnSchema
}

type columnSchema struct {
	Name      string
	Generated bool
}

type foreignKeyRelation struct {
	FromTable  string
	FromColumn string
	ToTable    string
	ToColumn   string
}

func queryDBSchema(ctx context.Context, db *pgx.Conn) (*Schema, error) {
	rels, err := getForeignKeys(ctx, db)
	if err != nil {
		return nil, fmt.Errorf("getForeignKeys: %w", err)
	}

	schema := &Schema{
		Relations: rels,
		Tables:    make(map[string]tableSchema),
	}

	tables, err := getTables(ctx, db)
	if err != nil {
		return nil, fmt.Errorf("getTables: %w", err)
	}
	for _, t := range tables {
		schema.Tables[t.Name] = t
	}

	return schema, nil
}

func getTables(ctx context.Context, conn *pgx.Conn) ([]tableSchema, error) {
	query := `
		SELECT
			t.table_name,
			c.column_name,
			CASE WHEN c.generation_expression != '' THEN true ELSE false END as is_generated
		FROM information_schema.tables t
			 JOIN information_schema.columns c ON c.table_name = t.table_name
		WHERE t.table_schema = 'public' and c.table_schema = 'public'
		ORDER BY t.table_name, c.ordinal_position;
	`

	rows, err := conn.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("querying tables: %w", err)
	}
	defer rows.Close()

	tables := make(map[string]*tableSchema)
	for rows.Next() {
		var tableName, colName string
		var isGenerated bool

		if err := rows.Scan(&tableName, &colName, &isGenerated); err != nil {
			return nil, fmt.Errorf("scanning row: %w", err)
		}

		if _, exists := tables[tableName]; !exists {
			tables[tableName] = &tableSchema{
				Name: tableName,
			}
		}

		tables[tableName].Cols = append(tables[tableName].Cols, columnSchema{
			Name:      colName,
			Generated: isGenerated,
		})
	}

	// Convert map to slice
	result := make([]tableSchema, 0, len(tables))
	for _, t := range tables {
		result = append(result, *t)
	}

	return result, nil
}

func getForeignKeys(ctx context.Context, conn *pgx.Conn) ([]foreignKeyRelation, error) {
	query := `
       select
           tc.table_name,
           kcu.column_name,
           ccu.table_name as foreign_table_name,
           ccu.column_name as foreign_column_name
       from information_schema.table_constraints tc
       join information_schema.key_column_usage kcu
           on tc.constraint_name = kcu.constraint_name
       join information_schema.constraint_column_usage ccu
           on ccu.constraint_name = tc.constraint_name
       where tc.constraint_type = 'FOREIGN KEY';
   `

	rows, err := conn.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("querying foreign keys: %w", err)
	}
	defer rows.Close()

	var relations []foreignKeyRelation
	for rows.Next() {
		var rel foreignKeyRelation
		if err := rows.Scan(&rel.FromTable, &rel.FromColumn, &rel.ToTable, &rel.ToColumn); err != nil {
			return nil, err
		}
		relations = append(relations, rel)
	}
	return relations, nil
}

func FromJSONFile(file string, ptr any) error {
	contents, err := os.ReadFile(file)
	if err != nil {
		return fmt.Errorf("read file: %v", err)
	}

	err = json.Unmarshal(contents, ptr)
	if err != nil {
		return fmt.Errorf("unmarshal: %v", err)
	}
	return nil
}

func SaveAsJSONFile(v any, file string) error {
	err := os.MkdirAll(filepath.Dir(file), os.ModePerm)
	if err != nil {
		return fmt.Errorf("create directory %s: %v", filepath.Dir(file), err)
	}

	f, err := os.Create(file)
	if err != nil {
		return fmt.Errorf("create file %s: %v", file, err)
	}
	defer f.Close()

	err = json.NewEncoder(f).Encode(v)
	if err != nil {
		return fmt.Errorf("encode graph: %v", err)
	}
	return nil
}
