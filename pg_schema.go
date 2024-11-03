package pg_mini

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

type Schema struct {
	Relations []foreignKeyRelation
}

type foreignKeyRelation struct {
	Table     string
	Column    string
	RefTable  string
	RefColumn string
}

func queryDBSchema(ctx context.Context, db *pgx.Conn) (*Schema, error) {
	rels, err := getForeignKeys(ctx, db)
	if err != nil {
		return nil, fmt.Errorf("getForeignKeys: %w", err)
	}

	return &Schema{Relations: rels}, nil
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
		if err := rows.Scan(&rel.Table, &rel.Column, &rel.RefTable, &rel.RefColumn); err != nil {
			return nil, err
		}
		relations = append(relations, rel)
	}
	return relations, nil
}
