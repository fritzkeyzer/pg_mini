package pg_mini

import (
	"fmt"
	"slices"
	"strings"
)

func tempCopyQueries(g *Graph, tbl, filter, raw string) []string {
	selectQuery := fmt.Sprintf("SELECT * FROM %s WHERE %s", tbl, genFilter(g, tbl))

	if tbl == g.RootTbl {
		selectQuery = fmt.Sprintf("select * from %s", tbl)
		if filter != "" {
			selectQuery += " " + filter
		}
		if raw != "" {
			selectQuery = raw
		}
	}
	
	queries := []string{
		fmt.Sprintf(`CREATE TEMP TABLE %s AS (%s);`, tmpTblName(tbl), selectQuery),
	}

	var indexCols []string
	for _, rel := range g.Relations {
		fromIndex := slices.Index(g.Order, rel.Table)
		toIndex := slices.Index(g.Order, rel.RefTable)

		if rel.RefTable == tbl && !slices.Contains(indexCols, rel.RefColumn) && fromIndex > toIndex {
			indexCols = append(indexCols, rel.RefColumn)
		}
		if rel.Table == tbl && !slices.Contains(indexCols, rel.Column) && fromIndex < toIndex {
			indexCols = append(indexCols, rel.Column)
		}
	}
	if len(indexCols) > 0 {
		queries = append(queries, fmt.Sprintf(`CREATE INDEX ON %s(%s);`, tmpTblName(tbl), strings.Join(indexCols, ",")))
	}

	return queries
}

//func tempCopyQueries(g *Graph, root, filter, raw string) []string {
//	selectQuery := fmt.Sprintf("select * from %s", root)
//	if filter != "" {
//		selectQuery += " " + filter
//	}
//	if raw != "" {
//		selectQuery = raw
//	}
//
//	var queries []string
//	for i, tbl := range g.Order {
//		if i != 0 {
//			selectQuery = fmt.Sprintf("SELECT * FROM %s WHERE %s", tbl, genFilter(g, tbl))
//		}
//
//		queries = append(queries, fmt.Sprintf(`CREATE TEMP TABLE %s AS (%s);`, tmpTblName(tbl), selectQuery))
//
//		var indexCols []string
//		for _, rel := range g.Relations {
//			fromIndex := slices.Index(g.Order, rel.Table)
//			toIndex := slices.Index(g.Order, rel.RefTable)
//
//			if rel.RefTable == tbl && !slices.Contains(indexCols, rel.RefColumn) && fromIndex > toIndex {
//				indexCols = append(indexCols, rel.RefColumn)
//			}
//			if rel.Table == tbl && !slices.Contains(indexCols, rel.Column) && fromIndex < toIndex {
//				indexCols = append(indexCols, rel.Column)
//			}
//		}
//		if len(indexCols) > 0 {
//			queries = append(queries, fmt.Sprintf(`CREATE INDEX ON %s(%s);`, tmpTblName(tbl), strings.Join(indexCols, ",")))
//		}
//	}
//
//	return queries
//}

const tmpTblPrefix = "tmp_mini_"

func tmpTblName(table string) string {
	return fmt.Sprintf("%s%s", tmpTblPrefix, table)
}

func genFilter(g *Graph, table string) string {
	colFilters := map[string][]string{}

	for _, rel := range g.Relations {
		fromIndex := slices.Index(g.Order, rel.Table)
		toIndex := slices.Index(g.Order, rel.RefTable)

		if rel.Table == table && fromIndex > toIndex {
			column := fmt.Sprintf("%s.%s", rel.Table, rel.Column)
			idsQ := fmt.Sprintf("SELECT %s FROM %s", rel.RefColumn, tmpTblName(rel.RefTable))

			colFilters[column] = append(colFilters[column], idsQ)
		}

		if rel.RefTable == table && fromIndex < toIndex {
			column := fmt.Sprintf("%s.%s", rel.RefTable, rel.RefColumn)
			idsQ := fmt.Sprintf("SELECT %s FROM %s", rel.Column, tmpTblName(rel.Table))

			colFilters[column] = append(colFilters[column], idsQ)
		}
	}

	var clauses []string
	for col, colSelects := range colFilters {
		idInSubQuery := strings.Join(colSelects, " UNION ALL ")
		clause := fmt.Sprintf("%s IN (%s)", col, idInSubQuery)
		clauses = append(clauses, clause)
	}
	slices.Sort(clauses)

	filter := ""
	for _, clause := range clauses {
		if filter != "" {
			filter += " OR "
		}
		filter += fmt.Sprintf("(%s)", clause)
	}

	return filter
}
