package pg_mini

import (
	"fmt"
	"slices"
	"strings"
)

const tmpTblPrefix = "tmp_mini_"

func tempCopyQueries(g *Graph, tbl, filter, raw string) []string {
	var selectQuery string
	selectCols := strings.Join(g.Tables[tbl].IncludeCols, ", ")

	if tbl == g.RootTbl {
		selectQuery = fmt.Sprintf("SELECT %s FROM %s", selectCols, tbl)
		if filter != "" {
			selectQuery += " " + filter
		}
		if raw != "" {
			selectQuery = raw
		}
	} else {
		selectQuery = fmt.Sprintf("SELECT %s FROM %s WHERE %s", selectCols, tbl, genFilter(g, tbl))
	}

	queries := []string{
		fmt.Sprintf(`CREATE TEMP TABLE %s AS (%s);`, tmpTblName(tbl), selectQuery),
	}

	var indexCols []string
	for _, rel := range g.Relations {
		fromIndex := slices.Index(g.ExportOrder, rel.FromTable)
		toIndex := slices.Index(g.ExportOrder, rel.ToTable)

		if rel.ToTable == tbl && !slices.Contains(indexCols, rel.ToColumn) && fromIndex > toIndex {
			indexCols = append(indexCols, rel.ToColumn)
		}
		if rel.FromTable == tbl && !slices.Contains(indexCols, rel.FromColumn) && fromIndex < toIndex {
			indexCols = append(indexCols, rel.FromColumn)
		}
	}
	if len(indexCols) > 0 {
		queries = append(queries, fmt.Sprintf(`CREATE INDEX ON %s (%s);`, tmpTblName(tbl), strings.Join(indexCols, ",")))
	}

	return queries
}

func tmpTblName(table string) string {
	return fmt.Sprintf("%s%s", tmpTblPrefix, table)
}

func genFilter(g *Graph, table string) string {
	colFilters := map[string][]string{}

	for _, rel := range g.Relations {
		fromIndex := slices.Index(g.ExportOrder, rel.FromTable)
		toIndex := slices.Index(g.ExportOrder, rel.ToTable)

		if rel.FromTable == table && fromIndex > toIndex {
			column := fmt.Sprintf("%s.%s", rel.FromTable, rel.FromColumn)
			idsQ := fmt.Sprintf("SELECT %s FROM %s", rel.ToColumn, tmpTblName(rel.ToTable))

			colFilters[column] = append(colFilters[column], idsQ)
		}

		if rel.ToTable == table && fromIndex < toIndex {
			column := fmt.Sprintf("%s.%s", rel.ToTable, rel.ToColumn)
			idsQ := fmt.Sprintf("SELECT %s FROM %s", rel.FromColumn, tmpTblName(rel.FromTable))

			colFilters[column] = append(colFilters[column], idsQ)
		}
	}

	var clauses []string
	for col, colSelects := range colFilters {
		idInSubQuery := strings.Join(colSelects, " UNION DISTINCT ")
		clause := fmt.Sprintf("%s IN (%s)", col, idInSubQuery)
		clauses = append(clauses, clause)
	}
	slices.Sort(clauses)

	if len(clauses) == 0 {
		return "TRUE"
	}

	filter := ""
	for _, clause := range clauses {
		if filter != "" {
			filter += " OR "
		}
		filter += fmt.Sprintf("(%s)", clause)
	}

	return filter
}
