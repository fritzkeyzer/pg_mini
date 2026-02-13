package pg_mini

import (
	"fmt"
	"strings"
)

type ExportTableQueries struct {
	Table       string
	CreateTmp   string // CREATE TEMP TABLE ...
	CreateIndex string // CREATE INDEX ... (empty if no index needed)
	CopyToCSV   string // COPY tmp_mini_X TO STDOUT ...
}

type ImportTableQueries struct {
	Table       string
	Truncate    string // TRUNCATE TABLE X CASCADE
	CopyFromCSV string // COPY X FROM STDIN ...
}

func generateExportQueries(g *Graph, filter, raw string) []ExportTableQueries {
	var result []ExportTableQueries

	for _, tbl := range g.ExportOrder {
		selectCols := strings.Join(g.Tables[tbl].IncludeCols, ", ")
		var selectQuery string

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

		tq := ExportTableQueries{
			Table:     tbl,
			CreateTmp: fmt.Sprintf(`CREATE TEMP TABLE %s AS (%s);`, tmpTblName(tbl), selectQuery),
			CopyToCSV: fmt.Sprintf("COPY %s TO STDOUT WITH CSV HEADER DELIMITER ',';", tmpTblName(tbl)),
		}

		// Build index query
		var indexCols []string
		for _, rel := range g.Relations {
			fromIndex := indexOf(g.ExportOrder, rel.FromTable)
			toIndex := indexOf(g.ExportOrder, rel.ToTable)

			if rel.ToTable == tbl && !contains(indexCols, rel.ToColumn) && fromIndex > toIndex {
				indexCols = append(indexCols, rel.ToColumn)
			}
			if rel.FromTable == tbl && !contains(indexCols, rel.FromColumn) && fromIndex < toIndex {
				indexCols = append(indexCols, rel.FromColumn)
			}
		}
		if len(indexCols) > 0 {
			tq.CreateIndex = fmt.Sprintf(`CREATE INDEX ON %s (%s);`, tmpTblName(tbl), strings.Join(indexCols, ","))
		}

		result = append(result, tq)
	}

	return result
}

func generateImportQueries(g *Graph) []ImportTableQueries {
	var result []ImportTableQueries

	for _, tbl := range g.ImportOrder {
		result = append(result, ImportTableQueries{
			Table:       tbl,
			Truncate:    fmt.Sprintf("TRUNCATE TABLE %s CASCADE;", tbl),
			CopyFromCSV: fmt.Sprintf("COPY %s FROM STDIN WITH CSV HEADER DELIMITER ',';", tbl),
		})
	}

	return result
}

func indexOf(slice []string, s string) int {
	for i, v := range slice {
		if v == s {
			return i
		}
	}
	return -1
}

func contains(slice []string, s string) bool {
	for _, v := range slice {
		if v == s {
			return true
		}
	}
	return false
}
