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
	Table    string
	Truncate string // TRUNCATE TABLE X CASCADE
	Copy     string // COPY X FROM STDIN ...

	// Upsert mode: COPY into temp table, then INSERT ... ON CONFLICT
	CreateTemp string // CREATE TEMP TABLE tmp_import_X (LIKE X INCLUDING ALL)
	CopyTemp   string // COPY tmp_import_X FROM STDIN WITH CSV HEADER ...
	Upsert     string // INSERT INTO X SELECT * FROM tmp ON CONFLICT (...) DO UPDATE SET ...
	DropTemp   string // DROP TABLE IF EXISTS tmp_import_X
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

func generateImportQueries(g *Graph, schema *Schema) []ImportTableQueries {
	var result []ImportTableQueries

	for _, tbl := range g.ImportOrder {
		tblSchema := schema.Tables[tbl]

		// Determine non-generated columns for COPY
		var includeCols []string
		for _, col := range tblSchema.Cols {
			if !col.Generated {
				includeCols = append(includeCols, col.Name)
			}
		}
		colList := strings.Join(includeCols, ", ")

		tmpName := "tmp_import_" + tbl

		tq := ImportTableQueries{
			Table:        tbl,
			Truncate:     fmt.Sprintf("TRUNCATE TABLE %s CASCADE;", tbl),
			Copy:  fmt.Sprintf("COPY %s (%s) FROM STDIN WITH CSV HEADER DELIMITER ',';", tbl, colList),
			CreateTemp: fmt.Sprintf("CREATE TEMP TABLE %s (LIKE %s INCLUDING ALL);", tmpName, tbl),
			CopyTemp: fmt.Sprintf("COPY %s (%s) FROM STDIN WITH CSV HEADER DELIMITER ',';", tmpName, colList),
			DropTemp:   fmt.Sprintf("DROP TABLE IF EXISTS %s;", tmpName),
		}

		// Determine conflict target columns: prefer primary key, fall back to first unique constraint
		var conflictCols []string
		if len(tblSchema.PrimaryKeyCols) > 0 {
			conflictCols = tblSchema.PrimaryKeyCols
		} else if len(tblSchema.UniqueConstraints) > 0 {
			conflictCols = tblSchema.UniqueConstraints[0]
		}

		// Generate upsert query if we have a conflict target
		if len(conflictCols) > 0 {
			conflictColList := strings.Join(conflictCols, ", ")

			// Build SET clause for non-conflict, non-generated columns
			var setClauses []string
			conflictSet := make(map[string]bool)
			for _, c := range conflictCols {
				conflictSet[c] = true
			}
			for _, col := range tblSchema.Cols {
				if !col.Generated && !conflictSet[col.Name] {
					setClauses = append(setClauses, fmt.Sprintf("%s = EXCLUDED.%s", col.Name, col.Name))
				}
			}

			doClause := "DO NOTHING"
			if len(setClauses) > 0 {
				doClause = "DO UPDATE SET " + strings.Join(setClauses, ", ")
			}

			tq.Upsert = fmt.Sprintf(
				"INSERT INTO %s (%s) SELECT %s FROM %s ON CONFLICT (%s) %s;",
				tbl, colList, colList, tmpName, conflictColList, doClause,
			)
		}

		result = append(result, tq)
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
