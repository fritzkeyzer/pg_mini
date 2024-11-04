package pg_mini

import (
	"fmt"
	"slices"
	"time"
)

type Graph struct {
	RootTbl     string
	Tables      map[string]*Table
	Relations   []foreignKeyRelation // flat list of all relations in this db schema
	ExportOrder []string
	ImportOrder []string
}

type status string

const (
	statusInitial     status = "initial"
	statusCopyStarted status = "copy_started"
	statusCopyDone    status = "copy_done"
	statusCSVStarted  status = "csv_started"
	statusCSVDone     status = "csv_done"
)

type Table struct {
	Name            string
	ReferencesTbl   []string
	ReferencedByTbl []string
	IncludeCols     []string

	Status       status
	Rows         int64
	CopyDuration time.Duration
	CSVDuration  time.Duration
	CSVSize      int64
}

func buildGraph(schema *Schema, rootTbl string) (*Graph, error) {
	g := &Graph{
		RootTbl:   rootTbl,
		Tables:    make(map[string]*Table),
		Relations: schema.Relations,
	}

	// first loop: create all tables
	for _, rel := range schema.Relations {
		if _, exists := g.Tables[rel.FromTable]; !exists {
			tbl := &Table{
				Name: rel.FromTable,
			}

			tblSchema := schema.Tables[rel.FromTable]
			for _, col := range tblSchema.Cols {
				if !col.Generated {
					tbl.IncludeCols = append(tbl.IncludeCols, col.Name)
				}
			}

			g.Tables[rel.FromTable] = tbl
		}
		if _, exists := g.Tables[rel.ToTable]; !exists {
			tbl := &Table{
				Name: rel.ToTable,
			}

			tblSchema := schema.Tables[rel.ToTable]
			for _, col := range tblSchema.Cols {
				if !col.Generated {
					tbl.IncludeCols = append(tbl.IncludeCols, col.Name)
				}
			}

			g.Tables[rel.ToTable] = tbl
		}
	}

	// 2nd loop determine dependencies
	for _, rel := range schema.Relations {
		fromTbl := rel.FromTable
		toTbl := rel.ToTable

		if !slices.Contains(g.Tables[fromTbl].ReferencesTbl, toTbl) {
			g.Tables[fromTbl].ReferencesTbl = append(g.Tables[fromTbl].ReferencesTbl, toTbl)
		}

		if !slices.Contains(g.Tables[toTbl].ReferencedByTbl, fromTbl) {
			g.Tables[toTbl].ReferencedByTbl = append(g.Tables[toTbl].ReferencedByTbl, fromTbl)
		}
	}

	// sort the dependency and dependent table slices for stable output
	for _, tbl := range g.Tables {
		slices.Sort(tbl.ReferencesTbl)
		slices.Sort(tbl.ReferencedByTbl)
	}

	// determine the correct order in which to export data
	exportOrder, err := calculateExportOrder(g.Tables, rootTbl)
	if err != nil {
		return nil, fmt.Errorf("calculateExportOrder: %v", err)
	}
	g.ExportOrder = exportOrder

	importOrder, err := calculateImportOrder(g.Tables)
	if err != nil {
		return nil, fmt.Errorf("calculateImportOrder: %v", err)
	}
	g.ImportOrder = importOrder

	return g, nil
}
