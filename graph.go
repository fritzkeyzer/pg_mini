package pg_mini

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/fatih/color"
)

type Graph struct {
	RootTbl   string
	Tables    map[string]*Table
	Relations []foreignKeyRelation // flat list of all relations in this db schema
	Order     []string
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
		if _, exists := g.Tables[rel.Table]; !exists {
			g.Tables[rel.Table] = &Table{
				Name: rel.Table,
			}
		}
		if _, exists := g.Tables[rel.RefTable]; !exists {
			g.Tables[rel.RefTable] = &Table{
				Name: rel.RefTable,
			}
		}
	}

	// 2nd loop determine dependencies
	for _, rel := range schema.Relations {
		fromTbl := rel.Table
		toTbl := rel.RefTable

		if !slices.Contains(g.Tables[fromTbl].ReferencesTbl, toTbl) {
			g.Tables[fromTbl].ReferencesTbl = append(g.Tables[fromTbl].ReferencesTbl, toTbl)
		}

		if !slices.Contains(g.Tables[toTbl].ReferencedByTbl, fromTbl) {
			g.Tables[toTbl].ReferencedByTbl = append(g.Tables[toTbl].ReferencedByTbl, fromTbl)
		}
	}

	// determine the correct order in which to export data
	order, err := topologicalSort(g.Tables, rootTbl)
	if err != nil {
		return nil, fmt.Errorf("topological sort: %v", err)
	}
	g.Order = order

	return g, nil
}

func topologicalSort(tables map[string]*Table, startTable string) ([]string, error) {
	var result []string

	// Track visited tables and detect cycles
	visited := make(map[string]bool)
	temp := make(map[string]bool) // for cycle detection

	// Special handling for tables that might need to be processed later
	// due to transitive dependencies (like tag table)
	needsLaterProcessing := make(map[string]bool)

	var visit func(table string) error
	visit = func(table string) error {
		if temp[table] {
			return fmt.Errorf("cycle detected at table: %s", table)
		}
		if visited[table] {
			return nil
		}

		temp[table] = true

		t := tables[table]
		if t == nil {
			return fmt.Errorf("table not found: %s", table)
		}

		// First, process tables this table references
		for _, ref := range t.ReferencesTbl {
			// Special case: if this table is referenced by others that haven't been processed yet,
			// we might need to process it later
			if len(tables[ref].ReferencedByTbl) > 1 {
				needsLaterProcessing[ref] = true
				continue
			}
			if err := visit(ref); err != nil {
				return err
			}
		}

		// Then add this table
		if !visited[table] {
			result = append(result, table)
		}

		// Then process tables that reference this table
		for _, ref := range t.ReferencedByTbl {
			if err := visit(ref); err != nil {
				return err
			}
		}

		visited[table] = true
		temp[table] = false
		return nil
	}

	// Start traversal from the given table
	if err := visit(startTable); err != nil {
		return nil, err
	}

	// Process tables that needed later processing
	for table := range needsLaterProcessing {
		if !visited[table] {
			if err := visit(table); err != nil {
				return nil, err
			}
		}
	}

	return result, nil
}

func (g *Graph) FromJSON(dir string) error {
	contents, err := os.ReadFile(dir)
	if err != nil {
		return fmt.Errorf("read file: %v", err)
	}

	err = json.Unmarshal(contents, g)
	if err != nil {
		return fmt.Errorf("unmarshal: %v", err)
	}
	return nil
}

func (g *Graph) SaveAsJSON(dir string) error {
	err := os.MkdirAll(dir, os.ModePerm)
	if err != nil {
		return fmt.Errorf("create directory %s: %v", dir, err)
	}

	f, err := os.Create(filepath.Join(dir, "graph.json"))
	if err != nil {
		return fmt.Errorf("create file %s: %v", dir, err)
	}
	defer f.Close()

	err = json.NewEncoder(f).Encode(g)
	if err != nil {
		return fmt.Errorf("encode graph: %v", err)
	}
	return nil
}

func (g *Graph) Print(anim bool) {
	// Define colored printers
	tableNamePrinter := color.New(color.FgCyan, color.Bold)
	statusPrinter := color.New(color.FgGreen)
	inProgressPrinter := color.New(color.FgYellow)
	treePrinter := color.New(color.FgWhite)
	circularRefPrinter := color.New(color.FgRed)

	buf := &bytes.Buffer{}
	var printTable func(tableName string, seen map[string]bool, level int, isLast bool, prefix string)
	printTable = func(tableName string, seen map[string]bool, level int, isLast bool, prefix string) {
		if seen[tableName] {
			circularRefPrinter.Fprintf(buf, "%s%s (circular ref)\n", prefix, tableName)
			return
		}

		table := g.Tables[tableName]
		if table == nil {
			return
		}

		// Print current table with appropriate prefix
		tableStatus := ""
		var statusWriter *color.Color = statusPrinter
		switch table.Status {
		case statusCopyStarted:
			tableStatus = " copying..."
			statusWriter = inProgressPrinter
		case statusCopyDone:
			tableStatus = fmt.Sprintf(" (%s rows, %s)",
				prettyCount(table.Rows),
				prettyDuration(table.CopyDuration),
			)
			statusWriter = inProgressPrinter
		case statusCSVStarted:
			tableStatus = fmt.Sprintf(" (%s rows, %s) writing csv...",
				prettyCount(table.Rows),
				prettyDuration(table.CopyDuration),
			)
			statusWriter = inProgressPrinter
		case statusCSVDone:
			tableStatus = fmt.Sprintf(" (%s rows, %s, copy %s, csv %s)",
				prettyCount(table.Rows),
				prettyFileSize(table.CSVSize),
				prettyDuration(table.CopyDuration),
				prettyDuration(table.CSVDuration),
			)
		}

		if level == 0 {
			treePrinter.Fprint(buf, prefix)
			tableNamePrinter.Fprint(buf, tableName)
			statusWriter.Fprintf(buf, "%s\n", tableStatus)
		} else {
			treePrinter.Fprint(buf, prefix)
			if isLast {
				treePrinter.Fprint(buf, "└── ")
			} else {
				treePrinter.Fprint(buf, "├── ")
			}
			tableNamePrinter.Fprint(buf, tableName)
			statusWriter.Fprintf(buf, "%s\n", tableStatus)
		}

		seen[tableName] = true

		if len(table.ReferencedByTbl) > 0 {
			newPrefix := prefix
			if level > 0 {
				if isLast {
					newPrefix += "    "
				} else {
					newPrefix += "│   "
				}
			}

			refs := make([]string, len(table.ReferencedByTbl))
			copy(refs, table.ReferencedByTbl)
			sort.Strings(refs)

			for i, refBy := range refs {
				isLastRef := i == len(refs)-1
				printTable(refBy, seen, level+1, isLastRef, newPrefix)
			}
		}

		delete(seen, tableName)
	}

	var rootTables []string
	for _, table := range g.Tables {
		if len(table.ReferencesTbl) == 0 {
			rootTables = append(rootTables, table.Name)
		}
	}
	sort.Strings(rootTables)

	for i, tableName := range rootTables {
		if i > 0 {
			fmt.Fprintln(buf)
		}
		printTable(tableName, make(map[string]bool), 0, i == len(rootTables)-1, "")
	}

	if anim {
		lines := strings.Split(buf.String(), "\n")
		fmt.Printf("\033[%dA", len(lines))
		for _, line := range lines {
			fmt.Print("\033[2K") // Clear line
			fmt.Println(line)
		}
	} else {
		fmt.Print(buf.String())
	}
}
