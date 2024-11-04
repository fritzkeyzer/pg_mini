package pg_mini

import (
	"fmt"
	"slices"
	"strings"
)

// calculateExportOrder performs a topological sort, starting with a root table.
// The resulting order ensures that all dependencies are maintained in the export.
func calculateExportOrder(tables map[string]*Table, startTable string) ([]string, error) {
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

// calculateImportOrder performs a topological sort.
// The resulting order ensures that all dependencies are imported before their dependants.
func calculateImportOrder(tables map[string]*Table) ([]string, error) {
	var result []string

	isVisited := func(table string) bool {
		return slices.Contains(result, table)
	}

	// find all tables with no dependencies (root tables)
	for _, table := range tables {
		if len(table.ReferencesTbl) == 0 {
			result = append(result, table.Name)
		}
	}
	slices.SortStableFunc(result, func(a, b string) int {
		return strings.Compare(a, b)
	})

	// put remaining tables in a queue
	var queue []*Table
	for _, table := range tables {
		if !isVisited(table.Name) {
			queue = append(queue, table)
		}
	}
	slices.SortStableFunc(queue, func(a, b *Table) int {
		return strings.Compare(a.Name, b.Name)
	})

	var prevTbl *Table
	for len(queue) > 0 {
		// pop value
		tbl := queue[0]
		if tbl == prevTbl {
			return nil, fmt.Errorf("cycle detected: %s", tbl.Name)
		}
		queue = queue[1:]

		// check if all the dependencies are satisfied
		dependenciesSatisfied := true
		for _, ref := range tbl.ReferencesTbl {
			if !isVisited(ref) {
				dependenciesSatisfied = false
				break
			}
		}

		if dependenciesSatisfied {
			result = append(result, tbl.Name)
		} else {
			// push table back into queue
			queue = append(queue, tbl)
			continue
		}
	}

	return result, nil
}
