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
			return nil
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

	// Process tables that needed later processing, sorted alphabetically for deterministic order.
	var laterTables []string
	for table := range needsLaterProcessing {
		if !visited[table] {
			laterTables = append(laterTables, table)
		}
	}
	slices.Sort(laterTables)
	for _, table := range laterTables {
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
	// handle self-references: a table with only self-references is considered a root for import order
	for _, table := range tables {
		isRoot := true
		for _, ref := range table.ReferencesTbl {
			if ref != table.Name {
				isRoot = false
				break
			}
		}
		if isRoot {
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

	for len(queue) > 0 {
		initialQueueLen := len(queue)
		var nextQueue []*Table

		for _, tbl := range queue {
			// check if all the dependencies are satisfied (ignoring self-references)
			dependenciesSatisfied := true
			for _, ref := range tbl.ReferencesTbl {
				if ref != tbl.Name && !isVisited(ref) {
					dependenciesSatisfied = false
					break
				}
			}

			if dependenciesSatisfied {
				result = append(result, tbl.Name)
			} else {
				nextQueue = append(nextQueue, tbl)
			}
		}

		if len(nextQueue) == initialQueueLen {
			// No progress was made in a full pass, we have a cycle
			var names []string
			for _, t := range nextQueue {
				names = append(names, t.Name)
			}
			return nil, fmt.Errorf("cycle detected among tables: %s", strings.Join(names, ", "))
		}
		queue = nextQueue
	}

	return result, nil
}
