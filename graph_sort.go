package pg_mini

import (
	"fmt"
	"slices"
	"strings"
)

// calculateExportOrder determines the order in which tables should be exported,
// starting from a root table with a user-defined filter.
//
// The key principle: data flows outward from the root. Downstream tables (those
// that reference already-processed tables) are exported first, so their data can
// be used to filter upstream lookup tables. This ensures every non-root table
// gets a meaningful WHERE filter instead of WHERE TRUE.
//
// Phase 1: BFS from root following ReferencedBy edges (downstream propagation).
// Phase 2: Add remaining upstream/lookup tables once all their FK targets are processed.
func calculateExportOrder(tables map[string]*Table, startTable string) ([]string, error) {
	if tables[startTable] == nil {
		return nil, fmt.Errorf("start table not found: %s", startTable)
	}

	var result []string
	added := make(map[string]bool)

	// Phase 1: BFS from root following ReferencedBy edges.
	// This propagates the root filter downstream through FK relationships.
	result = append(result, startTable)
	added[startTable] = true

	queue := []string{startTable}
	for len(queue) > 0 {
		var nextWave []string
		for _, tbl := range queue {
			for _, ref := range tables[tbl].ReferencedByTbl {
				if !added[ref] {
					added[ref] = true
					nextWave = append(nextWave, ref)
				}
			}
		}
		slices.Sort(nextWave)
		result = append(result, nextWave...)
		queue = nextWave
	}

	// Phase 2: Add remaining tables (reachable only via References edges).
	// These are upstream/lookup tables. A table is ready when all its FK targets
	// are already processed (ignoring self-references), ensuring it can be
	// properly filtered by the downstream data collected in phase 1.
	for len(result) < len(tables) {
		var wave []string
		for name, t := range tables {
			if added[name] {
				continue
			}
			allRefsSatisfied := true
			for _, ref := range t.ReferencesTbl {
				if ref != name && !added[ref] {
					allRefsSatisfied = false
					break
				}
			}
			if allRefsSatisfied {
				wave = append(wave, name)
			}
		}

		if len(wave) == 0 {
			var remaining []string
			for name := range tables {
				if !added[name] {
					remaining = append(remaining, name)
				}
			}
			slices.Sort(remaining)
			return nil, fmt.Errorf("cannot determine export order for tables: %s (circular FK references prevent proper filtering from root %q)", strings.Join(remaining, ", "), startTable)
		}

		slices.Sort(wave)
		for _, tbl := range wave {
			added[tbl] = true
		}
		result = append(result, wave...)
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
