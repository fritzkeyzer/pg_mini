package pg_mini

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"

	"github.com/fatih/color"
)

var tableNameColor = color.New(color.FgCyan, color.Bold)
var greenPrinter = color.New(color.FgGreen)
var yellowPrinter = color.New(color.FgYellow)
var whitePrinter = color.New(color.FgWhite)
var redPrinter = color.New(color.FgRed)

type GraphPrinter struct {
	g       *Graph
	w       io.Writer
	enabled bool
}

func (g *GraphPrinter) Init(w io.Writer) {
	g.w = w
	g.enabled = true
}

func (g *GraphPrinter) Render() {
	if !g.enabled {
		return
	}
	g.g.print(g.w, true)
}

func (g *Graph) Print() {
	g.print(os.Stdout, false)
}

func (g *Graph) print(w io.Writer, anim bool) {
	buf := &bytes.Buffer{}

	fmt.Fprintln(buf)

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
		g.printTable(buf, tableName, make(map[string]bool), 0, i == len(rootTables)-1, "")
	}

	fmt.Fprintln(buf)

	if anim {
		lines := strings.Split(buf.String(), "\n")
		fmt.Fprintf(w, "\033[%dA", len(lines)+2)
		for _, line := range lines {
			fmt.Fprint(w, "\033[2K") // Clear line
			fmt.Fprintln(w, line)
		}
	} else {
		fmt.Fprint(w, buf.String())
	}
}

func (g *Graph) printTable(w io.Writer, tableName string, seen map[string]bool, level int, isLast bool, prefix string) {
	if seen[tableName] {
		redPrinter.Fprintf(w, "%s%s (circular ref)\n", prefix, tableName)
		return
	}

	table := g.Tables[tableName]
	if table == nil {
		return
	}

	// Print current table with appropriate prefix
	tableStatus := ""
	var statusColor *color.Color = greenPrinter
	switch table.Status {
	case statusCopyStarted:
		tableStatus = " copying..."
		statusColor = yellowPrinter
	case statusCopyDone:
		tableStatus = fmt.Sprintf(" (%s rows, %s)",
			prettyCount(table.Rows),
			prettyDuration(table.CopyDuration),
		)
		statusColor = yellowPrinter
	case statusCSVStarted:
		tableStatus = fmt.Sprintf(" (%s rows, %s) writing csv...",
			prettyCount(table.Rows),
			prettyDuration(table.CopyDuration),
		)
		statusColor = yellowPrinter
	case statusCSVDone:
		tableStatus = fmt.Sprintf(" (%s rows, %s, copy %s, csv %s)",
			prettyCount(table.Rows),
			prettyFileSize(table.CSVSize),
			prettyDuration(table.CopyDuration),
			prettyDuration(table.CSVDuration),
		)
	}

	if level == 0 {
		whitePrinter.Fprint(w, prefix)
		tableNameColor.Fprint(w, tableName)
		statusColor.Fprintf(w, "%s\n", tableStatus)
	} else {
		whitePrinter.Fprint(w, prefix)
		if isLast {
			whitePrinter.Fprint(w, "└── ")
		} else {
			whitePrinter.Fprint(w, "├── ")
		}
		tableNameColor.Fprint(w, tableName)
		statusColor.Fprintf(w, "%s\n", tableStatus)
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
			g.printTable(w, refBy, seen, level+1, isLastRef, newPrefix)
		}
	}

	delete(seen, tableName)
}
