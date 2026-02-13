package pg_mini

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-test/deep"
)

func Test_generateExportQueries(t *testing.T) {
	tests := []struct {
		name   string
		dir    string
		root   string
		filter string
		raw    string
	}{
		{
			name:   "workflow",
			dir:    "testdata/workflow",
			root:   "workflow",
			filter: "order by updated_at desc",
		},
		{
			name: "company",
			dir:  "testdata/company",
			root: "company",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			schema := schemaFromFile(t, filepath.Join(tt.dir, "schema.json"))

			graph, err := buildGraph(schema, tt.root)
			if err != nil {
				t.Fatalf("buildGraph: %v", err)
			}

			queries := generateExportQueries(graph, tt.filter, tt.raw)

			goldenFile := filepath.Join(tt.dir, "export_queries.json")

			if os.Getenv("UPDATE_GOLDEN") == "1" {
				data, err := json.MarshalIndent(queries, "", "  ")
				if err != nil {
					t.Fatalf("marshal: %v", err)
				}
				err = os.WriteFile(goldenFile, data, 0644)
				if err != nil {
					t.Fatalf("write golden: %v", err)
				}
				t.Logf("Updated golden file: %s", goldenFile)
				return
			}

			data, err := os.ReadFile(goldenFile)
			if err != nil {
				t.Fatalf("read golden file (run with UPDATE_GOLDEN=1 to create): %v", err)
			}

			var want []ExportTableQueries
			err = json.Unmarshal(data, &want)
			if err != nil {
				t.Fatalf("unmarshal golden: %v", err)
			}

			if diff := deep.Equal(queries, want); diff != nil {
				for _, d := range diff {
					t.Error(d)
				}
			}
		})
	}
}

func Test_generateImportQueries(t *testing.T) {
	tests := []struct {
		name string
		dir  string
		root string
	}{
		{
			name: "workflow",
			dir:  "testdata/workflow",
			root: "workflow",
		},
		{
			name: "company",
			dir:  "testdata/company",
			root: "company",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			schema := schemaFromFile(t, filepath.Join(tt.dir, "schema.json"))

			graph, err := buildGraph(schema, tt.root)
			if err != nil {
				t.Fatalf("buildGraph: %v", err)
			}

			queries := generateImportQueries(graph, schema)

			goldenFile := filepath.Join(tt.dir, "import_queries.json")

			if os.Getenv("UPDATE_GOLDEN") == "1" {
				data, err := json.MarshalIndent(queries, "", "  ")
				if err != nil {
					t.Fatalf("marshal: %v", err)
				}
				err = os.WriteFile(goldenFile, data, 0644)
				if err != nil {
					t.Fatalf("write golden: %v", err)
				}
				t.Logf("Updated golden file: %s", goldenFile)
				return
			}

			data, err := os.ReadFile(goldenFile)
			if err != nil {
				t.Fatalf("read golden file (run with UPDATE_GOLDEN=1 to create): %v", err)
			}

			var want []ImportTableQueries
			err = json.Unmarshal(data, &want)
			if err != nil {
				t.Fatalf("unmarshal golden: %v", err)
			}

			if diff := deep.Equal(queries, want); diff != nil {
				for _, d := range diff {
					t.Error(d)
				}
			}
		})
	}
}
