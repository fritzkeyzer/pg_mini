package pg_mini

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func schemaFromFile(t *testing.T, filename string) *Schema {
	f, err := os.ReadFile(filename)
	if err != nil {
		t.Fatal(err)
	}
	schema := &Schema{}
	err = json.Unmarshal(f, schema)
	if err != nil {
		t.Fatal(err)
	}
	return schema
}

func graphFromFile(t *testing.T, filename string) *Graph {
	f, err := os.ReadFile(filename)
	if err != nil {
		t.Fatal(err)
	}
	graph := &Graph{}
	err = json.Unmarshal(f, graph)
	if err != nil {
		t.Fatal(err)
	}
	return graph
}

func Test_buildGraph(t *testing.T) {
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

			got, err := buildGraph(schema, tt.root)
			if err != nil {
				t.Fatalf("buildGraph() error = %v", err)
			}

			goldenFile := filepath.Join(tt.dir, "graph.json")

			if os.Getenv("UPDATE_GOLDEN") == "1" {
				data, err := json.MarshalIndent(got, "", "  ")
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

			want := graphFromFile(t, goldenFile)
			if !reflect.DeepEqual(got, want) {
				t.Errorf("buildGraph() got = %v, want %v", got, want)
			}
		})
	}
}
