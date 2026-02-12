package pg_mini

import (
	"encoding/json"
	"os"
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
	type args struct {
		schema  *Schema
		rootTbl string
	}
	tests := []struct {
		name    string
		args    args
		want    *Graph
		wantErr bool
	}{
		{
			name: "workflows",
			args: args{
				schema:  schemaFromFile(t, "testdata/workflow_schema.json"),
				rootTbl: "workflow",
			},
			want: graphFromFile(t, "testdata/workflow_graph.json"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := buildGraph(tt.args.schema, tt.args.rootTbl)
			if (err != nil) != tt.wantErr {
				t.Errorf("buildGraph() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("buildGraph() got = %v, want %v", got, tt.want)
			}
		})
	}
}
