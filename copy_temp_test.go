package pg_mini

import (
	"os"
	"strings"
	"testing"

	"github.com/go-test/deep"
)

func queriesFromFile(t *testing.T, filename string) []string {
	f, err := os.ReadFile(filename)
	if err != nil {
		t.Fatal(err)
	}
	return strings.Split(string(f), "\n")
}

func Test_tempCopyQueries(t *testing.T) {
	type args struct {
		g      *Graph
		filter string
		raw    string
	}
	tests := []struct {
		name string
		args args
		want []string
	}{
		{
			name: "workflow",
			args: args{
				g:      graphFromFile(t, "testdata/workflow_graph.json"),
				filter: "order by updated_at desc",
				raw:    "",
			},
			want: queriesFromFile(t, "testdata/workflow_export_queries.txt"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got []string
			for _, tbl := range tt.args.g.ExportOrder {
				for _, query := range tempCopyQueries(tt.args.g, tbl, tt.args.filter, tt.args.raw) {
					got = append(got, query)
				}
			}
			if diff := deep.Equal(got, tt.want); diff != nil {
				for _, d := range diff {
					t.Error(d)
				}
			}
		})
	}
}
