package pg_mini

import (
	"reflect"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func Test_calculateImportOrder(t *testing.T) {
	type args struct {
		tables map[string]*Table
	}
	tests := []struct {
		name    string
		args    args
		want    []string
		wantErr bool
	}{
		{
			name: "",
			args: args{
				tables: map[string]*Table{
					"company": {
						Name:            "company",
						ReferencesTbl:   nil,
						ReferencedByTbl: []string{"company_tag", "website", "profile", "legal_entity"},
					},
					"company_tag": {
						Name:            "company_tag",
						ReferencesTbl:   []string{"company", "tag"},
						ReferencedByTbl: nil,
					},

					"website": {
						Name:            "website",
						ReferencesTbl:   []string{"company"},
						ReferencedByTbl: []string{"website_tag", "website_description"},
					},
					"website_tag": {
						Name:            "website_tag",
						ReferencesTbl:   []string{"website", "tag"},
						ReferencedByTbl: nil,
					},
					"website_description": {
						Name:            "website_description",
						ReferencesTbl:   []string{"website"},
						ReferencedByTbl: nil,
					},

					"profile": {
						Name:            "profile",
						ReferencesTbl:   []string{"company"},
						ReferencedByTbl: []string{"profile_tag", "profile_ftes"},
					},
					"profile_tag": {
						Name:            "profile_tag",
						ReferencesTbl:   []string{"profile", "tag"},
						ReferencedByTbl: nil,
					},
					"profile_ftes": {
						Name:            "profile_ftes",
						ReferencesTbl:   []string{"profile"},
						ReferencedByTbl: nil,
					},

					"legal_entity": {
						Name:            "legal_entity",
						ReferencesTbl:   []string{"company"},
						ReferencedByTbl: []string{"legal_entity_tag", "legal_entity_financial"},
					},
					"legal_entity_tag": {
						Name:            "legal_entity_tag",
						ReferencesTbl:   []string{"legal_entity", "tag"},
						ReferencedByTbl: nil,
					},
					"legal_entity_financial": {
						Name:            "legal_entity_financial",
						ReferencesTbl:   []string{"legal_entity"},
						ReferencedByTbl: nil,
					},

					"tag": {
						Name:            "tag",
						ReferencesTbl:   nil,
						ReferencedByTbl: []string{"company_tag", "website_tag", "profile_tag", "legal_entity_tag"},
					},
				},
			},
			want: []string{
				"company",
				"tag",
				"company_tag",
				"legal_entity",
				"legal_entity_financial",
				"legal_entity_tag",
				"profile",
				"profile_ftes",
				"profile_tag",
				"website",
				"website_description",
				"website_tag",
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := calculateImportOrder(tt.args.tables)
			if (err != nil) != tt.wantErr {
				t.Errorf("calculateImportOrder() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("calculateImportOrder(): %s", cmp.Diff(tt.want, got))
			}
		})
	}
}
