package bigquery_schema

import (
	"errors"
	"fmt"
	"testing"

	"github.com/kvanticoss/goutils/iterator"
	"github.com/kvanticoss/goutils/iterator/test_utils"
)

func TestInferBQSchema(t *testing.T) {
	type args struct {
		it iterator.RecordIterator[interface{}]
	}
	tests := []struct {
		name          string
		args          args
		wantGetSchema TableFieldSchema
		wantErr       bool
	}{
		{
			name: "simple",
			args: args{
				it: test_utils.NewDummyIteratorFromArr([]interface{}{
					map[string]interface{}{
						"a_date":      "2020-01-01",
						"b_datetime":  "2020-01-01T00:00:00",
						"c_timestamp": "2020-01-01T00:00:00Z",
						"d_counter":   "1",
						"e_float":     "1.232",
					},
					map[string]interface{}{
						"a_date":      "2020-01-01",
						"b_datetime":  "2020-01-01 00:00:00",
						"c_timestamp": "2020-01-01 00:00:00Z",
						"d_counter":   1,
						"e_float":     1.232,
					},
				}),
			},
			wantGetSchema: TableFieldSchema{
				Fields: []*TableFieldSchema{
					{
						Name: "a_date",
						Type: "DATE",
						Mode: "NULLABLE",
					},
					{
						Name: "b_datetime",
						Type: "DATETIME",
						Mode: "NULLABLE",
					},
					{
						Name: "c_timestamp",
						Type: "TIMESTAMP",
						Mode: "NULLABLE",
					},
					{
						Name: "d_counter",
						Type: "INTEGER",
						Mode: "NULLABLE",
					},
					{
						Name: "e_float",
						Type: "FLOAT",
						Mode: "NULLABLE",
					},
				},
			},
			wantErr: false,
		}, {
			name: "incompatiblefields",
			args: args{
				it: test_utils.NewDummyIteratorFromArr([]interface{}{
					map[string]interface{}{
						"a_date":    "2020-01-01",
						"b_counter": "1",
						"boolean":   "true",
					},
					map[string]interface{}{
						"a_date":    "2020-01-01T00:00:00Z",
						"b_counter": []int{1, 2, 3},
						"boolean":   "10",
					},
				}),
			},
			wantGetSchema: TableFieldSchema{
				Fields: []*TableFieldSchema{
					{
						Name: "a_date",
						Type: "STRING",
						Mode: "NULLABLE",
					},
					{
						Name: "b_counter",
						Type: "JSON",
						Mode: "NULLABLE",
					},
					{
						Name: "boolean",
						Type: "JSON",
						Mode: "NULLABLE",
					},
				},
			},
			wantErr: false,
		}, {
			name: "lists",
			args: args{
				it: test_utils.NewDummyIteratorFromArr([]interface{}{
					map[string]interface{}{
						"a_date":     "2020-01-01",
						"b_counters": []interface{}{"1", "2", "3", 4},
					},
					map[string]interface{}{
						"a_date":     "2020-01-01",
						"b_counters": []interface{}{1, "2.0", "3.123", 4},
					},
				}),
			},
			wantGetSchema: TableFieldSchema{
				Fields: []*TableFieldSchema{
					{
						Name: "a_date",
						Type: "DATE",
						Mode: "NULLABLE",
					},
					{
						Name: "b_counters",
						Type: "FLOAT",
						Mode: "REPEATED",
					},
				},
			},
			wantErr: false,
		},
		{
			name: "sub_structs",
			args: args{
				it: test_utils.NewDummyIteratorFromArr([]interface{}{
					map[string]interface{}{
						"a_date": "2020-01-01",
						"sub_struct": map[string]interface{}{
							"a": "1",
							"b": "2",
							"c": 4,
						},
					},
					map[string]interface{}{
						"a_date": "2020-01-01",
						"sub_struct": map[string]interface{}{
							"a": 1,
							"b": "2",
							"c": 4.5,
							"d": "Kalleo",
						},
					},
				}),
			},
			wantGetSchema: TableFieldSchema{
				Fields: []*TableFieldSchema{
					{
						Name: "a_date",
						Type: "DATE",
						Mode: "NULLABLE",
					},
					{
						Name: "sub_struct",
						Type: "RECORD",
						Fields: []*TableFieldSchema{
							{Name: "a", Type: "INTEGER", Mode: "NULLABLE"},
							{Name: "b", Type: "INTEGER", Mode: "NULLABLE"},
							{Name: "c", Type: "FLOAT", Mode: "NULLABLE"},
							{Name: "d", Type: "STRING", Mode: "NULLABLE"},
						},
						Mode: "NULLABLE",
					},
				},
			},
			wantErr: false,
		},

		{
			name: "repeated_sub_structs",
			args: args{
				it: test_utils.NewDummyIteratorFromArr([]interface{}{
					map[string]interface{}{
						"a_date": "2020-01-01",
						"sub_struct": []map[string]interface{}{
							{
								"a": 1000,
								"b": "2",
								//"c": 4.5,
								//"d": "Kalleo",
							},
							{
								//"a": 1,
								//"b": "2.23",
								"c": 4.5,
								"d": "456",
							},
						},
					},
					map[string]interface{}{
						"a_date": "2020-01-01",
						"sub_struct": []map[string]interface{}{
							{
								"a": 1,
								//"b": "2",
								"c": 4.5,
								"d": "Kalleo",
							},
							{
								"a": 1,
								"b": "2.23",
								//"c": 4.5,
								"d": "Kalleo",
							},
						},
					},
				}),
			},
			wantGetSchema: TableFieldSchema{
				Fields: []*TableFieldSchema{
					{
						Name: "a_date",
						Type: "DATE",
						Mode: "NULLABLE",
					},
					{
						Name: "sub_struct",
						Type: "RECORD",
						Fields: []*TableFieldSchema{
							{Name: "a", Type: "INTEGER", Mode: "NULLABLE"},
							{Name: "b", Type: "FLOAT", Mode: "NULLABLE"},
							{Name: "c", Type: "FLOAT", Mode: "NULLABLE"},
							{Name: "d", Type: "STRING", Mode: "NULLABLE"},
						},
						Mode: "REPEATED",
					},
				},
			},
			wantErr: false,
		},
	}

	var sameFields func(t *testing.T, f1, f2 []*TableFieldSchema) error
	sameFields = func(t *testing.T, f1, f2 []*TableFieldSchema) error {
		if len(f1) != len(f2) {
			return errors.New("length mismatch")
		}
		for idx1, f := range f1 {
			if f.Name != f2[idx1].Name {
				return fmt.Errorf("Name mismatch: expected: %s, got: %s for field: %s (and err %v)", f.Name, f2[idx1].Name, f.Name, f2[idx1].err)
			}
			if f.Type != f2[idx1].Type {
				return fmt.Errorf("Type mismatch: expected: %s, got: %s for field: %s (and err %v)", f.Type, f2[idx1].Type, f.Name, f2[idx1].err)
			}
			if f.Mode != f2[idx1].Mode {
				return fmt.Errorf("Mode mismatch: expected: %s, got: %s for field: %s (and err %v)", f.Mode, f2[idx1].Mode, f.Name, f2[idx1].err)
			}
			if f.Type == string(RecordFieldType) {
				if err := sameFields(t, f.Fields, f2[idx1].Fields); err != nil {
					return err
				}
			}
		}
		return nil
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotIt2, gotGetSchema, err := InferBQSchema(tt.args.it)
			for {
				if _, err := gotIt2(); err != nil {
					break
				}
			}

			if (err != nil) != tt.wantErr {
				t.Errorf("InferBQSchema() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if err := sameFields(t, tt.wantGetSchema.Fields, gotGetSchema().Fields); err != nil {
				t.Errorf("InferBQSchema() didn't yield expected results: %v (and err %v)", err, gotGetSchema().err)
			}
		})
	}
}

func TestTableFieldSchema_mergeTypes(t *testing.T) {
	type state struct {
		err  error
		Type FieldType
	}

	tests := []struct {
		name     string
		state    state
		arg      FieldType
		expected state
	}{
		{
			name: "string+string => string",
			state: state{
				Type: StringFieldType,
			},
			arg: StringFieldType,
			expected: state{
				err:  nil,
				Type: StringFieldType,
			},
		}, {
			name: "string+integer => string",
			state: state{
				Type: StringFieldType,
			},
			arg: IntegerFieldType,
			expected: state{
				err:  nil,
				Type: StringFieldType,
			},
		}, {
			name: "string+numeric => string",
			state: state{
				Type: StringFieldType,
			},
			arg: NumericFieldType,
			expected: state{
				err:  nil,
				Type: StringFieldType,
			},
		}, {
			name: "string+bignumeric => string",
			state: state{
				Type: StringFieldType,
			},
			arg: BigNumericFieldType,
			expected: state{
				err:  nil,
				Type: StringFieldType,
			},
		}, {
			name: "string+float => string",
			state: state{
				Type: StringFieldType,
			},
			arg: FloatFieldType,
			expected: state{
				err:  nil,
				Type: StringFieldType,
			},
		}, {
			name: "string+boolean => string",
			state: state{
				Type: StringFieldType,
			},
			arg: BooleanFieldType,
			expected: state{
				err:  nil,
				Type: StringFieldType,
			},
		}, {
			name: "string+timestamp => string",
			state: state{
				Type: StringFieldType,
			},
			arg: TimestampFieldType,
			expected: state{
				err:  nil,
				Type: StringFieldType,
			},
		}, {
			name: "string+date => string",
			state: state{
				Type: StringFieldType,
			},
			arg: DateFieldType,
			expected: state{
				err:  nil,
				Type: StringFieldType,
			},
		}, {
			name: "string+datetime => string",
			state: state{
				Type: StringFieldType,
			},
			arg: DateTimeFieldType,
			expected: state{
				err:  nil,
				Type: StringFieldType,
			},
		}, {
			name: "integer+float => float",
			state: state{
				Type: IntegerFieldType,
			},
			arg: FloatFieldType,
			expected: state{
				err:  nil,
				Type: FloatFieldType,
			},
		}, {
			name: "integer+bignumeric => bignumeric",
			state: state{
				Type: IntegerFieldType,
			},
			arg: BigNumericFieldType,
			expected: state{
				err:  nil,
				Type: BigNumericFieldType,
			},
		}, {
			name: "bool+integer => integer",
			state: state{
				Type: BooleanFieldType,
			},
			arg: IntegerFieldType,
			expected: state{
				err:  nil,
				Type: IntegerFieldType,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			i := &TableFieldSchema{
				err:  tt.state.err,
				Type: string(tt.state.Type),
			}
			i.mergeTypes(tt.arg)

			if i.Type != string(tt.expected.Type) {
				t.Errorf("TableFieldSchema.mergeTypes().Type = %v, want %v", i.Type, tt.expected.Type)
			}

			if i.err != tt.expected.err {
				t.Errorf("TableFieldSchema.mergeTypes().err = %v, want %v", i.err, tt.expected.err)
			}

			// The same test but reserved order of the arguments. The result should be the same
			i = &TableFieldSchema{
				err:  tt.state.err,
				Type: string(tt.arg),
			}
			i.mergeTypes(tt.state.Type)

			if i.Type != string(tt.expected.Type) {
				t.Errorf("TableFieldSchema.mergeTypes().Type = %v, want %v", i.Type, tt.expected.Type)
			}

			if i.err != tt.expected.err {
				t.Errorf("TableFieldSchema.mergeTypes().err = %v, want %v", i.err, tt.expected.err)
			}

		})
	}
}

func TestTableFieldSchema_addInferenceToField(t *testing.T) {
	type fields struct {
		err              error
		fieldIndexByName map[string]int
		Description      string
		Fields           []*TableFieldSchema
		Mode             string
		Name             string
		Precision        int64
		Scale            int64
		Type             string
	}
	type args struct {
		fieldName string
		r         any
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			i := &TableFieldSchema{
				err:              tt.fields.err,
				fieldIndexByName: tt.fields.fieldIndexByName,
				Description:      tt.fields.Description,
				Fields:           tt.fields.Fields,
				Mode:             tt.fields.Mode,
				Name:             tt.fields.Name,
				Precision:        tt.fields.Precision,
				Scale:            tt.fields.Scale,
				Type:             tt.fields.Type,
			}
			i.addInferenceToField(tt.args.fieldName, tt.args.r)
		})
	}
}

func Test_isDate(t *testing.T) {
	type args struct {
		strVal string
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "valid date",
			args: args{
				strVal: "2019-01-01",
			},
			want: true,
		}, {
			name: "invalid date",
			args: args{
				strVal: "2019-01-32",
			},
			want: false,
		}, {
			name: "invalid date",
			args: args{
				strVal: "2019-13-01",
			},
			want: false,
		}, {
			name: "invalid date",
			args: args{
				strVal: "2019-01-01 00:00:00",
			},
			want: false,
		}, {
			name: "invalid date",
			args: args{
				strVal: "2019-01-01T00:00:00",
			},
			want: false,
		}, {
			name: "invalid date",
			args: args{
				strVal: "2019-01-01T00:00:00Z",
			},
			want: false,
		}, {
			name: "invalid date",
			args: args{
				strVal: "2019-01-01T00:00:00+00:00",
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isDate(tt.args.strVal); got != tt.want {
				t.Errorf("isDate() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_isDateTime(t *testing.T) {
	type args struct {
		strVal string
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "valid datetime",
			args: args{
				strVal: "2019-01-01 00:00:00",
			},
			want: true,
		}, {
			name: "valid datetime",
			args: args{
				strVal: "2019-01-01T00:00:00",
			},
			want: true,
		}, {
			name: "valid datetime",
			args: args{
				strVal: "2019-01-01T00:00:00Z",
			},
			want: true,
		}, {
			name: "valid datetime",
			args: args{
				strVal: "2019-01-01T00:00:00+00:00",
			},
			want: true,
		}, {
			name: "invalid datetime",
			args: args{
				strVal: "2019-01-01",
			},
			want: false,
		}, {
			name: "invalid datetime",
			args: args{
				strVal: "2019-01-32",
			},
			want: false,
		}, {
			name: "invalid datetime",
			args: args{
				strVal: "2019-13-01",
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isDateTime(tt.args.strVal); got != tt.want {
				t.Errorf("isDateTime('%s') = %v, want %v", tt.args.strVal, got, tt.want)
			}
		})
	}
}

func Test_isTimestamp(t *testing.T) {
	type args struct {
		strVal string
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "valid timestamp",
			args: args{
				strVal: "2019-01-01 00:00:00Z",
			},
			want: true,
		}, {
			name: "valid timestamp",
			args: args{
				strVal: "2019-01-01T00:00:00Z",
			},
			want: true,
		}, {
			name: "valid timestamp",
			args: args{
				strVal: "2019-01-01T00:00:00Z",
			},
			want: true,
		}, {
			name: "valid timestamp",
			args: args{

				strVal: "2019-01-01T00:00:00+00:00",
			},
			want: true,
		}, {
			name: "invalid timestamp",
			args: args{
				strVal: "2019-01-01",
			},
			want: false,
		}, {
			name: "invalid timestamp",
			args: args{
				strVal: "2019-01-32",
			},
			want: false,
		}, {
			name: "invalid timestamp",
			args: args{
				strVal: "2019-13-01",
			},
			want: false,
		}, {
			name: "invalid timestamp",
			args: args{
				strVal: "2019-01-01 00:00:00.000",
			},
			want: false,
		}, {
			name: "invalid timestamp",
			args: args{
				strVal: "2019-01-01T00:00:00.000",
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isTimestamp(tt.args.strVal); got != tt.want {
				t.Errorf("isTimestamp('%s') = %v, want %v", tt.args.strVal, got, tt.want)
			}
		})
	}
}
