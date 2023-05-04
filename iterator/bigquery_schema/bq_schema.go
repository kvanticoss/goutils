package bigquery_schema

// Copyright 2015 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// NOTE:
// ======================================================================
// this file is an except for the official bigquery package to remove
// external dependencies. The original files can be found at:
// https://github.com/googleapis/google-cloud-go/blob/bigquery/v1.51.0/bigquery/schema.go
// https://github.com/googleapis/google-api-go-client/blob/main/bigquery/v2/bigquery-gen.go
//
// This file only removes elements and is not compatible with the original.

import (
	"encoding/json"
	"math/big"
	"reflect"
	"time"
)

type TableFieldSchema struct {
	// Interim
	err error
	// utility for faster lookups
	fieldIndexByName map[string]int

	// Description: [Optional] The field description. The maximum length is
	// 1,024 characters.
	Description string `json:"description,omitempty"`

	// Fields: [Optional] Describes the nested schema fields if the type
	// property is set to RECORD.
	Fields []*TableFieldSchema `json:"fields,omitempty"`

	// Mode: [Optional] The field mode. Possible values include NULLABLE,
	// REQUIRED and REPEATED. The default value is NULLABLE.
	Mode string `json:"mode,omitempty"`

	// Name: [Required] The field name. The name must contain only letters
	// (a-z, A-Z), numbers (0-9), or underscores (_), and must start with a
	// letter or underscore. The maximum length is 300 characters.
	Name string `json:"name,omitempty"`

	// Precision: [Optional] Precision (maximum number of total digits in
	// base 10) and scale (maximum number of digits in the fractional part
	// in base 10) constraints for values of this field for NUMERIC or
	// BIGNUMERIC. It is invalid to set precision or scale if type ≠
	// "NUMERIC" and ≠ "BIGNUMERIC". If precision and scale are not
	// specified, no value range constraint is imposed on this field insofar
	// as values are permitted by the type. Values of this NUMERIC or
	// BIGNUMERIC field must be in this range when: - Precision (P) and
	// scale (S) are specified: [-10P-S + 10-S, 10P-S - 10-S] - Precision
	// (P) is specified but not scale (and thus scale is interpreted to be
	// equal to zero): [-10P + 1, 10P - 1]. Acceptable values for precision
	// and scale if both are specified: - If type = "NUMERIC": 1 ≤
	// precision - scale ≤ 29 and 0 ≤ scale ≤ 9. - If type =
	// "BIGNUMERIC": 1 ≤ precision - scale ≤ 38 and 0 ≤ scale ≤ 38.
	// Acceptable values for precision if only precision is specified but
	// not scale (and thus scale is interpreted to be equal to zero): - If
	// type = "NUMERIC": 1 ≤ precision ≤ 29. - If type = "BIGNUMERIC": 1
	// ≤ precision ≤ 38. If scale is specified but not precision, then
	// it is invalid.
	Precision int64 `json:"precision,omitempty,string"`

	// Scale: [Optional] See documentation for precision.
	Scale int64 `json:"scale,omitempty,string"`

	// Type: [Required] The field data type. Possible values include STRING,
	// BYTES, INTEGER, INT64 (same as INTEGER), FLOAT, FLOAT64 (same as
	// FLOAT), NUMERIC, BIGNUMERIC, BOOLEAN, BOOL (same as BOOLEAN),
	// TIMESTAMP, DATE, TIME, DATETIME, INTERVAL, RECORD (where RECORD
	// indicates that the field contains a nested schema) or STRUCT (same as
	// RECORD).
	Type string `json:"type,omitempty"`
}

// FieldType is the type of field.
type FieldType string

const (
	// StringFieldType is a string field type.
	StringFieldType FieldType = "STRING"
	// BytesFieldType is a bytes field type.
	BytesFieldType FieldType = "BYTES"
	// IntegerFieldType is a integer field type.
	IntegerFieldType FieldType = "INTEGER"
	// FloatFieldType is a float field type.
	FloatFieldType FieldType = "FLOAT"
	// BooleanFieldType is a boolean field type.
	BooleanFieldType FieldType = "BOOLEAN"
	// TimestampFieldType is a timestamp field type.
	TimestampFieldType FieldType = "TIMESTAMP"
	// RecordFieldType is a record field type. It is typically used to create columns with repeated or nested data.
	RecordFieldType FieldType = "RECORD"
	// DateFieldType is a date field type.
	DateFieldType FieldType = "DATE"
	// TimeFieldType is a time field type.
	TimeFieldType FieldType = "TIME"
	// DateTimeFieldType is a datetime field type.
	DateTimeFieldType FieldType = "DATETIME"
	// NumericFieldType is a numeric field type. Numeric types include integer types, floating point types and the
	// NUMERIC data type.
	NumericFieldType FieldType = "NUMERIC"
	// GeographyFieldType is a string field type.  Geography types represent a set of points
	// on the Earth's surface, represented in Well Known Text (WKT) format.
	GeographyFieldType FieldType = "GEOGRAPHY"
	// BigNumericFieldType is a numeric field type that supports values of larger precision
	// and scale than the NumericFieldType.
	BigNumericFieldType FieldType = "BIGNUMERIC"
	// IntervalFieldType is a representation of a duration or an amount of time.
	IntervalFieldType FieldType = "INTERVAL"
	// JSONFieldType is a representation of a json object.
	JSONFieldType FieldType = "JSON"

	// Default case
	NOFieldType FieldType = ""
)

var (
	typeOfGoTime     = reflect.TypeOf(time.Time{})
	typeOfRat        = reflect.TypeOf(&big.Rat{})
	typeOfByteSlice  = reflect.TypeOf([]byte{})
	typeOfJSONNumber = reflect.TypeOf(json.Number(""))
)
