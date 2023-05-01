package bigquery_schema

import (
	"errors"
	"fmt"
	"log"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/kvanticoss/goutils/iterator"
)

// InferBQSchema infers a bigquery schema from records yielded by the iterator.
// records are assumed to originate from JSON parsed data and as such strings
// can contain dates, timestamps, integers, floats.
// that type (e.g. "2020-01-01" will be inferred to be a date).
// this tries to mimic how BigQuery infers schema from JSON data.
// if two records have conflicting types for the same field, the field will be
// inferred to be of the type which can capture both values; most often this is
// "STRING" or "JSON". Conflicting types such as bool and float will be inferred
// as "JSON".
func InferBQSchema[T any](it iterator.RecordIterator[T]) (
	it2 iterator.RecordIterator[T],
	getSchema func() TableFieldSchema,
	err error,
) {

	schema := newSchema("")
	resGetSchema := func() TableFieldSchema {
		schema.sortFields()
		return *schema
	}

	resIt := func() (T, error) {
		r, err := it()
		if err == nil {
			schema.updateInference(r)
		}
		return r, err
	}

	return resIt, resGetSchema, nil
}

var ErrInvalidRootType = errors.New("invalid root type, must be map or struct")

func newSchema(mode string) *TableFieldSchema {
	return &TableFieldSchema{
		Mode:             mode,
		Fields:           []*TableFieldSchema{},
		fieldIndexByName: map[string]int{},
	}
}

func (i *TableFieldSchema) sortFields() {
	sort.Slice(i.Fields, func(ii, jj int) bool {
		return i.Fields[ii].Name < i.Fields[jj].Name
	})

	i.fieldIndexByName = map[string]int{}

	for ii, f := range i.Fields {
		i.fieldIndexByName[f.Name] = ii
		if f.Type == string(RecordFieldType) {
			f.sortFields()
		}
	}
}

func (i *TableFieldSchema) updateInference(r any) {
	if i.err != nil {
		log.Print("TableFieldSchema.AddToInference called after error")
		return
	}

	t := reflect.TypeOf(r)

	switch t {
	case typeOfByteSlice:
		i.mergeTypes(BytesFieldType)

	case typeOfGoTime:
		i.mergeTypes(TimestampFieldType)

	case typeOfRat:
		i.mergeTypes(BigNumericFieldType)
	}

	switch t.Kind() {
	case reflect.Bool:
		i.mergeTypes(BooleanFieldType)

	case reflect.String:
		strVal := r.(string)
		// check if strVal is a valid date
		if isDate(strVal) {
			i.mergeTypes(DateFieldType)
		} else if isTimestamp(strVal) {
			i.mergeTypes(TimestampFieldType)
		} else if isDateTime(strVal) {
			i.mergeTypes(DateTimeFieldType)
		} else if isInteger(strVal) {
			i.mergeTypes(IntegerFieldType)
		} else if isFloat(strVal) {
			i.mergeTypes(FloatFieldType)
		} else if isBoolean(strVal) {
			i.mergeTypes(BooleanFieldType)
		} else {
			i.mergeTypes(StringFieldType)
		}

	case reflect.Float32, reflect.Float64:
		i.mergeTypes(FloatFieldType)

	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		i.mergeTypes(IntegerFieldType)

	// Handle pointers
	case reflect.Ptr:
		i.updateInference(reflect.ValueOf(r).Elem())

	// List types
	case reflect.Array, reflect.Slice:
		if i.Mode != "REPEATED" && i.Mode != "" {
			i.err = fmt.Errorf("inconsistent mode was :%s", i.Mode)
			i.Type = string(JSONFieldType)
			return
		}
		i.Mode = "REPEATED"
		v := reflect.ValueOf(r)
		for j := 0; j < v.Len(); j++ {
			i.updateInference(v.Index(j).Interface())
		}

	// Object types
	case reflect.Map:
		if t.Key().Kind() != reflect.String {
			i.err = ErrInvalidRootType
			i.Type = string(JSONFieldType)
			return
		}
		i.Type = string(RecordFieldType)
		v := reflect.ValueOf(r)
		for _, key := range v.MapKeys() {
			i.addInferenceToField(key.String(), v.MapIndex(key).Interface())
		}

	case reflect.Struct:
		if t.Key().Kind() != reflect.String {
			i.err = ErrInvalidRootType
			i.Type = string(JSONFieldType)
			return
		}
		i.Type = string(RecordFieldType)
		v := reflect.ValueOf(r)
		for _, key := range v.MapKeys() {
			i.addInferenceToField(key.String(), v.MapIndex(key).Interface())
		}
	}

	if i.Mode == "" {
		i.Mode = "NULLABLE"
	}
}

var mergeRules = map[FieldType]map[FieldType]FieldType{
	BytesFieldType: {
		BytesFieldType: BytesFieldType,
		//TimestampFieldType: null,
		//BigNumericFieldType: null,
		//BooleanFieldType: null,
		StringFieldType: StringFieldType,
		//FloatFieldType: null,
		//IntegerFieldType: null,
	},
	TimestampFieldType: {
		//BytesFieldType: null,
		TimestampFieldType: TimestampFieldType,
		//BigNumericFieldType: null,
		//BooleanFieldType: null,
		StringFieldType: StringFieldType,
		//FloatFieldType: null,
		//IntegerFieldType: null,
		DateFieldType:     StringFieldType,
		DateTimeFieldType: StringFieldType,
	},
	DateFieldType: {
		//BytesFieldType: null,
		//BigNumericFieldType: null,
		//BooleanFieldType: null,
		StringFieldType: StringFieldType,
		//FloatFieldType: null,
		//IntegerFieldType: null,
		TimestampFieldType: StringFieldType,
		DateFieldType:      DateFieldType,
		DateTimeFieldType:  StringFieldType,
	},
	DateTimeFieldType: {
		//BytesFieldType: null,
		//BigNumericFieldType: null,
		//BooleanFieldType: null,
		StringFieldType: StringFieldType,
		//FloatFieldType: null,
		//IntegerFieldType: null,
		TimestampFieldType: StringFieldType,
		DateFieldType:      StringFieldType,
		DateTimeFieldType:  DateTimeFieldType,
	},
	BigNumericFieldType: {
		//BytesFieldType: null,
		//TimestampFieldType: null,
		BigNumericFieldType: BigNumericFieldType,
		BooleanFieldType:    BigNumericFieldType,
		StringFieldType:     StringFieldType,
		FloatFieldType:      BigNumericFieldType,
		IntegerFieldType:    BigNumericFieldType,
	},
	BooleanFieldType: {
		//BytesFieldType: null,
		//TimestampFieldType: null,
		BigNumericFieldType: JSONFieldType,
		BooleanFieldType:    BooleanFieldType,
		StringFieldType:     JSONFieldType,
		FloatFieldType:      JSONFieldType,
		IntegerFieldType:    JSONFieldType,
	},
	StringFieldType: {
		BytesFieldType:      StringFieldType,
		TimestampFieldType:  StringFieldType,
		NumericFieldType:    JSONFieldType,
		DateFieldType:       StringFieldType,
		DateTimeFieldType:   StringFieldType,
		BigNumericFieldType: StringFieldType,
		BooleanFieldType:    JSONFieldType,
		StringFieldType:     StringFieldType,
		FloatFieldType:      JSONFieldType,
		IntegerFieldType:    JSONFieldType,
	},
	FloatFieldType: {
		//BytesFieldType: null,
		//TimestampFieldType: null,
		BigNumericFieldType: BigNumericFieldType,
		BooleanFieldType:    JSONFieldType,
		StringFieldType:     StringFieldType,
		FloatFieldType:      FloatFieldType,
		IntegerFieldType:    FloatFieldType,
	},
	NumericFieldType: {
		//BytesFieldType: null,
		//TimestampFieldType: null,
		BigNumericFieldType: BigNumericFieldType,
		BooleanFieldType:    JSONFieldType,
		StringFieldType:     StringFieldType,
		FloatFieldType:      FloatFieldType,
		IntegerFieldType:    FloatFieldType,
	},
	IntegerFieldType: {
		//BytesFieldType: null,
		//TimestampFieldType: null,
		BigNumericFieldType: BigNumericFieldType,
		BooleanFieldType:    JSONFieldType,
		StringFieldType:     StringFieldType,
		FloatFieldType:      FloatFieldType,
		IntegerFieldType:    IntegerFieldType,
	},
	JSONFieldType: {
		JSONFieldType:       JSONFieldType,
		BytesFieldType:      JSONFieldType,
		TimestampFieldType:  JSONFieldType,
		NumericFieldType:    JSONFieldType,
		DateFieldType:       JSONFieldType,
		DateTimeFieldType:   JSONFieldType,
		BigNumericFieldType: JSONFieldType,
		BooleanFieldType:    JSONFieldType,
		StringFieldType:     JSONFieldType,
		FloatFieldType:      JSONFieldType,
		IntegerFieldType:    JSONFieldType,
	},
	NOFieldType: {
		JSONFieldType:       JSONFieldType,
		BytesFieldType:      BytesFieldType,
		TimestampFieldType:  TimestampFieldType,
		NumericFieldType:    NumericFieldType,
		DateFieldType:       DateFieldType,
		DateTimeFieldType:   DateTimeFieldType,
		BigNumericFieldType: BigNumericFieldType,
		BooleanFieldType:    BooleanFieldType,
		StringFieldType:     StringFieldType,
		FloatFieldType:      FloatFieldType,
		IntegerFieldType:    IntegerFieldType,
		RecordFieldType:     RecordFieldType,
	},
	RecordFieldType: {
		RecordFieldType: RecordFieldType,

		JSONFieldType:       JSONFieldType,
		TimestampFieldType:  JSONFieldType,
		NumericFieldType:    JSONFieldType,
		DateFieldType:       JSONFieldType,
		DateTimeFieldType:   JSONFieldType,
		BigNumericFieldType: JSONFieldType,
		BooleanFieldType:    JSONFieldType,
		StringFieldType:     JSONFieldType,
		FloatFieldType:      JSONFieldType,
		IntegerFieldType:    JSONFieldType,
	},
}

func (i *TableFieldSchema) mergeTypes(newType FieldType) {
	currentType := i.Type
	newTypeStr := string(newType)

	// Should be normal case
	if currentType == newTypeStr {
		return
	}

	fromTypeIndex, ok := mergeRules[FieldType(currentType)]
	if !ok {
		i.err = errors.New("invalid state; unknown type: " + currentType)
		i.Type = string(JSONFieldType)
		return
	}

	toType, ok := fromTypeIndex[newType]
	if !ok {
		i.err = errors.New("invalid state transition " + currentType + "->" + newTypeStr)
		i.Type = string(JSONFieldType)
		return
	}

	i.Type = string(toType)
}

func (i *TableFieldSchema) addInferenceToField(fieldName string, r any) {
	index, ok := i.fieldIndexByName[fieldName]
	if !ok {
		newSchema := newSchema("")
		newSchema.Name = fieldName
		i.Fields = append(
			i.Fields,
			newSchema,
		)
		i.fieldIndexByName[fieldName] = len(i.Fields) - 1
		index = i.fieldIndexByName[fieldName]
	}

	i.Fields[index].updateInference(r)
}

// isDate check if the shape matches a date without doing proper date parsing.
func isDate(strVal string) bool {
	if len(strVal) != 10 {
		return false
	}
	if strVal[4] != '-' || strVal[7] != '-' {
		return false
	}
	if v, err := strconv.Atoi(strVal[0:4]); err != nil || v < 0 || v > 9999 {
		return false
	}
	if v, err := strconv.Atoi(strVal[5:7]); err != nil || v < 1 || v > 12 {
		return false
	}
	// Can technically cause false positives, but we'll live with it
	if v, err := strconv.Atoi(strVal[8:10]); err != nil || v < 1 || v > 31 {
		return false
	}
	return true
}

func isDateTime(strVal string) bool {
	// Let's start with some quick checks
	if len(strVal) < 19 {
		return false
	}
	if strVal[4] != '-' || strVal[7] != '-' {
		return false
	}
	// handle cases without T but with space
	if strVal[10] == ' ' {
		tmp := []rune(strVal)
		tmp[10] = 'T'
		strVal = string(tmp)
	}
	if strVal[10] != 'T' {
		return false
	}
	if strVal[13] != ':' || strVal[16] != ':' {
		return false
	}
	if v, err := strconv.Atoi(strVal[0:4]); err != nil || v < 0 || v > 9999 {
		return false
	}
	if v, err := strconv.Atoi(strVal[5:7]); err != nil || v < 1 || v > 12 {
		return false
	}
	// Can technically cause false positives, but we'll live with it
	if v, err := strconv.Atoi(strVal[8:10]); err != nil || v < 1 || v > 31 {
		return false
	}
	// check hour
	if v, err := strconv.Atoi(strVal[11:13]); err != nil || v < 0 || v > 23 {
		return false
	}
	// check minute
	if v, err := strconv.Atoi(strVal[14:16]); err != nil || v < 0 || v > 59 {
		return false
	}
	// check second
	if v, err := strconv.Atoi(strVal[17:19]); err != nil || v < 0 || v > 59 {
		return false
	}

	return true
}

func isTimestamp(strVal string) bool {
	// Let's start with some quick checks
	if len(strVal) < 19 {
		return false
	}
	if strVal[4] != '-' || strVal[7] != '-' {
		return false
	}
	// handle cases without T but with space
	if strVal[10] == ' ' {
		tmp := []rune(strVal)
		tmp[10] = 'T'
		strVal = string(tmp)
	}
	if strVal[10] != 'T' {
		return false
	}
	if strVal[13] != ':' || strVal[16] != ':' {
		return false
	}
	if _, err := time.Parse(time.RFC3339, strVal); err != nil {
		return false
	}
	return true
}

func isFloat(strVal string) bool {
	if _, err := strconv.ParseFloat(strVal, 64); err != nil {
		return false
	}
	return true
}

func isInteger(strVal string) bool {
	if _, err := strconv.ParseInt(strVal, 10, 64); err != nil {
		return false
	}
	return true
}

func isBoolean(strVal string) bool {
	strVal = strings.ToLower(strVal)
	if strVal == "true" || strVal == "false" {
		return true
	}
	if strVal == "1" || strVal == "0" {
		return true
	}
	if strVal == "t" || strVal == "f" {
		return true
	}
	return false
}
