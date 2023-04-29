package recordwriter

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/kvanticoss/goutils/iterator"
	"github.com/kvanticoss/goutils/iterator/test_utils"
	"github.com/kvanticoss/goutils/writerfactory"
)

func TestNewLineJSONBatched(t *testing.T) {
	type args[T any] struct {
		it               iterator.RecordIterator[T]
		maxBytesParBatch int
		gzip             bool
		base_name        string
	}
	tests := []struct {
		name        string
		args        args[interface{}]
		wantErr     error
		writeErrors func(db map[string]*bytes.Buffer) error
	}{
		{
			name:    "write_all_no_limit",
			wantErr: iterator.ErrIteratorStop,
			args: args[interface{}]{
				base_name: "test",
				it: test_utils.DummyIteratorFromArr([]interface{}{
					map[string]interface{}{"a": "a1", "b": "b1", "c": "c1", "d": []string{"arr1", "arr2"}},
					map[string]interface{}{"a": "a2", "b": "b2", "c": "c2", "d": []string{"arr1", "arr2"}},
				}),
				maxBytesParBatch: -1,
			},
			writeErrors: func(db map[string]*bytes.Buffer) error {
				excpectedOutput := `{"a":"a1","b":"b1","c":"c1","d":["arr1","arr2"]}` + "\n" +
					`{"a":"a2","b":"b2","c":"c2","d":["arr1","arr2"]}` + "\n"
				if db["test_000000.ndjson"].String() != excpectedOutput {
					return fmt.Errorf("unexpected output: %s", db["test_000000.ndjson"].String())
				}
				return nil
			},
		},
		{
			name:    "write_all_small_limit",
			wantErr: iterator.ErrIteratorStop,
			args: args[interface{}]{
				base_name: "test",
				it: test_utils.DummyIteratorFromArr([]interface{}{
					map[string]interface{}{"a": "a1", "b": "b1", "c": "c1", "d": []string{"arr1", "arr2"}},
					map[string]interface{}{"a": "a2", "b": "b2", "c": "c2", "d": []string{"arr1", "arr2"}},
					map[string]interface{}{"a": "a3", "b": "b3", "c": "c3", "d": []string{"arr1", "arr2"}},
				}),
				maxBytesParBatch: 1, // not enough for 1 record => 1 record per batch
			},
			writeErrors: func(db map[string]*bytes.Buffer) error {
				excpectedOutput_0 := `{"a":"a1","b":"b1","c":"c1","d":["arr1","arr2"]}` + "\n"
				excpectedOutput_1 := `{"a":"a2","b":"b2","c":"c2","d":["arr1","arr2"]}` + "\n"
				excpectedOutput_2 := `{"a":"a3","b":"b3","c":"c3","d":["arr1","arr2"]}` + "\n"
				if db["test_000000.ndjson"].String() != excpectedOutput_0 {
					return fmt.Errorf("test_000000.ndjson: unexpected output: %s", db["test_000000.ndjson"].String())
				}
				if db["test_000001.ndjson"].String() != excpectedOutput_1 {
					return fmt.Errorf("test_000001.ndjson: unexpected output: %s", db["test_000001.ndjson"].String())
				}
				if db["test_000002.ndjson"].String() != excpectedOutput_2 {
					return fmt.Errorf("test_000001.ndjson: unexpected output: %s", db["test_000001.ndjson"].String())
				}
				return nil
			},
		},
		{
			name:    "write_all_medium_limit",
			wantErr: iterator.ErrIteratorStop,
			args: args[interface{}]{
				base_name: "test",
				it: test_utils.DummyIteratorFromArr([]interface{}{
					map[string]interface{}{"a": "a1", "b": "b1", "c": "c1", "d": []string{"arr1", "arr2"}},
					map[string]interface{}{"a": "a2", "b": "b2", "c": "c2", "d": []string{"arr1", "arr2"}},
					map[string]interface{}{"a": "a3", "b": "b3", "c": "c3", "d": []string{"arr1", "arr2"}},
				}),
				maxBytesParBatch: 60, // enough for 1 record but not two => 2 record per batch
			},
			writeErrors: func(db map[string]*bytes.Buffer) error {
				excpectedOutput_0 := `{"a":"a1","b":"b1","c":"c1","d":["arr1","arr2"]}` + "\n"
				excpectedOutput_1 := `{"a":"a2","b":"b2","c":"c2","d":["arr1","arr2"]}` + "\n"
				excpectedOutput_2 := `{"a":"a3","b":"b3","c":"c3","d":["arr1","arr2"]}` + "\n"
				if db["test_000000.ndjson"].String() != excpectedOutput_0+excpectedOutput_1 {
					return fmt.Errorf("test_000000.ndjson: unexpected output: %s", db["test_000000.ndjson"].String())
				}
				if db["test_000001.ndjson"].String() != excpectedOutput_2 {
					return fmt.Errorf("test_000001.ndjson: unexpected output: %s", db["test_000001.ndjson"].String())
				}
				return nil
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, wf := writerfactory.GetMemoryWriterFactory()

			if err := NewLineJSONBatched(tt.args.it, wf, tt.args.maxBytesParBatch, tt.args.base_name); err != nil && err != tt.wantErr {
				t.Errorf("NewLineJSONBatched() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.writeErrors != nil {
				if err := tt.writeErrors(db); err != nil {
					t.Errorf("NewLineJSONBatched() writeErrors.error = %v", err)
				}
			}
		})
	}
}
