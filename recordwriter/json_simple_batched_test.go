package recordwriter

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
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
			name:    "write_all_no_limit_gzip",
			wantErr: iterator.ErrIteratorStop,
			args: args[interface{}]{
				base_name: "test",
				it: test_utils.DummyIteratorFromArr([]interface{}{
					map[string]interface{}{"a": "a1", "b": "b1", "c": "c1", "d": []string{"arr1", "arr2"}},
					map[string]interface{}{"a": "a2", "b": "b2", "c": "c2", "d": []string{"arr1", "arr2"}},
				}),
				maxBytesParBatch: -1,
				gzip:             true,
			},
			writeErrors: func(db map[string]*bytes.Buffer) error {

				b, err := readGzipbuffer(db["test_000000.ndjson.gz"])
				if err != nil {
					return fmt.Errorf("failed to read gzip reader: %w", err)
				}
				excpectedOutput := `{"a":"a1","b":"b1","c":"c1","d":["arr1","arr2"]}` + "\n" +
					`{"a":"a2","b":"b2","c":"c2","d":["arr1","arr2"]}` + "\n"
				if string(b) != excpectedOutput {
					return fmt.Errorf("unexpected output: %s", string(b))
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
		{
			name:    "write_all_medium_limit_gzip",
			wantErr: iterator.ErrIteratorStop,
			args: args[interface{}]{
				base_name: "test",
				it: test_utils.DummyIteratorFromArr([]interface{}{
					map[string]interface{}{"a": "a1", "b": "b1", "c": "c1", "d": []string{"arr1", "arr2"}},
					map[string]interface{}{"a": "a2", "b": "b2", "c": "c2", "d": []string{"arr1", "arr2"}},
					map[string]interface{}{"a": "a3", "b": "b3", "c": "c3", "d": []string{"arr1", "arr2"}},
				}),
				maxBytesParBatch: 60, // enough for 1 record but not two => 2 record per batch
				gzip:             true,
			},
			writeErrors: func(db map[string]*bytes.Buffer) error {

				excpectedOutput_0 := `{"a":"a1","b":"b1","c":"c1","d":["arr1","arr2"]}` + "\n"
				excpectedOutput_1 := `{"a":"a2","b":"b2","c":"c2","d":["arr1","arr2"]}` + "\n"
				excpectedOutput_2 := `{"a":"a3","b":"b3","c":"c3","d":["arr1","arr2"]}` + "\n"

				b1, err := readGzipbuffer(db["test_000000.ndjson.gz"])
				if err != nil {
					return fmt.Errorf("failed to read gzip reader: %w", err)
				}
				if string(b1) != excpectedOutput_0+excpectedOutput_1 {
					return fmt.Errorf("test_000000.ndjson: unexpected output: %s", string(b1))
				}

				b2, err := readGzipbuffer(db["test_000001.ndjson.gz"])
				if err != nil {
					return fmt.Errorf("failed to read gzip reader: %w", err)
				}
				if string(b2) != excpectedOutput_2 {
					return fmt.Errorf("test_000001.ndjson: unexpected output: %s", string(b2))
				}
				return nil
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, wf := writerfactory.GetMemoryWriterFactory()

			if err := NewLineJSONPartitionedBySize(tt.args.it, wf, tt.args.maxBytesParBatch, tt.args.base_name, tt.args.gzip); err != nil && err != tt.wantErr {
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

func readGzipbuffer(b *bytes.Buffer) (string, error) {
	gr, err := gzip.NewReader(bytes.NewReader(b.Bytes()))
	if err != nil {
		return "", fmt.Errorf("new gzipReader err: %v", err)
	}
	defer gr.Close()
	var buf bytes.Buffer
	if _, err := io.Copy(&buf, gr); err != nil {
		return "", fmt.Errorf("read gzip err: %v", err)
	}
	return buf.String(), nil
}
