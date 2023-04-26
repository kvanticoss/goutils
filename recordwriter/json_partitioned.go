package recordwriter

import (
	"fmt"
	"path"

	"github.com/kvanticoss/goutils/iterator"
	"github.com/kvanticoss/goutils/keyvaluelist"
	"github.com/kvanticoss/goutils/writerfactory"

	jsoniter "github.com/json-iterator/go"
)

// NewLineJSONPartitioned extracts possible partitions from the records yielded by the iterator
// and writes them to the cache (and underlying writer) under the key {record.GetPartitions().ToPartitionKey()}/unsorted_records_s{suffix}.json
func NewLineJSONPartitioned(
	it iterator.RecordIterator,
	wf writerfactory.WriterFactory,
	pathBuilder func(record interface{}) string,
) error {
	var record interface{}
	var err error

	if pathBuilder == nil {
		pathBuilder = DefaultPathbuilder
	}

	for record, err = it(); err == nil; record, err = it() {
		d, err := jsoniter.ConfigCompatibleWithStandardLibrary.Marshal(record)
		if err != nil {
			return err
		}

		path := pathBuilder(record)
		writer, err := wf(path)
		if err != nil {
			return err
		}
		if _, err := writer.Write(append(d, jsonRecordDelimiter...)); err != nil {
			return err
		}
	}
	return err
}

// DefaultPathbuilder builds a path from the GetPartitions
func DefaultPathbuilder(record interface{}) string {
	return path.Join(keyvaluelist.MaybePartitions(record), fmt.Sprintf("unsorted_records_s{suffix}.json"))
}
