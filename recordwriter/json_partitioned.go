package recordwriter

import (
	"github.com/kvanticoss/goutils/iterator"
	"github.com/kvanticoss/goutils/keyvaluelist"
	"github.com/kvanticoss/goutils/writerfactory"

	jsoniter "github.com/json-iterator/go"
)

// NewLineJSONPartitioned extracts possible partitions from the records yeilded by the iterator
// and writes them to the cache (and underlying writer) under the key {record.GetPartitions().ToPartitionKey()}/unsorted_records_s{suffix}.json
func NewLineJSONPartitioned(
	it iterator.RecordIterator,
	wf writerfactory.WriterFactory,
) error {
	var record interface{}
	var err error

	for record, err = it(); err == nil; record, err = it() {
		d, err := jsoniter.ConfigCompatibleWithStandardLibrary.Marshal(record)
		if err != nil {
			return err
		}

		path := keyvaluelist.MaybePartitions(record) + "unsorted_records_s{suffix}.json"

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
