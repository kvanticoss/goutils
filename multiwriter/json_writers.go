package multiwriter

import (
	"fmt"

	"github.com/kvanticoss/goutils/iterator"
	"github.com/kvanticoss/goutils/keyvaluelist"

	jsoniter "github.com/json-iterator/go"
)

// StreamJSONBySortedPartitions extracts possible partitions from the records yeilded by the sorted iterator
// and writes them to the cache (and underlying writer) with the cluster-ids which guarrantees sorted order within each cluster
func StreamJSONBySortedPartitions(
	it iterator.LesserIteratorClustered,
	cache *Cache,
	pathBuilder func(record interface{}, partition int) string,
) error {
	recordDelimiter := []byte("\n")

	var partition int
	var record iterator.Lesser
	var err error

	for partition, record, err = it(); err == nil; partition, record, err = it() {
		d, err := jsoniter.ConfigCompatibleWithStandardLibrary.Marshal(record)
		if err != nil {
			return err
		}

		path := pathBuilder(record, partition)

		if _, err := cache.Write(path, append(d, recordDelimiter...)); err != nil {
			return err
		}
	}
	return err
}

// DefaultPathbuilder builds a path from the GetPartions + an incremntal partition id.
func DefaultPathbuilder(record interface{}, partition int) string {
	return keyvaluelist.MaybePartitions(record) + fmt.Sprintf("records_p%04d_s{suffix}.json", partition)
}

// StreamJSONByPartitions extracts possible partitions from the records yeilded by the iterator
// and writes them to the cache (and underlying writer)
func StreamJSONByPartitions(
	it iterator.RecordIterator,
	cache *Cache,
) error {
	recordDelimiter := []byte("\n")

	var record interface{}
	var err error

	for record, err = it(); err == nil; record, err = it() {
		d, err := jsoniter.ConfigCompatibleWithStandardLibrary.Marshal(record)
		if err != nil {
			return err
		}

		path := keyvaluelist.MaybePartitions(record) + "records_s{suffix}.json"
		if _, err := cache.Write(path, append(d, recordDelimiter...)); err != nil {
			return err
		}
	}
	return err
}
