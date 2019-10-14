package multiwriter

import (
	"context"
	"fmt"

	"github.com/kvanticoss/goutils/iterator"
	"github.com/kvanticoss/goutils/keyvaluelist"

	jsoniter "github.com/json-iterator/go"
)

// StreamJSONBySortedPartitions extracts possible partitions from the records yeilded by the sorted iterator
// and writes them to the cache (and underlying writer) with the cluster-ids which guarrantees sorted order within each cluster
func StreamJSONBySortedPartitions(
	ctx context.Context,
	it iterator.LesserIteratorClustered,
	cache *Cache,
) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	recordDelimiter := []byte("\n")

	var partition int
	var record iterator.Lesser
	var err error

	for partition, record, err = it(); err == nil; partition, record, err = it() {
		d, err := jsoniter.ConfigCompatibleWithStandardLibrary.Marshal(record)
		if err != nil {
			fmt.Println(err.Error())
			return err
		}

		path := maybePartitions(record) + fmt.Sprintf("records_p%04d_s{suffix}.json", partition)

		if _, err := cache.Write(path, append(d, recordDelimiter...)); err != nil {
			return err
		}
	}
	return err
}

// StreamJSONByPartitions extracts possible partitions from the records yeilded by the iterator
// and writes them to the cache (and underlying writer)
func StreamJSONByPartitions(
	ctx context.Context,
	it iterator.RecordIterator,
	cache *Cache,
) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	recordDelimiter := []byte("\n")

	var record interface{}
	var err error

	for record, err = it(); err == nil; record, err = it() {
		d, err := jsoniter.ConfigCompatibleWithStandardLibrary.Marshal(record)
		if err != nil {
			return err
		}

		path := maybePartitions(record) + "records_s{suffix}.json"
		if _, err := cache.Write(path, append(d, recordDelimiter...)); err != nil {
			return err
		}
	}
	return err
}

func maybePartitions(record interface{}) string {
	if recordPartitioner, ok := record.(keyvaluelist.PartitionGetter); ok {
		maybeParts, err := recordPartitioner.GetPartions()
		if err != nil {
			return ""
		}
		return maybeParts.ToPartitionKey() + "/"
	}
	return ""
}
