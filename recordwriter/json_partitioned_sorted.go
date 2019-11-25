package recordwriter

import (
	"fmt"
	"path"

	"github.com/kvanticoss/goutils/iterator"
	"github.com/kvanticoss/goutils/keyvaluelist"
	"github.com/kvanticoss/goutils/writercache"

	jsoniter "github.com/json-iterator/go"
)

// NewLineJSONPartitionedClustered extracts possible partitions from the records yeilded by the sorted iterator
// and writes them to the cache (and underlying writer) with the cluster-ids which guarrantees sorted order within each cluster
func NewLineJSONPartitionedClustered(
	it iterator.LesserIteratorClustered,
	wf *writercache.Cache,
	pathBuilder func(record interface{}, partition int) string,
) error {
	var cluster int
	var record iterator.Lesser
	var err error

	if pathBuilder == nil {
		pathBuilder = DefaultPathbuilder
	}

	for cluster, record, err = it(); err == nil; cluster, record, err = it() {
		d, err := jsoniter.ConfigCompatibleWithStandardLibrary.Marshal(record)
		if err != nil {
			return err
		}

		path := pathBuilder(record, cluster)
		writer, err := wf.GetWriter(path)
		if err != nil {
			return err
		}
		if _, err := writer.Write(append(d, jsonRecordDelimiter...)); err != nil {
			return err
		}
	}
	return err
}

// DefaultPathbuilder builds a path from the GetPartitions + an incremntal partition id.
func DefaultPathbuilder(record interface{}, partition int) string {
	return path.Join(keyvaluelist.MaybePartitions(record), fmt.Sprintf("sorted_records_p%04d_s{suffix}.json", partition))
}
