package recordwriter

import (
	"fmt"
	"path"

	"github.com/kvanticoss/goutils/v2/internal/iterator"
	"github.com/kvanticoss/goutils/v2/keyvaluelist"
	"github.com/kvanticoss/goutils/v2/writerfactory"

	jsoniter "github.com/json-iterator/go"
)

// NewLineJSONClustered extracts possible partitions from the records yielded by the sorted iterator
// and writes them to the cache (and underlying writer) with the cluster-ids which guarrantees sorted order within each cluster
// Note the writers will NOT be closed by NewLineJSONClustered; that should be handled after it has been returned; as such it is
// usefull to use a writercache.Cache{}
func NewLineJSONClustered(
	it iterator.LesserIteratorClustered,
	wf writerfactory.WriterFactory,
	pathBuilder func(record interface{}, partition int) string,
) error {
	var cluster int
	var record iterator.Lesser
	var err error

	if pathBuilder == nil {
		pathBuilder = DefaultClusteredPathbuilder
	}

	for cluster, record, err = it(); err == nil; cluster, record, err = it() {
		d, err := jsoniter.ConfigCompatibleWithStandardLibrary.Marshal(record)
		if err != nil {
			return err
		}

		path := pathBuilder(record, cluster)
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

// DefaultClusteredPathbuilder builds a path from the GetPartitions + an incremntal partition id.
func DefaultClusteredPathbuilder(record interface{}, partition int) string {
	return path.Join(keyvaluelist.MaybePartitions(record), fmt.Sprintf("sorted_records_p%04d_s{suffix}.json", partition))
}
