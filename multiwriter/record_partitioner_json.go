package multiwriter

import (
	"context"
	"log"
	"os"
	"sync"
	"time"

	"github.com/kvanticoss/goutils/eioutil"
	"github.com/kvanticoss/goutils/iterator"
	"github.com/kvanticoss/goutils/gzip"

	jsoniter "github.com/json-iterator/go"
)

const maxTreeSize = 10000
const maxOpenPartitions = 250

var recordDelimiter = []byte("\n")

// StreamJSONRecords reads records from a recordIterator and writes them to the WriterFactory provided writers
// Any record that provides GetPartions() method will have their partions expanded to the path.
//
// NOTE: The RecordIterator MUST NOT re-use the same data underlying datastructure IF the record implements the Lesser interface
// since implementing the Lesser interface implies that records should be saved in sorted order. To achieve this effect
// a copy of the last record (per partition) must be keept to compare with. If the record-reference is volatile between
// invocations of the RecordIterator any comparisson with be of limited value.
//
// e.g The following iterator will NOT work
//  s := struct{i int}{}
//  func() interface{} {
//    s.i++
//    return s // returns the same reference in each invokation; As interfaces hold pointers any struct will be cast to a pointer and not a call by value
//  }
//
// while this WILL WORK
//  s := struct{i int}{}
//  func() interface{} {
//    sCopy := s
//    sCopy.i++
//    return sCopy // returns a reference to a new instance of the struct
//  }
//
func StreamJSONRecords(
	ctx context.Context,
	writerFactory WriterFactory,
	ri iterator.RecordIterator,
	bucketTTL time.Duration,
) (err error) {
	rs, err := NewRecordsStreamer(ctx, writerFactory, bucketTTL, 150)
	if err != nil {
		return err
	}
	defer rs.Close()
	var record interface{}
	for record, err = ri(); err == nil; record, err = ri() {
		if err := rs.WriteRecord(record); err != nil {
			return err
		}
	}
	return err
}

// JSONRecordStreamer provides partitioned writing of records to a store
type JSONRecordStreamer struct {
	ctx context.Context
	mwc *Cache

	partitions   map[string]*SortedRecordWriter
	clusterMutex sync.Mutex

	ttl           time.Duration
	maxPartitions int

	hostName string
}

// NewRecordsStreamer creates an Json Record writer. It will write each record to a hadoop partitioned path (if the record implements GetPartitions()).
// All data will be gzip:ed before written to the WriterFactory-provided Writer.
func NewRecordsStreamer(ctx context.Context, writerFactory WriterFactory, ttl time.Duration, maxPartitions int) (rs *JSONRecordStreamer, err error) {
	host, err := os.Hostname()
	if err != nil {
		return nil, err
	}

	gzipWriterFactory := func(path string) (eioutil.WriteCloser, error) {
		w, err := WriterFactory(path + ".gz")
		if err != nil {
			return nil, err
		}
		return gzip.NewWriter(w), err
	}

	return &JSONRecordStreamer{
		ctx: ctx,
		mwc: NewCache(ctx, gzipWriterFactory, ttl),

		partitions:   make(map[string]*SortedRecordWriter),
		clusterMutex: sync.Mutex{},

		ttl:           ttl,
		maxPartitions: maxPartitions,

		hostName: host,
	}, nil
}

// Close will flush any records and close all the underlying writers
func (rs *JSONRecordStreamer) Close() error {
	rs.clusterMutex.Lock()
	defer rs.clusterMutex.Unlock()

	for partitionPath, sortedPartition := range rs.partitions {
		log.Printf("Closing sorted partition @ %s", partitionPath)
		sortedPartition.Close()
	}

	var allErrors MultiError
	log.Printf("Closing MWC")
	if err := rs.mwc.Close(); err != nil {
		allErrors = append(allErrors, err)
	}
	log.Printf("Done closing MWC")

	return allErrors.MaybeError()
}

// WriteRecord will extract any partitions (assumes record implementes keyvaluelist.PartitionGetter) and write the record
// as a new-line-delimited-JSON (gzip:ed) byte-stream. It is important that any record which implements the Lesser-interface
// have a distinct sort-order. If two records A & B fullfills A.Less(B) == B.Less(A) it is interpreted as they being the same
// record. At such a time it is undefined if A OR B OR both are written (timing dependent).
//
// Writing a nil-record will return a nil-error but have no effect
func (rs *JSONRecordStreamer) WriteRecord(record interface{}) error {
	if record == nil {
		return ErrInvalidRecord
	}

	maybePartitions := rs.maybePartitions(record)

	recAsLesser, recIsComparable := record.(Lesser)
	if !recIsComparable {
		rs.writeRecord(maybePartitions+"data_"+rs.hostName+"_"+"b0_{suffix}", record)
	}

	SRW, err := rs.getSertSortedWriter(maybePartitions)
	if err != nil {
		return err
	}
	return SRW.WriteRecord(recAsLesser)
}

func (rs *JSONRecordStreamer) reducePartitions() error {
	// TODO: Add reduction strategies (like close LRU, or random)
	return ErrTooManyPartitions
}

func (rs *JSONRecordStreamer) getSertSortedWriter(partition string) (*SortedRecordWriter, error) {
	SRW, exists := rs.partitions[partition]
	if !exists {

		if len(rs.partitions) > rs.maxPartitions {
			if err := rs.reducePartitions(); err != nil {
				return nil, err
			}
		}

		log.Printf("Creating INITIAL sortTree for path %s", partition)
		SRW = NewSortedRecordWriter(rs.ctx, func(bucketID string, record Lesser) {
			path := partition + "data_" + rs.hostName + "_" + "b" + bucketID + "_{suffix}"
			err := rs.writeRecord(path, record)
			if err != nil {
				panic(err)
			}
		}, WithMaxCacheIdleTime(rs.ttl))

		rs.partitions[partition] = SRW
	}
	return SRW, nil
}

func (rs *JSONRecordStreamer) writeRecord(path string, record interface{}) error {
	if record == nil {
		return ErrInvalidRecord
	}

	if btl, ok := record.(iterator.btreeLesser); ok {
		record = btl.Lesser
	}

	d, err := jsoniter.ConfigCompatibleWithStandardLibrary.Marshal(record)
	if err != nil {
		return err
	}
	_, err = rs.mwc.Write(path+".json", append(d, recordDelimiter...))
	return err
}

func (rs *JSONRecordStreamer) maybePartitions(record interface{}) string {
	if recordPartitioner, ok := record.(keyvaluelist.PartitionGetter); ok {
		maybeParts, err := recordPartitioner.GetPartions()
		if err != nil {
			return ""
		}
		return maybeParts.ToPartitionKey() + "/"
	}
	return ""
}
