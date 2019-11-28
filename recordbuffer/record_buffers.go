package recordbuffer

import (
	"github.com/kvanticoss/goutils/iterator"
)

// SortedRecordBuffers creates a simple LSM like buffer for records and uses the ReadWriteResetterFactory
// for byte storage of the records
type SortedRecordBuffers struct {
	partitions []*recordBuffer
	factory    ReadWriteResetterFactory
	newer      func() iterator.Lesser

	recordWriters []func(interface{}) (int, error)
	clusters      []iterator.Lesser
}

// NewSortedRecordBuffers creates a new SortedRecordBuffer using the underlying ReadWriteResetterFactory
func NewSortedRecordBuffers(factory ReadWriteResetterFactory, newer func() iterator.Lesser) *SortedRecordBuffers {
	return &SortedRecordBuffers{
		partitions: []*recordBuffer{},
		factory:    factory,
		newer:      newer,
	}
}

// LoadFromRecordIterator populates the buffer through a record iterator.
func (srb *SortedRecordBuffers) LoadFromRecordIterator(it iterator.RecordIterator) (int, error) {
	// Read all the records in "this folder" into the cache
	return srb.LoadFromLesserIterator(it.ToLesserIterator())
}

// LoadFromLesserIterator will add a single record to the buffer
func (srb *SortedRecordBuffers) LoadFromLesserIterator(it iterator.LesserIterator) (int, error) {
	// Read all the records in "this folder" into the cache
	var rec iterator.Lesser
	var err error
	var bytesWritten int
	for rec, err = it(); err == nil; rec, err = it() {
		// Ensured to be lesser due since it is produced by newer
		if n, err := srb.AddRecord(rec); err != nil {
			return bytesWritten, err
		} else {
			bytesWritten += n
		}
	}
	if err == iterator.ErrIteratorStop {
		return bytesWritten, nil
	}
	return bytesWritten, err
}

// AddRecord will add a single record to the buffer
func (srb *SortedRecordBuffers) AddRecord(record interface{}) (int, error) {
	nextVal, ok := record.(iterator.Lesser)
	if !ok {
		return 0, iterator.ErrNotLesser
	}
	return srb.AddLesser(nextVal)
}

// AddLesser will add a single record to the buffer
func (srb *SortedRecordBuffers) AddLesser(nextVal iterator.Lesser) (int, error) {
	var lastRecordInCluster iterator.Lesser
	var index int
	for index, lastRecordInCluster = range srb.clusters {
		if !nextVal.Less(lastRecordInCluster) {
			srb.clusters[index] = nextVal
			return srb.recordWriters[index](lastRecordInCluster)
		}
	}
	// A new cluster index might be needed
	srb.clusters = append(srb.clusters, nextVal)
	cache := srb.decoratedCacheFactory()
	srb.partitions = append(srb.partitions, cache)
	srb.recordWriters = append(srb.recordWriters, cache.WriteRecord)
	return cache.WriteRecord(lastRecordInCluster)

}

// GetSortedIterator returns an (sorted) interator for all the records stored in the buffer
func (srb *SortedRecordBuffers) GetSortedIterator() (iterator.LesserIterator, error) {
	var sortedIterators []iterator.LesserIterator
	for _, cache := range srb.partitions {
		sortedIterators = append(sortedIterators, cache.GetLesserIt(srb.newer))
	}
	return iterator.SortedLesserIterators(sortedIterators)
}

func (srb *SortedRecordBuffers) decoratedCacheFactory() *recordBuffer {
	return &recordBuffer{srb.factory()}
}
