package iterator

import (
	"sync"
	"time"

	"github.com/kvanticoss/goutils/keyvaluelist"
)

const maxLeakybuckets = 6

// NewBufferedClusterIteartor works like NewBufferedRecordIteratorBTree but returns the cluster-id the records was returned from
// with the guarrantee that within each cluster; records are always in sorted order. Good for handling out of order records.
// NOTE: to give this guarrantee a cache of records must be keep in memory effectively creating an memory leak. The best way
// to avoid this memory leak to grow is to reset the iterator from time to time. Fututre iterators might include a GC based on inactivity
func NewBufferedClusterIteartor(ri LesserIterator, bufferSize int) LesserIteratorClustered {
	clustersPartitions := map[string][]Lesser{}
	it := newBufferedClusterIteartor(ri, bufferSize)
	return func() (int, Lesser, error) {
		_, nextVal, err := it()
		if err != nil {
			return -1, nextVal, err
		}

		maybePartition := keyvaluelist.MaybePartitions(nextVal)
		clusters := clustersPartitions[maybePartition]

		for index, lastRecordInCluster := range clusters {
			if !nextVal.Less(lastRecordInCluster) {
				clusters[index] = nextVal
				return index, nextVal, nil
			}
		}

		clusters = append(clusters, nextVal)
		clustersPartitions[maybePartition] = clusters
		return len(clusters) - 1, nextVal, nil
	}
}

// newBufferedClusterIteartor introduces multiple leaky buckets of buffers (at most 6); The last (bucket id 6) is not guarranteed to be in order.
func newBufferedClusterIteartor(ri LesserIterator, bufferSize int) LesserIteratorClustered {
	clusters := []*BtreeSyncedLesser{}
	//it := NewBufferedRecordIteratorBTree(ri, bufferSize)

	var resultingIt func() (int, Lesser, error)

	resultingIt = func() (int, Lesser, error) {
		nextRecord, err := ri()
		if err != nil {
			if err == ErrIteratorStop {
				// Flush secondary buffers; TODO: improve performance
				for index, cluster := range clusters {
					rec := cluster.DeleteMinLesser()
					if rec != nil {
						return index + 1, rec, nil
					}
				}
			}
			return -1, nextRecord, err
		}

		// if not in order; find the first cluster to house it.
		for index, cluster := range clusters {
			if cluster.Len() < bufferSize { // Haven't started emitting records from it.
				cluster.ReplaceOrInsertLesser(nextRecord)
				return resultingIt()
			} else if lastRecordInCluster := cluster.Min().(*btreeLesser).Lesser; !nextRecord.Less(lastRecordInCluster) || index >= maxLeakybuckets { // Let's stop creating sub-buffers once we reach ~1% of original buffer size. Very magical number
				cluster.ReplaceOrInsertLesser(nextRecord)
				return index + 1, cluster.DeleteMinLesser(), nil
			}
		}

		newTree := NewBtreeSyncedLesser(2)
		newTree.ReplaceOrInsertLesser(nextRecord)

		clusters = append(clusters, newTree)
		return resultingIt()
	}

	return resultingIt
}

func newBufferedClusterIteartorWithPartitionBuffersV2(ri LesserIterator, bufferSize int) LesserIterator {
	partitions := map[string]*BtreeSyncedLesser{}
	inSortQueue := 0

	mu := sync.Mutex{}
	var lastErr error
	// Collect records into sorted partitions.
	// Once the buffer is full (counted over all partitions)
	// Alternate between phases
	// 1) Dump the largest partition (until empty, can be refilled during dumping); This might cause a starving pattern of other partitions if the partition is never emptied. TODO: add very small random phase shift
	// 2) Dump the oldest partition (until empty, can be refilled during dumping);

	findLargestPartition := func() string {
		largestPartition := ""
		for partition, tree := range partitions {
			if largestPartition == "" || tree.Len() > partitions[largestPartition].Len() {
				largestPartition = partition
			}
		}
		return largestPartition
	}

	findOldestPartition := func() string {
		oldestTs := time.Now() //at least one partition must be older than now, right?
		oldestPartition := ""
		for partition, tree := range partitions {
			if tree.lastUpdate.Before(oldestTs) {
				oldestPartition = partition
			}
		}
		return oldestPartition
	}

	loadRecord := func() {
		mu.Lock()
		defer mu.Unlock()

		if lastErr != nil {
			return
		}

		nextVal, err := ri()
		if err != nil {
			lastErr = err
			return
		}

		maybePartition := keyvaluelist.MaybePartitions(nextVal)
		if tree, ok := partitions[maybePartition]; !ok {
			tree = NewBtreeSyncedLesser(2)
			partitions[maybePartition] = tree
			//fmt.Printf("Inserting records into new partition %s\n", maybePartition)
			tree.ReplaceOrInsertLesser(nextVal)
			inSortQueue++
		} else {
			//fmt.Printf("Inserting records into old partition %s\n", maybePartition)
			tree.ReplaceOrInsertLesser(nextVal)
			inSortQueue++
		}
	}

	// Load cache assync
	cacheIsFull := make(chan struct{})
	go func() {
		for ; lastErr == nil; loadRecord() {
			if inSortQueue >= bufferSize {
				cacheIsFull <- struct{}{}
			}
		}
		//fmt.Printf("Closing cacheIsFull after err %v", lastErr)
		close(cacheIsFull)
	}()

	activePhase := 1
	getPartitionPicker := func() string {
		if activePhase == 1 {
			activePhase = 2
			return findLargestPartition()
		}
		activePhase = 1
		return findOldestPartition()
	}

	var activePartition string
	var partitionIterator func() (Lesser, error)
	partitionIterator = func() (Lesser, error) {
		for {
			mu.Lock()

			if activePartition == "" {
				activePartition = getPartitionPicker()
			}
			if activePartition == "" || inSortQueue <= 0 {
				mu.Unlock()
				return nil, ErrIteratorStop // Cache can not be full/closed and not get a partition back.
			}

			if tree, ok := partitions[activePartition]; !ok {
				panic("i shouldn't be here...")
			} else {
				rec := tree.DeleteMinLesser()
				if rec != nil {
					inSortQueue--
					mu.Unlock()
					return rec, nil
				}
				activePartition = ""
				delete(partitions, activePartition)
				mu.Unlock()
				continue
			}
		}
	}

	return func() (Lesser, error) {
		<-cacheIsFull
		return partitionIterator()
	}
}
