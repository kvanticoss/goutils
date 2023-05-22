package iterator

import (
	"github.com/kvanticoss/goutils/v2/keyvaluelist"
)

const maxLeakybuckets = 6

// NewBufferedClusterIterator works like NewBufferedRecordIteratorBTree but returns the cluster-id the records was returned from
// with the guarantee that within each cluster; records are always in sorted order. Good for handling out of order records.
// NOTE: to give this guarantee a cache of records must be keep in memory effectively creating an memory leak. The best way
// to avoid this memory leak to grow is to reset the iterator from time to time. Future iterators might include a GC based on inactivity
func NewBufferedClusterIterator(ri LesserIterator, bufferSize int) LesserIteratorClustered {
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
