package iterator

import (
	"context"
)

// NewBufferedClusterIteartor works like NewBufferedRecordIteratorBTree but returns the cluster-id the records was returned from
// with the guarrantee that within each cluster; records are always in sorted order. Good for handling out of order records.
func NewBufferedClusterIteartor(ctx context.Context, ri LesserIterator, bufferSize int) LesserIteratorClustered {
	clusters := []Lesser{}
	it := NewBufferedRecordIteratorBTree(ctx, ri, bufferSize)
	return func() (int, Lesser, error) {
		nextVal, err := it()
		if err != nil {
			return -1, nextVal, err
		}

		for index, lastRecordInCluster := range clusters {
			if !nextVal.Less(lastRecordInCluster) {
				clusters[index] = nextVal
				return index, nextVal, nil
			}
		}

		clusters = append(clusters, nextVal)
		return len(clusters) - 1, nextVal, nil
	}
}
