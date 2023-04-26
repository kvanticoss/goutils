package iterator_test

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"

	"github.com/kvanticoss/goutils/iterator"
	"github.com/kvanticoss/goutils/iterator/test_utils"
	"github.com/kvanticoss/goutils/keyvaluelist"

	"github.com/stretchr/testify/assert"
)

type partitionStats map[string]map[int]int

func mapKeys(tmp map[string]bool) []string {
	keys := make([]string, 0)
	for k := range tmp {
		keys = append(keys, k)
	}
	return keys
}

func (stats partitionStats) countRecords() (int, int, map[int]int) {
	sum := 0
	max := 0
	clusterCount := map[int]int{}
	for _, v1 := range stats {
		for k, v := range v1 {
			sum = sum + v
			clusterCount[k] = clusterCount[k] + v
		}
	}

	return sum, max, clusterCount
}

func (stats partitionStats) print(writersCreated int, details bool) {
	sum, max, clusterCount := stats.countRecords()
	fmt.Printf("Sorted %d records into %d unique partitions(ids*day*hours) requiring %d files (%.2f files / partition; %.2f records / file)\n",
		sum,
		len(stats),
		writersCreated,
		float64(writersCreated)/float64(len(stats)),
		float64(sum)/float64(writersCreated),
	)

	if !details {
		return
	}

	// Sort mapkeys in order
	keys := make([]int, 0)
	for k := range clusterCount {
		if clusterCount[k] > max {
			max = clusterCount[k]
		}
		keys = append(keys, k)
	}
	sort.Ints(keys)

	for k := range keys {
		v := clusterCount[k]
		fmt.Printf("%d: %0"+fmt.Sprintf("%d", 100*v/sum)+"d (%.2f %%) [%d]\n", k, 0, 100*float64(v)/float64(sum), v)
	}
}

func TestClusteredBuffers(t *testing.T) {

	tests := []struct {
		ids        float64
		days       float64
		records    int
		sortBuffer int
		maxWriters int
	}{
		{
			ids:        1,
			days:       1,
			records:    10000,
			sortBuffer: 1000,
			maxWriters: 250,
		},
		{
			ids:        1,
			days:       1,
			records:    100000,
			sortBuffer: 20000,
			maxWriters: 250,
		},
		{
			ids:        1,
			days:       1,
			records:    500000,
			sortBuffer: 20000,
			maxWriters: 250,
		},
		{
			ids:        30,
			days:       2,
			records:    10000,
			sortBuffer: 1000,
			maxWriters: 250,
		},
		{
			ids:        30,
			days:       2,
			records:    100000,
			sortBuffer: 40000,
			maxWriters: 250,
		},
		{
			ids:        30,
			days:       2,
			records:    500000,
			sortBuffer: 40000,
			maxWriters: 250,
		},
	}

	for _, test := range tests {
		var cluster int
		var r interface{}
		var err error

		it := iterator.NewBufferedClusterIterator(test_utils.DummyIterator(test.ids, test.days, test.records), test.sortBuffer)

		stats := partitionStats{}
		writers := map[string]bool{}
		writersCreated := 0
		count := 0

		for cluster, r, err = it(); err == nil; cluster, r, err = it() {
			count++
			partition := keyvaluelist.MaybePartitions(r)
			if _, ok := stats[partition]; !ok {
				stats[partition] = map[int]int{}
			}

			stats[partition][cluster] = stats[partition][cluster] + 1

			if _, ok := writers[fmt.Sprintf("%s/%d", partition, cluster)]; !ok {
				writers[fmt.Sprintf("%s/%d", partition, cluster)] = true
				writersCreated++
			}

			// Simulate that writers are being closed randomly
			if len(writers) > test.maxWriters {
				keys := mapKeys(writers)
				delete(writers, keys[rand.Int31n(int32(len(writers))-1)])
			}
		}
		assert.Equal(t, count, test.records)
		assert.EqualError(t, iterator.ErrIteratorStop, err.Error())

		//stats.print(writersCreated, true)
	}
}
