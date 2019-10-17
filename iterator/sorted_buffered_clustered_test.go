package iterator_test

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"

	"github.com/kvanticoss/goutils/iterator"
	"github.com/kvanticoss/goutils/iterator/test_utils"
	"github.com/kvanticoss/goutils/keyvaluelist"
)

func mapKeys(tmp map[string]bool) []string {
	keys := make([]string, 0)
	for k := range tmp {
		keys = append(keys, k)
	}
	return keys
}

func TestClusterdBuffers(t *testing.T) {

	stats := map[string]map[int]int{}

	writers := map[string]bool{}
	writersCreated := 0

	printStats := func() {
		tmp := map[int]int{}
		max := 0
		sum := 0
		for _, v1 := range stats {
			for k, v := range v1 {
				sum = sum + v
				tmp[k] = tmp[k] + v
			}
		}

		keys := make([]int, 0)
		for k := range tmp {
			if tmp[k] > max {
				max = tmp[k]
			}
			keys = append(keys, k)
		}
		sort.Ints(keys)

		fmt.Printf("Found %d records and %d partitions requireing %d writers\n", sum, len(stats), writersCreated)
		for k := range keys {
			v := tmp[k]
			fmt.Printf("%d: %0"+fmt.Sprintf("%d", 100*v/sum)+"d (%.2f %%) [%d]\n", k, 0, 100*float64(v)/float64(sum), v)
		}
	}

	maxWriters := 250
	it := iterator.NewBufferedClusterIteartor(test_utils.DummyIterator(20, 2, 500000), 125000)

	var cluster int
	var r interface{}
	var err error
	count := 0
	for cluster, r, err = it(); err == nil; cluster, r, err = it() {
		partition := keyvaluelist.MaybePartitions(r)
		count++
		if _, ok := stats[partition]; ok {
			stats[partition][cluster] = stats[partition][cluster] + 1
		} else {
			stats[partition] = map[int]int{}
			stats[partition][cluster] = 1
		}
		if _, ok := writers[fmt.Sprintf("%s/%d", partition, cluster)]; !ok {
			writers[fmt.Sprintf("%s/%d", partition, cluster)] = true
			writersCreated++
		}

		if len(writers) > maxWriters {
			keys := mapKeys(writers)
			delete(writers, keys[rand.Int31n(int32(len(writers))-1)])
		}
	}
	t.Logf("Error was %v", err)

	printStats()

}
