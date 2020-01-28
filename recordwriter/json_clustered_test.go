package recordwriter_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/kvanticoss/goutils/eioutil"
	"github.com/kvanticoss/goutils/keyvaluelist"
	"github.com/kvanticoss/goutils/recordwriter"
	"github.com/kvanticoss/goutils/writercache"

	"github.com/kvanticoss/goutils/iterator"
	"github.com/kvanticoss/goutils/iterator/test_utils"

	"github.com/stretchr/testify/assert"
)

func assertIsSorted(t *testing.T, b []byte) bool {
	rows := bytes.Split(b, []byte("\n"))
	if len(rows) <= 1 {
		return true
	}

	s1 := test_utils.SortableStruct{}
	s2 := test_utils.SortableStruct{}
	assert.NoError(t, json.Unmarshal(rows[0], &s1))

	for _, row := range rows[1:] {
		if len(row) == 0 { // remove last new line
			continue
		}

		if err := json.Unmarshal(row, &s2); err != nil {
			t.Logf("JsonError on :'%s'", string(row))
			assert.NoError(t, err)
		}

		if !s1.Less(&s2) && s2.Less(&s1) { // Second check to skip equality
			assert.Truef(t, s1.Less(&s2), "List not sorted; s1 (%v) > s2(%v)", s2, s1)
			return false
		}
		s1 = s2
	}
	return true
}

func valsToTestIt(partitions keyvaluelist.KeyValues, vals ...int) iterator.LesserIteratorClustered {
	return func() (int, iterator.Lesser, error) {
		if len(vals) == 0 {
			return 0, nil, iterator.ErrIteratorStop
		}
		res := vals[0]
		vals = vals[1:]
		//log.Printf("yeilding %d", res)
		return res, &test_utils.SortableStruct{
			Val:        res,
			Partitions: partitions,
		}, nil
	}
}

func TestMultiJSONSClusteredAndPartitioned(t *testing.T) {
	db := map[string]*bytes.Buffer{}
	wf := func(path string) (wc eioutil.WriteCloser, err error) {
		if _, ok := db[path]; !ok {
			db[path] = &bytes.Buffer{}
		}
		return eioutil.NewWriteNOPCloser(db[path]), nil
	}

	tests := []struct {
		name        string
		testRecords []int
	}{
		{
			name:        "Simple In order",
			testRecords: []int{1, 1, 1, 2, 2, 3, 4, 5, 5, 5, 5, 5, 5},
		},
		{
			name:        "Simple empty iterator",
			testRecords: []int{},
		},
		{
			name:        "Out of order",
			testRecords: []int{1, 2, 1, 4, 1, 3, 5, 5, 5, 5, 6, 6, 67, 77, 7, 7},
		},
		{
			name:        "Reverse order",
			testRecords: []int{9, 8, 7, 6, 5, 4, 3, 2, 1},
		},
	}

	for index, test := range tests {
		t.Run(t.Name(), func(t *testing.T) {
			partitions := keyvaluelist.KeyValues{{
				Key:   "testname",
				Value: fmt.Sprintf("%d", index),
			}}

			it := valsToTestIt(partitions, test.testRecords...)
			assert.EqualError(t, recordwriter.NewLineJSONClustered(it, wf, recordwriter.DefaultClusteredPathbuilder), "iterator stop")

			// Check total row in the test
			partitionsContent := []byte{}
			for key, val := range db {
				if strings.HasPrefix(key, partitions.ToPartitionKey()) {
					partitionsContent = append(partitionsContent, val.Bytes()...)
				}
			}
			assert.Len(t, bytes.Split(partitionsContent, []byte("\n")), len(test.testRecords)+1, "Expected all records to be accounted for in %v", string(partitionsContent))

			// Checkt that each record value exists
			for _, clusterID := range test.testRecords {
				filePath := recordwriter.DefaultClusteredPathbuilder(&test_utils.SortableStruct{
					Val:        clusterID,
					Partitions: partitions,
				}, clusterID)
				assert.Truef(t, strings.Contains(db[filePath].String(), fmt.Sprintf(`{"Val":%d`, clusterID)), "Couldn't find %s in %v", filePath, db)
			}
		})
	}
}

func BenchmarkJsonWriter(b *testing.B) {
	simulatedCostOfOpeningfile := time.Millisecond * 10

	ctx := context.Background()
	db := map[string]*bytes.Buffer{}
	writers := 0
	wf := func(path string) (wc eioutil.WriteCloser, err error) {
		db[path] = &bytes.Buffer{}
		writers++
		time.Sleep(simulatedCostOfOpeningfile)
		return eioutil.NewWriteNOPCloser(db[path]), nil
	}

	bufferSize := 2000
	maxConcurrentPartions := bufferSize / 5
	scale := 10
	c := writercache.NewCache(ctx, wf, time.Second*10, maxConcurrentPartions) // 1% of buffers will be allowed to be opened as files.

	for n := 0; n < b.N; n++ {
		b.StopTimer()
		it := iterator.NewBufferedClusterIteartor(
			test_utils.GetRandomLesserIterator(maxConcurrentPartions*20, bufferSize*scale, keyvaluelist.KeyValues{}),
			bufferSize, // Number to cache in ram before writing
		)
		pre := time.Now()
		b.StartTimer()
		assert.NoError(b, recordwriter.NewLineJSONClustered(it, c.GetWriter, recordwriter.DefaultClusteredPathbuilder))
		assert.NoError(b, c.Close())

		b.Logf(
			"Exported and sorted %d records with 1%% sorting capacity over %v (%f records / second) using %d distination buckets (allowing for max %d concurrent Partitions)",
			bufferSize*scale,
			time.Now().Sub(pre),
			float64(bufferSize*scale)/time.Now().Sub(pre).Seconds(),
			writers,
			maxConcurrentPartions,
		)
	}
}
