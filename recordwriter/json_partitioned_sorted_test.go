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

func TestMultiJSONStreamingWithNoTimeout(t *testing.T) {
	db := map[string]*bytes.Buffer{}
	wf := func(path string) (wc eioutil.WriteCloser, err error) {
		db[path] = &bytes.Buffer{}
		return eioutil.NewWriteNOPCloser(db[path]), nil
	}

	tests := []struct {
		name               string
		values, buffersize int
		expectations       [][]byte
	}{
		{
			name:       "Ensure sorted with large buffer",
			values:     101,
			buffersize: 1000,
		}, {
			name:       "Ensure sorted with large same size buffer",
			values:     101,
			buffersize: 102,
		},
		{
			name:       "Ensure sorted with too small buffer",
			values:     101,
			buffersize: 10,
		}, {
			name:       "Test with bigger iterator",
			values:     1000,
			buffersize: 10,
		},
	}

	t.Parallel()
	for index, test := range tests {
		t.Run(t.Name(), func(t *testing.T) {

			partitions := keyvaluelist.KeyValues{{
				Key:   "testname",
				Value: fmt.Sprintf("%d", index),
			}}

			it := iterator.NewBufferedClusterIteartor(
				test_utils.GetRandomLesserIterator(
					99999,
					test.values, //number of items
					partitions,
				),
				test.buffersize, // Number to cache in ram before writing
			)

			assert.EqualError(t, recordwriter.NewLineJSONPartitionedClustered(it, wf, recordwriter.DefaultPathbuilder), "iterator stop")
			partitionsContent := []byte{}
			for key, val := range db {
				//t.Log("Checking partition:" + key)
				if !assertIsSorted(t, val.Bytes()) {
					//t.Logf("Db-state: %v", db)
				}
				if strings.HasPrefix(key, partitions.ToPartitionKey()) {
					partitionsContent = append(partitionsContent, val.Bytes()...)
				}
			}
			/*
				for _, expected := range test.expectations {
					assert.Truef(t, bytes.Contains(partitionsContent, expected), "Couldn't find %s in %s", expected, partitionsContent)
				}
			*/
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
		recordwriter.NewLineJSONPartitionedClustered(it, c.GetWriter, recordwriter.DefaultPathbuilder)

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
