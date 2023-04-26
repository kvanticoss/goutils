package iterator_test

import (
	"testing"

	"github.com/kvanticoss/goutils/iterator"

	"github.com/stretchr/testify/assert"
)

type SortableStruct struct {
	Val int
}

func TestPipeRecordIteratorPerservesAllRecords(t *testing.T) {

	writer, it := iterator.NewRecordPipe[SortableStruct]()

	go func() {
		writer(&SortableStruct{1})
		writer(&SortableStruct{2})
		writer(&SortableStruct{3})
		writer(&SortableStruct{4})
		writer(&SortableStruct{5})
		writer(&SortableStruct{6})
		writer(&SortableStruct{7})
		writer(nil) // Writing nill closes the writer

		assert.Equal(t, iterator.ErrIteratorStop, writer(nil), "Expected error on closed writer")
	}()

	start := 1
	rec, err := it()
	for ; err == nil; rec, err = it() {
		assert.Equal(t, start, rec.Val)
		start = start + 1
	}
	assert.Equal(t, iterator.ErrIteratorStop, err, "Expected iterator stop after 7 times")
	assert.Equal(t, start-1, 7, "Expected iterator to run for 7 times")
}

func BenchmarkPipeRecordIteratorPerservesAllRecords(b *testing.B) {
	b.StopTimer()
	writer, it := iterator.NewRecordPipe[SortableStruct]()

	b.StartTimer()
	go func() {
		for n := 0; n < b.N; n++ {
			writer(&SortableStruct{n})
		}
		writer(nil)
	}()

	start := 0
	_, err := it()
	for ; err == nil; _, err = it() {
		start = start + 1
	}

}
