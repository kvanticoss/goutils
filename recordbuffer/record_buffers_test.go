package recordbuffer_test

import (
	"bytes"
	"testing"

	"github.com/kvanticoss/goutils/iterator"
	"github.com/kvanticoss/goutils/iterator/test_utils"
	"github.com/kvanticoss/goutils/recordbuffer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSortedRecordBuffers(t *testing.T) {
	buffer := recordbuffer.NewSortedRecordBuffers(
		func() recordbuffer.ReadWriteResetter {
			return &bytes.Buffer{}
		},
		func() iterator.Lesser {
			return &test_utils.SortableStruct{}
		},
	)

	_, err := buffer.LoadFromLesserIterator(test_utils.GetRandomLesserIterator(20000, 200, nil))
	assert.NoError(t, err)
	_, err = buffer.LoadFromLesserIterator(test_utils.GetRandomLesserIterator(20000, 1000, nil))
	assert.NoError(t, err)
	_, err = buffer.LoadFromLesserIterator(test_utils.GetRandomLesserIterator(20000, 100, nil))
	assert.NoError(t, err)
	_, err = buffer.LoadFromLesserIterator(test_utils.GetRandomLesserIterator(20000, 50, nil))
	assert.NoError(t, err)
	_, err = buffer.LoadFromLesserIterator(test_utils.GetRandomLesserIterator(20000, 100, nil)) // Dupliacted rec
	assert.NoError(t, err)

	it, err := buffer.GetSortedIterator()
	require.NoError(t, err)

	count := 0
	var prevLesser, rec iterator.Lesser
	for rec, err = it(); err == nil; rec, err = it() {
		count++
		if prevLesser != nil && rec.Less(prevLesser) {
			t.Error("Expected records to be emitted in sorted order but got out of order reocrds")
		}
	}
	assert.EqualError(t, err, iterator.ErrIteratorStop.Error())

	assert.Equal(t, 200+1000+100+50+100, count, "expected all records to be returned")
}
