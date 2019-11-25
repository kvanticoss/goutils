package recordbuffer_test

import (
	"bytes"
	"testing"

	"github.com/kvanticoss/goutils/iterator"
	"github.com/kvanticoss/goutils/iterator/test_utils"
	"github.com/kvanticoss/goutils/recordbuffer"
	"github.com/stretchr/testify/assert"
)

func TestSortedRecordBuffers(t *testing.T) {
	buffer := recordbuffer.NewSortedRecordBuffers(
		func() recordbuffer.ReadWriteResetter {
			return &bytes.Buffer{}
		},
		func() iterator.Lesser {
			return test_utils.NewDummyRecordPtr()
		},
	)

	assert.NoError(t, buffer.LoadFromLesserIterator(test_utils.DummyIterator(1, 2, 200)))
	assert.NoError(t, buffer.LoadFromLesserIterator(test_utils.DummyIterator(1, 2, 1000)))
	assert.NoError(t, buffer.LoadFromLesserIterator(test_utils.DummyIterator(1, 2, 100)))
	assert.NoError(t, buffer.LoadFromLesserIterator(test_utils.DummyIterator(1, 2, 50)))
	assert.NoError(t, buffer.LoadFromLesserIterator(test_utils.DummyIterator(1, 2, 100))) // Dupliacted records

	it, err := buffer.GetSortedIterator()
	assert.NoError(t, err)

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
