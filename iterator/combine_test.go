package iterator_test

import (
	"testing"

	"github.com/kvanticoss/goutils/v2/iterator"
	"github.com/kvanticoss/goutils/v2/iterator/test_utils"

	"github.com/stretchr/testify/assert"
)

func TestCombineIteartors(t *testing.T) {
	numberOfRecords := 10
	it1 := test_utils.DummyIterator(1, 1, numberOfRecords)
	it2 := test_utils.DummyIterator(1, 1, numberOfRecords)

	combined := iterator.CombineIterators(it1, it2)

	count := 0
	for _, err := combined(); err == nil; _, err = combined() {
		count++
	}
	assert.Equal(t, 2*numberOfRecords, count)

}
