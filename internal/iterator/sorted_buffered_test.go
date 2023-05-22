package iterator_test

import (
	"testing"

	"github.com/kvanticoss/goutils/v2/internal/iterator"
	"github.com/kvanticoss/goutils/v2/internal/iterator/test_utils"
	"github.com/kvanticoss/goutils/v2/keyvaluelist"
)

func TestSortedRecordBufferIterators(t *testing.T) {

	expectedCount := 100

	it := iterator.NewBufferedRecordIteratorBTree(test_utils.GetRandomLesserIterator(9999, expectedCount, keyvaluelist.KeyValues{}), 100)

	count := 0
	lastVal := 0
	var r interface{}
	var err error
	for r, err = it(); err == nil; r, err = it() {
		record, ok := r.(*test_utils.SortableStruct)
		if !ok {
			t.Errorf("Failure in type assertion to SortableStruct; got %#v", r)
			break
		}
		if record.Val < lastVal {
			t.Errorf("Expected each record value to be higher than the last; got %v", record.Val)
		}
		count++
		lastVal = record.Val
	}

	if err != iterator.ErrIteratorStop {
		t.Errorf("Expected only error to be IteratorStop; Got %v", err)
	}

	if count != expectedCount {
		t.Errorf("Sorted Iterator skipped some records; all should be processed. Only processed %d / %d records", count, expectedCount)
	}

	if lastVal == 0 {
		t.Error("Record emitter didn't yield any records")
	}
}
