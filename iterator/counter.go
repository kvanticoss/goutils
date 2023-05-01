package iterator

// NewCountIterator returns another iterator which counts the number records
func NewCountIterator[T any](it RecordIterator[T]) (RecordIterator[T], func() int) {
	resCount := 0
	resIt := func() (T, error) {
		r, err := it()
		if err == nil {
			resCount++
		}
		return r, err
	}
	getCount := func() int {
		return resCount
	}
	return resIt, getCount
}
