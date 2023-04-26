package iterator

// CombineIterators will yield the results from each of the consisting iterators
// the error ErrIteratorStop is expected to progress to the next iterator
func CombineIterators[T any](iterators ...RecordIterator[T]) RecordIterator[T] {
	var f func() (T, error)
	f = func() (T, error) {
		if len(iterators) == 0 {
			var empty T
			return empty, ErrIteratorStop
		}
		rec, err := iterators[0]()
		if err == ErrIteratorStop {
			iterators = iterators[1:]
			return f()
		}
		return rec, err
	}
	return f
}
