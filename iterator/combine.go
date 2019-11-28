package iterator

// CombineIterators will yeild the results from each of the consisting iterators
// the error ErrIteratorStop is expected to progress to the next iterator
func CombineIterators(iterators ...RecordIterator) RecordIterator {
	var f func() (interface{}, error)
	f = func() (interface{}, error) {
		if len(iterators) == 0 {
			return nil, ErrIteratorStop
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

// CombineLesserIterators will yeild the results from each of the consisting iterators
// the error ErrIteratorStop is expected to progress to the next iterator. To combine LesserIterators in
// sorted fashion use SortedLesserIterators()
func CombineLesserIterators(iterators ...LesserIterator) LesserIterator {
	var f func() (Lesser, error)
	f = func() (Lesser, error) {
		if len(iterators) == 0 {
			return nil, ErrIteratorStop
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
