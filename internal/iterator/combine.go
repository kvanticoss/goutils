package iterator


// CombineLesserIterators will yield the results from each of the consisting iterators
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
