package iterator

// SortedRecordIterators combines a list of iterators; always yeilding the lowest value
// available from all iterators. To do this it keeps a local "peak cache" of the next
// value for each iterator. This means that iterators that produces data from volatile
// sources (e.g time) might be experience unexpected results.
func SortedRecordIterators(iterators []RecordIterator) (RecordIterator, error) {
	var err error
	LesserIterators := make([]LesserIterator, len(iterators))
	nextCandidates := make([]Lesser, len(iterators))
	for i, ri := range iterators {
		LesserIterators[i] = ri.toLesserIterator()
		nextCandidates[i], err = LesserIterators[i]()
		if err != nil && err != ErrIteratorStop { // Stops are not errors
			return nil, err
		}
	}

	return func() (interface{}, error) {
		bestIndex := -1
		var bestCandidate Lesser

		for i, candidate := range nextCandidates {
			if candidate == nil {
				continue
			}
			if bestIndex == -1 {
				bestIndex = i
				bestCandidate = candidate
				continue
			}

			if !bestCandidate.Less(candidate) {
				bestIndex = i
				bestCandidate = candidate
			}
		}

		if bestIndex == -1 {
			return nil, ErrIteratorStop
		}

		nextRecord, err := LesserIterators[bestIndex]()
		if err == ErrIteratorStop {
			nextCandidates[bestIndex] = nil
		} else if err != nil {
			nextCandidates[bestIndex] = nil
			return nil, err
		} else if l, ok := nextRecord.(Lesser); !ok {
			return nil, ErrNotLesser
		} else {
			nextCandidates[bestIndex] = l
		}

		return bestCandidate, nil
	}, nil
}

func SortedLesserIterators(iterators []LesserIterator) (LesserIterator, error) {
	var err error
	LesserIterators := make([]LesserIterator, len(iterators))
	nextCandidates := make([]Lesser, len(iterators))
	for i, ri := range iterators {
		nextCandidates[i], err = ri()
		if err != nil && err != ErrIteratorStop { // Stops are not errors
			return nil, err
		}
	}

	return func() (Lesser, error) {
		bestIndex := -1
		var bestCandidate Lesser

		for i, candidate := range nextCandidates {
			if candidate == nil {
				continue
			}
			if bestIndex == -1 {
				bestIndex = i
				bestCandidate = candidate
				continue
			}

			if !bestCandidate.Less(candidate) {
				bestIndex = i
				bestCandidate = candidate
			}
		}

		if bestIndex == -1 {
			return nil, ErrIteratorStop
		}

		nextRecord, err := LesserIterators[bestIndex]()
		if err == ErrIteratorStop {
			nextCandidates[bestIndex] = nil
		} else if err != nil {
			nextCandidates[bestIndex] = nil
			return nil, err
		} else if l, ok := nextRecord.(Lesser); !ok {
			return nil, ErrNotLesser
		} else {
			nextCandidates[bestIndex] = l
		}

		return bestCandidate, nil
	}, nil
}
