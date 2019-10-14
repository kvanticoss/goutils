package iterator

import (
	"errors"
)

var (
	// ErrIteratorStop is returned by RecordIterators where there are not more records to be found.
	ErrIteratorStop = errors.New("iterator stop")

	// ErrNotLesser is returned if records yeilded from a recorditerator can't be changed to Lessers
	ErrNotLesser = errors.New("record does not implement the Lesser interface and can't be sorted")
)

// Lesser is implemented by type which can be compared to each other and should answer
// i'm I/this/self less than the other record (argument 1)
type Lesser interface {
	Less(other interface{}) bool
}

// LesserIterator iterators is the function interface
type LesserIterator func() (Lesser, error)

// LesserIteratorClustered iterators is the function interface
type LesserIteratorClustered func() (int, Lesser, error)

func (it RecordIterator) toLesserIterator() LesserIterator {
	return func() (Lesser, error) {
		record, err := it()
		if err != nil {
			return nil, err
		}
		if l, ok := record.(Lesser); ok {
			return l, nil
		}
		return nil, ErrNotLesser
	}
}
