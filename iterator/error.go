package iterator

import "errors"

var (
	// ErrIteratorStop is returned by RecordIterators where there are not more records to be found.
	ErrIteratorStop = errors.New("iterator stop")

	// ErrNotLesser is returned if records yielded from a recorditerator can't be changed to Lessers
	ErrNotLesser = errors.New("record does not implement the Lesser interface and can't be sorted")
)
