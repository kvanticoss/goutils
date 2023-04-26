package iterator

import "errors"

var (
	// ErrIteratorStop is returned by RecordIterators where there are not more records to be found.
	ErrIteratorStop = errors.New("iterator stop")
)
