package iterator

import (
	"sync"
)

// RecordWriter writes records. Writing nil will close the pipe
type RecordWriter[T any] func(T) error

// NewRecordPipe returns a pipe from writer to Iterator. Band-aid solution for cases where
// providing an iterator is not feasible and a writer interface is required. Uses channels under the hood
func NewRecordPipe[T any]() (RecordWriter[*T], RecordIterator[T]) {
	recChan := make(chan T)
	done := false

	mu := sync.Mutex{}

	var empty T
	writer := func(record *T) error {
		mu.Lock()
		defer mu.Unlock()
		if done {
			return ErrIteratorStop
		}
		if record == nil {
			close(recChan)
			done = true
			return ErrIteratorStop
		}

		recChan <- *record
		return nil
	}

	reader := func() (T, error) {
		record, ok := <-recChan
		if !ok {
			return empty, ErrIteratorStop
		}
		return record, nil
	}

	return writer, reader
}
