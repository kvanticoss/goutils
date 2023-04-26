package iterator

import (
	"sync"
)

// RecordWriter writes records. Writing nil will close the pipe
type RecordWriter func(interface{}) error

// NewRecordPipe returns a pipe from writer to Iterator. Band-aid solution for cases where
// providing an iterator is not feasible and a writer interface is required. Uses channels under the hood
func NewRecordPipe() (RecordWriter, RecordIterator) {
	recChan := make(chan interface{})
	done := false

	mu := sync.Mutex{}
	f1 := func(record interface{}) error {
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

		recChan <- record
		return nil
	}

	f2 := func() (interface{}, error) {
		record, ok := <-recChan
		if !ok {
			return nil, ErrIteratorStop
		}
		return record, nil
	}

	return f1, f2
}
