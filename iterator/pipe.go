package iterator

import (
	"sync"
)

// RecordWriter writes records. Writing nil will close the pipe
type RecordWriter func(interface{}) error

// LesserWriter writes lesser records. Writing nil will close the pipe
type LesserWriter func(Lesser) error

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

// NewLesserPipe returns a pipe from writer to Iterator. Band-aid solution for cases where
// providing an iterator is not feasible and a writer interface is required. Uses channels under the hood
func NewLesserPipe() (LesserWriter, LesserIterator) {
	recChan := make(chan Lesser)

	errClosed := func(record Lesser) error {
		return ErrIteratorStop
	}

	var f1 func(record Lesser) error
	f1 = func(record Lesser) (err error) {
		// Capture edge cases
		defer func() {
			if err := recover(); err != nil {
				err = ErrIteratorStop
			}
		}()

		if record == nil {
			f1 = errClosed
			close(recChan)
			return ErrIteratorStop
		}

		recChan <- record
		return nil
	}

	f2 := func() (Lesser, error) {
		record, ok := <-recChan
		if !ok {
			return nil, ErrIteratorStop
		}
		return record, nil
	}

	return func(record Lesser) error { return f1(record) }, f2
}

// NewLesserPipeFromChan turns a channel into an iterator; will yield ErrIteratorStop when the chan is closed
func NewLesserPipeFromChan(recChan chan Lesser) LesserIterator {
	return func() (Lesser, error) {
		record, ok := <-recChan
		if !ok || record == nil {
			return nil, ErrIteratorStop
		}
		return record, nil
	}
}

// NewLesserChanFromIterator returns a channels form an iterator; any iterator error will close the channel
func NewLesserChanFromIterator(it LesserIterator) chan Lesser {
	resChan := make(chan Lesser)
	go func() {
		for record, err := it(); err == nil; record, err = it() {
			if err != nil {
				close(resChan)
				return
			}
			resChan <- record
		}
	}()
	return resChan
}
