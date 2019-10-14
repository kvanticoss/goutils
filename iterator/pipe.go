package iterator

// RecordWriter writes records. Writing nil will close the pipe
type RecordWriter func(interface{}) error

// LesserWriter writes lesser records. Writing nil will close the pipe
type LesserWriter func(Lesser) error

// NewRecordPipe returns a pipe from writer to Iterator. Bandaid solution for cases where
// providing an iterator is not feasable and a writer iterface is required. Uses channels under the hood
func NewRecordPipe() (RecordWriter, RecordIterator) {
	recChan := make(chan interface{})
	done := false

	f1 := func(record interface{}) error {
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

// NewLesserPipe returns a pipe from writer to Iterator. Bandaid solution for cases where
// providing an iterator is not feasable and a writer iterface is required. Uses channels under the hood
func NewLesserPipe() (LesserWriter, LesserIterator) {
	recChan := make(chan Lesser)
	done := false

	f1 := func(record Lesser) error {
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

	f2 := func() (Lesser, error) {
		record, ok := <-recChan
		if !ok {
			return nil, ErrIteratorStop
		}
		return record, nil
	}

	return f1, f2
}
