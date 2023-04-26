package iterator

import "sync"

// NewBufferedRecordIteratorBTree creates a B+Tree of bufferSize from which records are emitted in sorted order.
// useful when sortable records are recieved out of order and should be emitted in (best effort) sorted order.
// Will block until the underlying LesserIterator has yielded at least bufferSize items or returned an error,
// as such iterators can unblock with virtual errors such as ErrIteratorStall which will propagate up and can
// be discarded by the consumer.
func NewBufferedRecordIteratorBTree(ri LesserIterator, bufferSize int) LesserIterator {
	bufferFull := make(chan bool)
	tree := NewBtreeSyncedLesser(2)
	var lastErr error
	var mu sync.Mutex

	go func() {
		for {
			val, err := ri()

			mu.Lock()
			if err != nil {
				if lastErr == nil {
					close(bufferFull)
				}
				lastErr = err
			}
			mu.Unlock()

			if val == nil {
				return
			}

			//if existing := tree.GetLesser(val); existing != nil {
			//	log.Printf("Warning, Dropping duplicate \n%#v\n%#v", existing.(*btreeLesser).Lesser, val)
			//}
			tree.ReplaceOrInsertLesser(val)
			if bufferSize > 0 && tree.Len() >= bufferSize {
				bufferFull <- true // cap
			}
		}
	}()

	// Sorted iterator
	return func() (Lesser, error) {
		if bufferSize > 0 {
			select {
			case <-bufferFull:
			}
		}
		res := tree.DeleteMinLesser()
		if res == nil {
			mu.Lock()
			defer mu.Unlock()
			return nil, lastErr
		}
		return res, nil
	}
}
