package iterator

import (
	"context"
	"sync"

	"github.com/google/btree"
)

// NewBufferedRecordIteratorBTree creates a B+Tree of bufferSize from which records are emitted in sorted order.
// useful when sortable records are recieved out of order and should be emitted in (best effort) sorted order.
// Will block until the underlying LesserIterator has yeilded at least bufferSize items or returned an error,
// as such iterators can unblock with virtual errors such as ErrIteratorStall which will propagate up and can
// be discarded by the consumer.
func NewBufferedRecordIteratorBTree(ctx context.Context, ri LesserIterator, bufferSize int) LesserIterator {
	ctx, cancel := context.WithCancel(ctx)
	mu := sync.Mutex{}
	bufferFull := make(chan bool)
	tree := btree.New(2)
	var lastErr error

	go func() {
		for {
			val, err := ri()

			mu.Lock()
			if err == nil {
				tree.ReplaceOrInsert(btreeLesser{val})
				if bufferSize > 0 && tree.Len() >= bufferSize {
					mu.Unlock()        // inside if/else statement to avoid race detector in tests. btree.Len() / Reads are ok to call concurrently but this cause race:y results the go test suite complains about.
					bufferFull <- true // cap
				} else {
					mu.Unlock()
				}
			} else {
				lastErr = err
				mu.Unlock()
			}

			if lastErr == ErrIteratorStop {
				cancel()
				return
			}
			if ctx.Err() != nil {
				lastErr = ctx.Err()
				return
			}
		}
	}()

	// Sorted iterator
	return func() (Lesser, error) {
		if bufferSize > 0 {
			select {
			case <-bufferFull:
			case <-ctx.Done():
			}
		}

		mu.Lock()
		res := tree.DeleteMin()
		mu.Unlock()

		if res == nil {
			return nil, lastErr
		}
		return res.(btreeLesser).Lesser, nil
	}
}
