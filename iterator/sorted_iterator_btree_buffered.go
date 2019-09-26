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
		var val Lesser
		for {
			val, lastErr = ri()
			if lastErr == nil {

				mu.Lock()
				tree.ReplaceOrInsert(btreeLesser{val})
				mu.Unlock()

				if tree.Len() >= bufferSize {
					bufferFull <- true // cap
				}
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
		select {
		case <-bufferFull:
		case <-ctx.Done():
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
