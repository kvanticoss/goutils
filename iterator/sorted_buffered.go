package iterator

import (
	"sync"

	"github.com/google/btree"
)

// NewBufferedRecordIteratorBTree creates a B+Tree of bufferSize from which records are emitted in sorted order.
// useful when sortable records are recieved out of order and should be emitted in (best effort) sorted order.
// Will block until the underlying LesserIterator has yeilded at least bufferSize items or returned an error,
// as such iterators can unblock with virtual errors such as ErrIteratorStall which will propagate up and can
// be discarded by the consumer.
func NewBufferedRecordIteratorBTree(ri LesserIterator, bufferSize int) LesserIterator {
	mu := sync.Mutex{}
	bufferFull := make(chan bool)
	tree := btree.New(2)
	var lastErr error

	go func() {
		for {
			val, err := ri()

			mu.Lock()
			if err == ErrIteratorStop {
				lastErr = err
				close(bufferFull)
				mu.Unlock()
				return
			} else if err == nil {
				if existing := tree.Get(&btreeLesser{val}); existing != nil {
					//log.Printf("Warning, Dropping duplicate \n%#v\n%#v", existing.(*btreeLesser).Lesser, val)
				}
				tree.ReplaceOrInsert(&btreeLesser{val})

				if bufferSize > 0 && tree.Len() >= bufferSize {
					mu.Unlock()
					bufferFull <- true // cap
				} else {
					mu.Unlock()
				}
			} else {
				mu.Unlock()
				lastErr = err
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
		mu.Lock()
		res := tree.DeleteMin()
		mu.Unlock()

		if res == nil {
			return nil, lastErr
		}
		return res.(*btreeLesser).Lesser, nil
	}
}

type btreeSynced struct {
	*btree.BTree

	mu         sync.Mutex
	bufferFull chan bool
	lastErr    error
	bufferSize int
}

func NewBTreeSorter(bufferSize int) *btreeSynced {
	return &btreeSynced{
		BTree:      btree.New(2),
		bufferFull: make(chan bool),
		bufferSize: bufferSize,
	}
}

func (tree *btreeSynced) Close() {
	tree.mu.Lock()
	defer tree.mu.Unlock()
	close(tree.bufferFull)
	tree.bufferFull = nil
}

func (tree *btreeSynced) ReplaceOrInsertLesser(val Lesser, blockOnFull bool) {
	if existing := tree.Get(&btreeLesser{val}); existing != nil {
		//log.Printf("Warning, Dropping duplicate \n%#v\n%#v", existing.(*btreeLesser).Lesser, val)
	}

	tree.mu.Lock()
	tree.ReplaceOrInsert(&btreeLesser{val})
	if !blockOnFull {
		tree.mu.Unlock()
		return
	}

	if blockOnFull && tree.bufferFull != nil && tree.bufferSize > 0 && tree.Len() >= tree.bufferSize {
		tree.mu.Unlock()
		tree.bufferFull <- true // cap
	} else {
		tree.mu.Unlock()
	}
}

func (tree *btreeSynced) DeleteMinLesser(waitForBuffer bool) Lesser {
	if waitForBuffer && tree.bufferSize > 0 {
		select {
		case <-tree.bufferFull:
		}
	} else {
		select {
		case <-tree.bufferFull:
		default: // Extra direct dropout in we don't want to wait
		}
	}

	tree.mu.Lock()
	res := tree.DeleteMin()
	tree.mu.Unlock()

	if res == nil {
		return nil
	}
	return res.(*btreeLesser).Lesser
}

func (tree *btreeSynced) IsFull() bool {
	tree.mu.Lock()
	defer tree.mu.Unlock()
	return tree.Len() >= tree.bufferSize
}

func (tree *btreeSynced) DeleteMinLesserIfFullBuffer() Lesser {
	select {
	case <-tree.bufferFull:
	default:
		return nil
	}

	tree.mu.Lock()
	res := tree.DeleteMin()
	tree.mu.Unlock()

	if res == nil {
		return nil
	}
	return res.(*btreeLesser).Lesser
}
