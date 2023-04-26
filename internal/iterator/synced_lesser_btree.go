package iterator

import (
	"sync"
	"time"

	"github.com/google/btree"
)

// BtreeSyncedLesser is an mutex protected version of github.com/google/btree
// with Lesser typed methods.
type BtreeSyncedLesser struct {
	*btree.BTree
	mu         sync.Mutex
	lastUpdate time.Time
}

func NewBtreeSyncedLesser(degree int) *BtreeSyncedLesser {
	return &BtreeSyncedLesser{
		BTree: btree.New(degree),
	}
}

func (tree *BtreeSyncedLesser) ReplaceOrInsertLesser(item Lesser) Lesser {
	tree.mu.Lock()
	defer tree.mu.Unlock()

	tree.lastUpdate = time.Now()

	if res := tree.ReplaceOrInsert(&btreeLesser{item}); res != nil {
		return res.(*btreeLesser).Lesser
	}
	return nil
}
func (tree *BtreeSyncedLesser) ClearLesser(addNodesToFreelist bool) {
	tree.mu.Lock()
	defer tree.mu.Unlock()

	tree.lastUpdate = time.Now()

	tree.BTree.Clear(addNodesToFreelist)
}
func (tree *BtreeSyncedLesser) CloneLesser() *BtreeSyncedLesser {
	tree.mu.Lock()
	defer tree.mu.Unlock()
	if res := tree.BTree.Clone(); res != nil {
		return &BtreeSyncedLesser{BTree: res, mu: sync.Mutex{}, lastUpdate: tree.lastUpdate}
	}
	return nil
}
func (tree *BtreeSyncedLesser) DeleteLesser(item Lesser) Lesser {
	tree.mu.Lock()
	defer tree.mu.Unlock()

	tree.lastUpdate = time.Now()

	if res := tree.BTree.Delete(&btreeLesser{item}); res != nil {
		return res.(*btreeLesser).Lesser
	}
	return nil
}
func (tree *BtreeSyncedLesser) DeleteMaxLesser() Lesser {
	tree.mu.Lock()
	defer tree.mu.Unlock()

	tree.lastUpdate = time.Now()

	if res := tree.BTree.DeleteMax(); res != nil {
		return res.(*btreeLesser).Lesser
	}
	return nil
}
func (tree *BtreeSyncedLesser) DeleteMinLesser() Lesser {
	tree.mu.Lock()
	defer tree.mu.Unlock()

	tree.lastUpdate = time.Now()

	if res := tree.BTree.DeleteMin(); res != nil {
		return res.(*btreeLesser).Lesser
	}
	return nil
}
func (tree *BtreeSyncedLesser) GetLesser(key Lesser) Lesser {
	tree.mu.Lock()
	defer tree.mu.Unlock()
	if res := tree.BTree.Get(&btreeLesser{key}); res != nil {
		return res.(*btreeLesser).Lesser
	}
	return nil
}
func (tree *BtreeSyncedLesser) HasLesser(key Lesser) bool {
	tree.mu.Lock()
	defer tree.mu.Unlock()
	return tree.BTree.Has(&btreeLesser{key})
}
func (tree *BtreeSyncedLesser) LenLesser() int {
	tree.mu.Lock()
	defer tree.mu.Unlock()
	return tree.BTree.Len()
}
func (tree *BtreeSyncedLesser) MaxLesser() Lesser {
	tree.mu.Lock()
	defer tree.mu.Unlock()
	if res := tree.BTree.Max(); res != nil {
		return res.(*btreeLesser).Lesser
	}
	return nil
}
func (tree *BtreeSyncedLesser) MinLesser() Lesser {
	tree.mu.Lock()
	defer tree.mu.Unlock()
	if res := tree.BTree.Min(); res != nil {
		return res.(*btreeLesser).Lesser
	}
	return nil
}
