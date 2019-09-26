package iterator

import (
	"github.com/google/btree"
)

// btreeLesser is a supporting struct to translate Less(other interface{}) to Less(other github.com/google/btree.Item)
type btreeLesser struct {
	Lesser
}

func (l btreeLesser) Less(other btree.Item) bool {
	if other == nil {
		return false
	}
	if btl, ok := other.(btreeLesser); ok {
		return l.Lesser.Less(btl.Lesser)
	}
	if btl, ok := other.(*btreeLesser); ok && btl != nil {
		return l.Lesser.Less(btl.Lesser)
	}
	return l.Lesser.Less(other)
}
