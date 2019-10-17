package iterator

import (
	"github.com/google/btree"
)

// btreeLesser is a supporting struct to translate Less(other interface{}) to Less(other github.com/google/btree.Item)
type btreeLesser struct {
	Lesser
}

func (l btreeLesser) Less(other btree.Item) (res bool) {
	if other == nil {
		return false
	}
	if otherL, ok := other.(*btreeLesser); ok && otherL != nil {
		return l.Lesser.Less(otherL.Lesser)
	}

	if otherL, ok := other.(btreeLesser); ok {
		return l.Lesser.Less(otherL.Lesser)
	}

	//log.Fatalf("Other is not of type iterator.btreeLesser; it is %#v", other)
	return l.Lesser.Less(other)
}
