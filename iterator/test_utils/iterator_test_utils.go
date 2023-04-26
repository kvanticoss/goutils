package test_utils

import (
	"log"
	"math/rand"

	"github.com/kvanticoss/goutils/iterator"
	"github.com/kvanticoss/goutils/keyvaluelist"
)

// SortableStruct is a struct which implements the Lesser-interface
type SortableStruct struct {
	Val        int
	Partitions keyvaluelist.KeyValues
}

func (s *SortableStruct) GetPartitions() (keyvaluelist.KeyValues, error) {
	return s.Partitions, nil
}

// Less answers if "other" is Less (should be sorted before) this struct
func (s *SortableStruct) Less(other interface{}) bool {
	otherss, ok := other.(*SortableStruct)
	if !ok {
		log.Printf("Type assertion failed in SortableStruct; got other of type %#v", other)
		return true
	}
	res := s.Val < otherss.Val
	return res
}

// GetLesserIterator returns an iterators that runs up until max
func GetLesserIterator(max int, maybePartitions keyvaluelist.KeyValues) iterator.LesserIterator {
	i := 0
	return func() (iterator.Lesser, error) {
		i = i + 1
		if i <= max {
			return &SortableStruct{
				Val:        i,
				Partitions: maybePartitions,
			}, nil
		}
		return nil, iterator.ErrIteratorStop
	}
}

// GetReverseLesserIterator returns an iterators that runs from max to 0
func GetReverseLesserIterator(max int, maybePartitions keyvaluelist.KeyValues) iterator.LesserIterator {
	i := max + 1
	return func() (iterator.Lesser, error) {
		i = i - 1
		if i >= 0 {
			return &SortableStruct{
				Val:        i,
				Partitions: maybePartitions,
			}, nil
		}
		return nil, iterator.ErrIteratorStop
	}
}

// GetRandomLesserIterator returns an iterators which yields (stable) random-looking numbers in [0,n) at most maxElements times
func GetRandomLesserIterator(maxNum, maxElements int, maybePartitions keyvaluelist.KeyValues) iterator.LesserIterator {
	i := 0
	rand.Seed(int64(maxNum))
	return func() (iterator.Lesser, error) {
		i = i + 1
		if i <= maxElements {
			return &SortableStruct{
				Val:        rand.Intn(maxNum),
				Partitions: maybePartitions,
			}, nil
		}
		return nil, iterator.ErrIteratorStop
	}
}
