package test_utils

import (
	"log"

	"github.com/kvanticoss/goutils/v2/keyvaluelist"
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
