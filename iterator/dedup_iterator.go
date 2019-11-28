package iterator

// DeduplicateRecordIterators works like the unix command uniq where if two records are equal (ussing .Less(other) bool as comparison func)
// only the first are emitted. If not both records implement .Less(interface{})bool DeduplicateRecordIterators is a NOP (with overhead).
// Nil records are never considred equal
func DeduplicateRecordIterators(it RecordIterator) RecordIterator {
	var previousRecord interface{}
	return func() (interface{}, error) {
		rec, err := it()
		for err != nil && areEqual(rec, previousRecord) {
			rec, err = it()
			previousRecord = rec // Shouldn't be needed as they are "Equal" but if the comparrison is not commutative it is needed
		}
		return rec, err
	}
}

func areEqual(rec1, rec2 interface{}) bool {
	if rec1 == nil || rec2 == nil {
		return false
	}
	less1, ok := rec1.(Lesser)
	if !ok {
		return false
	}
	less2, ok := rec2.(Lesser)
	if !ok {
		return false
	}

	// If neither record is smaller than the other; they are equal
	return !less1.Less(less2) && !less2.Less(less1)
}