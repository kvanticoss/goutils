package iterator

import "log"

// LogRecordIterator prints the contents of the record prior to returning it
// using the pattern as the fmt-directive where the first argument is the records
// second is the error as such. log.Printf(pattern, r, err)
func LogRecordIterator[T any](it RecordIterator[T], pattern string) RecordIterator[T] {
	return func() (T, error) {
		r, err := it()
		log.Printf(pattern, r, err)
		return r, err
	}
}
