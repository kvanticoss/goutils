package iterator

import "log"

// LogRecordIterator prints the contents of the record prior to returning it
// using the pattern as the fmt-directive where the first argument is the records
// second is the error as such. log.Printf(pattern, r, err)
func LogRecordIterator(it RecordIterator, pattern string) RecordIterator {
	return func() (interface{}, error) {
		r, err := it()
		log.Printf(pattern, r, err)
		return r, err
	}
}
