package iterator

import (
	"encoding/json"
	"io"
)

// JSONRecordIterator returns a RecordIterator based on a JSON stream of data, NewLine delimited
// @new - creator to allocate a new struct for each record; used to allow concurrent use of records yielded
// @r - byte stream reader containing new line delimited json data
func JSONRecordIterator[T any](new func() T, r io.Reader) RecordIterator[T] {
	dec := json.NewDecoder(r)
	return func() (T, error) {
		dst := new()
		if !dec.More() {
			if closer, ok := r.(io.Closer); ok {
				closer.Close()
			}
			var empty T
			return empty, ErrIteratorStop
		}
		return dst, dec.Decode(dst)
	}
}
