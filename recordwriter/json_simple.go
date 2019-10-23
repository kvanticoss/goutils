package recordwriter

import (
	"io"

	"github.com/kvanticoss/goutils/iterator"

	jsoniter "github.com/json-iterator/go"
)

// NewLineJSON writes all the records from the records iterator as newline json to the writer.
// returns the first error from either the record iterator or the json encoding.
func NewLineJSON(
	it iterator.RecordIterator,
	w io.Writer,
) error {
	var record interface{}
	var err error

	for record, err = it(); err == nil; record, err = it() {
		d, err := jsoniter.ConfigCompatibleWithStandardLibrary.Marshal(record)
		if err != nil {
			return err
		}
		if _, err := w.Write(append(d, jsonRecordDelimiter...)); err != nil {
			return err
		}
	}
	return err
}
