package recordwriter

import (
	"fmt"
	"io"

	"github.com/kvanticoss/goutils/eioutil"
	"github.com/kvanticoss/goutils/iterator"
	"github.com/kvanticoss/goutils/writerfactory"

	jsoniter "github.com/json-iterator/go"
)

// NewLineJSON writes all the records from the records iterator as newline json to the writer.
// returns the first error from either the record iterator or the json encoding.
func NewLineJSONBatched[T any](
	it iterator.RecordIterator[T],
	wf writerfactory.WriterFactory,
	maxBytesParBatch int,
	base_name string,
) error {
	var record interface{}
	var err error

	partitionCount := -1
	filenameBase := base_name + "_" + "%06d" + ".ndjson"

	getNewFileWriter := func() (io.WriteCloser, error) {
		partitionCount++
		file := fmt.Sprintf(filenameBase, partitionCount)
		wc, err := wf(file)
		if err != nil {
			return nil, fmt.Errorf("failed to open file (%s) for writing: %w", file, err)
		}
		if maxBytesParBatch > 0 {
			wc = eioutil.NewWriterCloserWithSelfDestructAfterMaxBytes(maxBytesParBatch, wc)
		}
		return wc, nil
	}

	w, err := getNewFileWriter()
	if err != nil {
		return err
	}

	for record, err = it(); err == nil; record, err = it() {
		d, err := jsoniter.ConfigCompatibleWithStandardLibrary.Marshal(record)
		if err != nil {
			return err
		}
		b := append(d, jsonRecordDelimiter...)

		if _, err := w.Write(b); err != nil && err != eioutil.ErrAlreadyClosed {
			return err
		} else if err == eioutil.ErrAlreadyClosed {
			w, err = getNewFileWriter()
			if err != nil {
				return err
			}
			// retry write with new writer; should never fail under normal circumstances
			if _, err := w.Write(b); err != nil {
				return err
			}
		}
	}
	return err
}
