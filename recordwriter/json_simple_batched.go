package recordwriter

import (
	"compress/gzip"
	"fmt"
	"io"

	"github.com/kvanticoss/goutils/eioutil"
	"github.com/kvanticoss/goutils/iterator"
	"github.com/kvanticoss/goutils/writerfactory"

	jsoniter "github.com/json-iterator/go"
)

// NewLineJSON writes all the records from the records iterator as newline json to the writer.
// returns the first error from either the record iterator or the json encoding.
// if gz is true, the output will be gzipped and the size limit will be based on gz
// records will be put in batches of maxBytesParBatch bytes into the writer factory
// with a name of $basename_%06d$index.ndjson(.gz)
func NewLineJSONPartitionedBySize[T any](
	it iterator.RecordIterator[T],
	wf writerfactory.WriterFactory,
	maxBytesParBatch int,
	baseName string,
	gz bool,
) error {
	var record interface{}
	var err error

	w, err := getNewFileWriter(wf, baseName, maxBytesParBatch, gz)
	if err != nil {
		return err
	}

	for record, err = it(); err == nil; record, err = it() {
		d, err := jsoniter.ConfigCompatibleWithStandardLibrary.Marshal(record)
		if err != nil {
			return err
		}
		b := append(d, jsonRecordDelimiter...)
		if _, err := w.Write(b); err != nil {
			return err
		}
	}

	return w.Close()
}

func getNewFileWriter(wf writerfactory.WriterFactory, basename string, sizeLimit int, gz bool) (io.WriteCloser, error) {

	// Setup a new file writer and gzip writer
	partitionCount := -1
	filenameBase := basename + "_" + "%06d" + ".ndjson"
	if gz {
		filenameBase = filenameBase + ".gz"
	}

	getNewFileWriter := func() (io.WriteCloser, error) {
		partitionCount++
		file := fmt.Sprintf(filenameBase, partitionCount)
		wc, err := wf(file)
		if err != nil {
			return nil, fmt.Errorf("failed to open file (%s) for writing: %w", file, err)
		}
		return wc, nil
	}

	var writer io.WriteCloser
	var rawWriter io.WriteCloser
	var gzipWriter *gzip.Writer
	ptw := &passthroughWriter{
		w: writer,
	}

	// base case
	setupWriters := func() error {
		var err error
		rawWriter, err = getNewFileWriter()
		if err != nil {
			return err
		}
		if gz {
			gzipWriter = gzip.NewWriter(rawWriter)
			writer = gzipWriter
		} else {
			writer = rawWriter
		}
		ptw.w = writer
		return nil
	}
	if err := setupWriters(); err != nil {
		return nil, err
	}

	// utility for ordered closing of writers
	closer := func() error {
		if gzipWriter == nil {
			return writer.Close()
		}
		if err := gzipWriter.Close(); err != nil {
			return fmt.Errorf("gzip writer flush error: %w", err)
		}
		return rawWriter.Close()
	}

	// Setup the limited writer which resets to a new file after sizeLimit bytes
	// will be called after each call to Write() which is either a write to
	// gzipWriter or rawWriter
	bytesWritten := 0
	monitor := func(b []byte) error {
		bytesWritten = bytesWritten + len(b)
		if sizeLimit > 0 && bytesWritten > sizeLimit {
			if err := closer(); err != nil {
				return err
			}
			bytesWritten = 0
			return setupWriters()
		}
		return nil
	}

	return eioutil.NewWriteCloser(
		eioutil.NewPostWriteCallback(ptw, monitor),
		closer,
	), nil
}

// passthroughWriter allows us to update the writer implementation inflight
type passthroughWriter struct {
	w io.WriteCloser
}

func (ptw *passthroughWriter) Write(b []byte) (int, error) {
	return ptw.w.Write(b)
}
func (ptw *passthroughWriter) Close() error {
	return ptw.w.Close()
}
