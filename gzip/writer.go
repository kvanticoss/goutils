package gzip

import (
	"compress/gzip"
	"io"
)

// Writer creates a gzip file which closes the underlying stream as well as the gzip stream on close
type Writer struct {
	*gzip.Writer
	underlyingWriter io.Writer
}

// NewWriter acts like a compress/gzip.NewWriter but that Close And Flushes will be cascaded to underlying writer. As such
// w is allowed to be a Closer & Flusher. If it implements those interfaces they will be called prior to a Close()-call
func NewWriter(w io.Writer) *Writer {
	return &Writer{
		Writer:           gzip.NewWriter(w),
		underlyingWriter: w,
	}
}

// Write writes data to the gzip stream
func (gz *Writer) Write(p []byte) (int, error) {
	return gz.Writer.Write(p)
}

// Flush flushes and flushes the gzip writer AND the underlying writer
func (gz *Writer) Flush() error {
	if err := gz.Writer.Flush(); err != nil {
		return err
	}

	if flusher, ok := gz.underlyingWriter.(flusher); ok {
		if err := flusher.Flush(); err != nil {
			return err
		}
	}
	return nil
}

// Close flushes and closes the gzip writer AND the underlying writer
func (gz *Writer) Close() error {
	if err := gz.Flush(); err != nil {
		return err
	}

	if err := gz.Writer.Close(); err != nil {
		return err
	}

	if closer, ok := gz.underlyingWriter.(closer); ok {
		if err := closer.Close(); err != nil {
			return err
		}
	}

	return nil
}

type flusher interface {
	Flush() error
}

type closer interface {
	Close() error
}
