package eioutil

import (
	"io"
)

// WriteCloser is the comination of a writer and closer.
type WriteCloser interface {
	io.Writer
	Close() error
}

// writeCloserImpl ads a NOP Close method to any writer.
type writeCloserImpl struct {
	io.Writer
	closer func() error
}

// Close is a NOP
func (nc writeCloserImpl) Close() error {
	if nc.closer != nil {
		return nc.closer()
	}
	return nil
}

// NewWriteCloser returns a WriteCloser; will decorate the writer with the closer
func NewWriteCloser(w io.Writer, closer func() error) WriteCloser {
	return &writeCloserImpl{
		Writer: w,
		closer: closer,
	}
}

// NewWriteNOPCloser returns a WriteCloser that have a NOP closer method.
func NewWriteNOPCloser(w io.Writer) WriteCloser {
	return NewWriteCloser(w, nil)
}
