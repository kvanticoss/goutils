package eioutil

import (
	"io"
)

// ReadCloser is the comination of a Reader and closer.
type ReadCloser interface {
	io.Reader
	Close() error
}

// ReadCloserImpl ads a NOP Close method to any Reader.
type ReadCloserImpl struct {
	io.Reader
	closer func() error
}

// Close is a NOP
func (nc ReadCloserImpl) Close() error {
	if nc.closer != nil {
		return nc.closer()
	}
	return nil
}

// NewReadCloser returns a ReadCloser; will decorate the Reader with the closer
func NewReadCloser(r io.Reader, closer func() error) ReadCloser {
	return &ReadCloserImpl{
		Reader: r,
		closer: closer,
	}
}

// NewReadNOPCloser returns a ReadCloser that have a NOP closer method.
// Deprecated: use io.NopCloser() instead (available since Go 1.16)
func NewReadNOPCloser(r io.Reader) ReadCloser {
	return NewReadCloser(r, nil)
}
