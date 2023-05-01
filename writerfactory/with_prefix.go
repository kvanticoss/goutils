package writerfactory

import (
	"io"
	"path"
)

// WrapWFWithPrefix adds a prefix to all writes conducted by the writerfactory
func WrapWFWithPrefix(wf WriterFactory, prefix string) WriterFactory {
	return func(writePath string) (io.WriteCloser, error) {
		return wf(path.Join(prefix, writePath))
	}
}

// WithPrefix adds a prefix to all writes conducted by the writerfactory
func (wf WriterFactory) WithPrefix(prefix string) WriterFactory {
	return WrapWFWithPrefix(wf, prefix)
}
