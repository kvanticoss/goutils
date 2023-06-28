package writerfactory

import (
	"io"
	"path"
)

// WrapWFWithPrefix adds a path prefix to all writes conducted by the writerfactory
func WrapWFWithPrefix(wf WriterFactory, prefix string) WriterFactory {
	return func(writePath string) (io.WriteCloser, error) {
		return wf(path.Join(prefix, writePath))
	}
}

// WithPrefix adds a path prefix to all writes conducted by the writerfactory
func (wf WriterFactory) WithPrefix(prefix string) WriterFactory {
	return WrapWFWithPrefix(wf, prefix)
}

// WrapWFWithSuffix adds a path suffix to all writes conducted by the writerfactory
func WrapWFWithSuffix(wf WriterFactory, suffix string) WriterFactory {
	return func(writePath string) (io.WriteCloser, error) {
		return wf(path.Join(writePath, suffix))
	}
}

// WithSuffix adds a Suffix to all writes conducted by the writerfactory
func (wf WriterFactory) WithSuffix(suffix string) WriterFactory {
	return WrapWFWithSuffix(wf, suffix)
}
