package writerfactory

import (
	"io"
	"strings"

	"github.com/kvanticoss/goutils/v2/gzip"
)

// WrapWFWithGzip adds gzip compresison to the writers that is returned by the underlying WriterFactory
func WithGzip(wf WriterFactory) WriterFactory {
	return func(path string) (io.WriteCloser, error) {
		if !strings.HasSuffix(path, ".gz") {
			path = path + ".gz"
		}

		w, err := wf(path)
		if err != nil {
			return nil, err
		}
		return gzip.NewWriter(w), err
	}
}

// WithGzip adds gzip compresison to the writers that is returned by the underlying WriterFactory
func (wf WriterFactory) WithGzip() WriterFactory {
	return WithGzip(wf)
}
