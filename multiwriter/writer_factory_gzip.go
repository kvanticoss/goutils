package multiwriter

import (
	"github.com/kvanticoss/goutils/eioutil"
	"github.com/kvanticoss/goutils/gzip"
)

// WFWithGzip adds gzip compresison to the writers that is returned by the underlying WriterFactory
func WFWithGzip(writerFactory WriterFactory) WriterFactory {
	return func(path string) (eioutil.WriteCloser, error) {
		w, err := writerFactory(path + ".gz")
		if err != nil {
			return nil, err
		}
		return gzip.NewWriter(w), err
	}
}
