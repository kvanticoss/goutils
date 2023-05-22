package writerfactory

import (
	"bytes"
	"io"

	"github.com/kvanticoss/goutils/v2/eioutil"
)

// GetMemoryWriterFactory returns a writer factory which is backed by RAM
func GetMemoryWriterFactory() (map[string]*bytes.Buffer, WriterFactory) {
	res := map[string]*bytes.Buffer{}
	wf := func(path string) (wc io.WriteCloser, err error) {
		_, ok := res[path]
		if !ok {
			res[path] = bytes.NewBuffer(nil)
		}
		return eioutil.NewWriteNOPCloser(res[path]), nil
	}

	return res, wf
}
