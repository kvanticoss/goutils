package writerfactory

import (
	"bytes"

	"github.com/kvanticoss/goutils/eioutil"
)

// GetMemoryWriterFactory returns a writer factory which is backed by RAM
func GetMemoryWriterFactory() (map[string]*bytes.Buffer, WriterFactory) {
	res := map[string]*bytes.Buffer{}
	wf := func(path string) (wc eioutil.WriteCloser, err error) {
		_, ok := res[path]
		if !ok {
			res[path] = bytes.NewBuffer(nil)
		}
		return eioutil.NewWriteNOPCloser(res[path]), nil
	}

	return res, wf
}
