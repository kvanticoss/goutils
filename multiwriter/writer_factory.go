package multiwriter

import "github.com/kvanticoss/goutils/eioutil"

// WriterFactory should yeild a new WriteCloser under the given path.
type WriterFactory func(path string) (wc eioutil.WriteCloser, err error)
