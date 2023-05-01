package writerfactory

import "io"

// WriterFactory should yield a new WriteCloser under the given path.
type WriterFactory func(path string) (wc io.WriteCloser, err error)
