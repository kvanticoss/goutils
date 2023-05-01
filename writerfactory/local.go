package writerfactory

import (
	"io"
	"os"
	"path/filepath"
)

// GetLocalWriterFactory returns a writer factory which creates local files in the basePath.
// If basePath == "" it defaults to current working dir ("./")
func GetLocalWriterFactory(basePath string) WriterFactory {
	if basePath == "" {
		basePath = "./"
	}
	return func(path string) (wc io.WriteCloser, err error) {
		os.MkdirAll(filepath.Dir(basePath+path), os.ModePerm)
		w, err := os.Create(basePath + path)
		if err != nil {
			return nil, err
		}
		return w, err
	}
}
