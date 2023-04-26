package writerfactory

import (
	"os"
	"path/filepath"

	"github.com/kvanticoss/goutils/eioutil"
)

// GetLocalWriterFactory returns a writer factory which creates local files in the basePath.
// If basePath == "" it defaults to current working dir ("./")
func GetLocalWriterFactory(basePath string) WriterFactory {
	if basePath == "" {
		basePath = "./"
	}
	return func(path string) (wc eioutil.WriteCloser, err error) {
		os.MkdirAll(filepath.Dir(basePath+path), os.ModePerm)
		w, err := os.Create(basePath + path)
		if err != nil {
			return nil, err
		}
		return w, err
	}
}
