package writerfactory

import (
	"compress/gzip"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWithGzipWriterFactory(t *testing.T) {
	buffers, rawWF := GetMemoryWriterFactory()
	gzipWf := rawWF.WithGzip()

	testCase1Path := "subpath"
	testCase1Content := []byte("testing string")

	// Write to the automatic gzipper
	writer, err := gzipWf(testCase1Path)
	assert.NoError(t, err, "MemoryWriterFactory should never yield errors")
	writer.Write(testCase1Content)
	writer.Close()

	// Create a refrence
	rawWriter, err := rawWF("rawGzipWritten.gz")
	assert.NoError(t, err, "MemoryWriterFactory should never yield errors")
	gzipWriter := gzip.NewWriter(rawWriter)
	gzipWriter.Write(testCase1Content)
	gzipWriter.Flush()
	gzipWriter.Close()

	assert.Equal(t, buffers["rawGzipWritten.gz"].Bytes(), buffers[testCase1Path+".gz"].Bytes(), "MemWriter factory should store the results in buffers")
}
