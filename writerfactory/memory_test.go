package writerfactory

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// GetMemoryWriterFactory returns a writer factory which is backed by RAM
func TestGetMemoryWriterFactory(t *testing.T) {
	buffers, wf := GetMemoryWriterFactory()

	testCase1Path := "subpath"
	testCase1Content := []byte("testing string")

	writer, err := wf(testCase1Path)
	assert.NoError(t, err, "MemoryWriterFactory should never yeild errors")
	writer.Write(testCase1Content)
	assert.Equal(t, testCase1Content, buffers[testCase1Path].Bytes(), "MemWriter factory should store the results in buffers")

	testCase2Path := "subpath2"
	testCase2Content := []byte("testing string2")
	writer, err = wf(testCase2Path)
	assert.NoError(t, err, "MemoryWriterFactory should never yeild errors")
	writer.Write(testCase2Content)
	assert.Equal(t, testCase2Content, buffers[testCase2Path].Bytes(), "MemWriter factory should store the results in buffers")
}
