package writerfactory

import (
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWithPrefixyWriterFactory(t *testing.T) {
	BasePath := "BasePath"
	buffers, wf := GetMemoryWriterFactory()
	wf = wf.WithPrefix(BasePath)

	testCase1Path := "subpath"
	testCase1Content := []byte("testing string")

	writer, err := wf(testCase1Path)
	assert.NoError(t, err, "MemoryWriterFactory should never yield errors")
	writer.Write(testCase1Content)
	assert.Equal(t, testCase1Content, buffers[path.Join(BasePath, testCase1Path)].Bytes(), "MemWriter factory should store the results in buffers")

	testCase2Path := "subpath2"
	testCase2Content := []byte("testing string2")
	writer, err = wf(testCase2Path)
	assert.NoError(t, err, "MemoryWriterFactory should never yield errors")
	writer.Write(testCase2Content)
	assert.Equal(t, testCase2Content, buffers[path.Join(BasePath, testCase2Path)].Bytes(), "MemWriter factory should store the results in buffers")
}
