package eioutil_test

import (
	"bytes"
	"sync"
	"testing"

	"github.com/kvanticoss/goutils/eioutil"
	"github.com/stretchr/testify/assert"
)

func TestTimedSelfDestructAfterMaxBytes(t *testing.T) {
	var buffer bytes.Buffer

	m := sync.Mutex{}
	closeCalled := false
	writeCloser := eioutil.NewWriterCloserWithSelfDestructAfterMaxBytes(1, eioutil.NewWriteCloser(&buffer, func() error {
		m.Lock()
		defer m.Unlock()
		closeCalled = true
		return nil
	}))

	testString1 := "something with more than 1 byte"
	n, err := writeCloser.Write([]byte(testString1))
	assert.Equal(t, n, len([]byte(testString1)))
	assert.Equal(t, closeCalled, true)
	assert.NoError(t, err)

	testString2 := "Second write should be blocked"
	n, err = writeCloser.Write([]byte(testString2))
	assert.Equal(t, closeCalled, true)
	assert.Equal(t, []byte(testString1), buffer.Bytes(), true)
	assert.EqualError(t, eioutil.ErrAlreadyClosed, err.Error())
}
