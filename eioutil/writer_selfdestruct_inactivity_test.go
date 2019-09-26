package eioutil_test

import (
	"bytes"
	"sync"
	"testing"
	"time"

	"github.com/kvanticoss/goutils/eioutil"
	"github.com/stretchr/testify/assert"
)

func TestTimedSelfDestructAfterIdle(t *testing.T) {
	var buffer bytes.Buffer
	maxIdle := time.Millisecond * 10
	testdata := []byte("this is a test")
	m := sync.Mutex{}
	closeCalled := false

	writeCloser := eioutil.NewWriterCloserWithSelfDestructAfterIdle(maxIdle, eioutil.NewWriteCloser(&buffer, func() error {
		m.Lock()
		defer m.Unlock()
		closeCalled = true
		return nil
	}))
	// Every write should reset the timeout
	_, err := writeCloser.Write(testdata)
	assert.NoError(t, err)
	time.Sleep(maxIdle / 2)
	_, err = writeCloser.Write(testdata)
	assert.NoError(t, err)
	time.Sleep(maxIdle / 2)
	_, err = writeCloser.Write(testdata)
	assert.NoError(t, err)
	time.Sleep(maxIdle / 2)
	_, err = writeCloser.Write(testdata)
	assert.NoError(t, err)
	time.Sleep(maxIdle / 2)

	// Reset the buffer for the last write test
	buffer.Reset()
	_, err = writeCloser.Write(testdata)
	assert.NoError(t, err)

	assert.Equal(t, testdata, buffer.Bytes())
	time.Sleep(maxIdle * 2)
	m.Lock()
	assert.True(t, closeCalled)
	m.Unlock()

	_, err = writeCloser.Write([]byte("SHOULD FAIL"))
	assert.Error(t, err, "No writes should be possible after timeout")
	_, err = writeCloser.Write([]byte("SHOULD FAIL"))
	assert.Error(t, err, "No writes should be possible after timeout")
	_, err = writeCloser.Write([]byte("SHOULD FAIL"))
	assert.Error(t, err, "No writes should be possible after timeout")
}

func BenchmarkNewWriterCloserWithSelfDestructAfterIdle(b *testing.B) {
	b.StopTimer()
	var buffer bytes.Buffer
	maxIdle := time.Second * 2
	testdata := []byte("this is a test..") //
	writeCloser := eioutil.NewWriterCloserWithSelfDestructAfterIdle(maxIdle, eioutil.NewWriteNOPCloser(&buffer))
	tstart := time.Now()
	b.StartTimer()
	for n := 0; n < b.N; n++ {
		for i := 0; i < 1000000; i++ {
			_, err := writeCloser.Write(testdata)
			if err != nil {
				b.Fatal(err)
			}
		}
	}
	b.StopTimer()

	b.Logf("Took %v to save 16 byte * 1 000 000 => 16 MB %d times", time.Now().Sub(tstart), b.N)

	writeCloser.Close()

}
