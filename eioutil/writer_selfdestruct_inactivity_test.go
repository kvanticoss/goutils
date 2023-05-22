package eioutil_test

import (
	"bytes"
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/kvanticoss/goutils/v2/eioutil"
	"github.com/stretchr/testify/assert"
)

func TestTimedSelfDestructAfterIdle(t *testing.T) {
	var buffer bytes.Buffer
	maxIdle := time.Millisecond * 10
	testdata := []byte("this is a test")
	m := sync.Mutex{}
	closeCalled := false

	writeCloser := eioutil.NewWriterCloserWithSelfDestructAfterIdle(context.Background(), maxIdle, eioutil.NewWriteCloser(&buffer, func() error {
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

func TestTimedSelfDestructAfterIdleNoInterupts(t *testing.T) {
	var buffer bytes.Buffer
	maxIdle := time.Millisecond * 10
	testdata := []byte("this is a test")

	w := eioutil.NewPreWriteCallback(&buffer, func(_ []byte) error {
		time.Sleep(maxIdle * 2)
		return nil
	})
	wc := eioutil.NewWriteCloser(w, func() error { return nil })

	writeCloser := eioutil.NewWriterCloserWithSelfDestructAfterIdle(context.Background(), maxIdle, wc)
	// Every write should reset the timeout
	_, err := writeCloser.Write(testdata)
	assert.NoError(t, err)

	assert.EqualValues(t, testdata, buffer.Bytes(), "all data should be written even if a timeout occurs")

	// Additional writes should fail
	_, err = writeCloser.Write(testdata)
	assert.EqualValues(t, eioutil.ErrAlreadyClosed, err)

}

func BenchmarkNewWriterCloserWithSelfDestructAfterIdle(b *testing.B) {
	b.StopTimer()
	maxIdle := time.Second * 2
	testdata := []byte("this is a test..") //
	writeCloser := eioutil.NewWriterCloserWithSelfDestructAfterIdle(context.Background(), maxIdle, eioutil.NewWriteNOPCloser(io.Discard))
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

	b.Logf("Took %v to save 16 byte * 1 000 000 => 16 MB %d times", time.Since(tstart), b.N)

	writeCloser.Close()

}
