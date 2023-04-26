package eioutil

import (
	"context"
	"sync"
	"time"

	"github.com/kvanticoss/goutils/keepalive"
)

// NewWriterCloserWithSelfDestructAfterIdle will add a timeout that when reached the writecloser has been inactive (no writes)
// for the duration, it will be automatically closed and any future writes will yield ErrDestructed
func NewWriterCloserWithSelfDestructAfterIdle(ctx context.Context, maxIdle time.Duration, wc WriteCloser) WriteCloser {
	var once sync.Once
	var closeErr error
	onceBody := func() {
		closeErr = wc.Close()
	}

	lock := make(chan struct{}, 1)

	KA := keepalive.New(ctx, maxIdle, true, func() {
		lock <- struct{}{}
		once.Do(onceBody)
		<-lock // cleanup
		close(lock)
	})

	// Let's update the last access both before and after so we don't end up
	// in a situation where a file that has been written for a long time
	// is closed directly after the write.
	writer := NewPreWriteCallback(wc, func(_ []byte) error {
		KA.Ping()
		if KA.Done() {
			return ErrAlreadyClosed
		}
		lock <- struct{}{}
		return nil
	})
	writer = NewPostWriteCallback(writer, func(_ []byte) error {
		<-lock
		KA.Ping()
		return nil
	})

	return NewWriteCloser(writer, func() error {
		KA.Close()
		return closeErr
	})
}
