package eioutil

import (
	"context"
	"sync"
	"time"

	"github.com/kvanticoss/goutils/keepalive"
)

// NewWriterCloserWithSelfDestructAfterIdle will add a timeout that when reached the writecloser has been inactive (no writes)
// for the duration, it will be automatically closed and any future writes will yeild ErrDestructed
func NewWriterCloserWithSelfDestructAfterIdle(ctx context.Context, maxIdle time.Duration, wc WriteCloser) WriteCloser {
	var once sync.Once
	var closeErr error
	onceBody := func() {
		closeErr = wc.Close()
	}

	KA := keepalive.New(ctx, maxIdle, true, func() {
		once.Do(onceBody)
	})

	// Let's update the last access both before and after so we don't end up
	// in a situation where a file that has been written for a long time
	// is closed directly after the write.
	writer := NewPrePostWriteCallback(wc, func(_ []byte) error {
		KA.Ping()
		if KA.Done() {
			return ErrAlreadyClosed
		}
		return nil
	})

	return NewWriteCloser(writer, func() error {
		KA.Close()
		return closeErr
	})
}
