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

// n__ewWriterCloserWithSelfDestructAfterIdle will add a timeout that when reached the writecloser has been inactive (no writes)
// for the duration, it will be automatically closed and any future writes will yeild ErrDestructed
/*

//
// Keept here for now since this version have slightly better performance.
//
func N_ewWriterCloserWithSelfDestructAfterIdle(_ context.Context, maxIdleTime time.Duration, wc WriteCloser) WriteCloser {
	done := make(chan struct{}, 1)

	m := sync.RWMutex{}
	lastReset := time.Now()

	// Let's update the last access both before and after so we don't end up
	// in a situation where a file that has been written for a long time
	// is closed directly after the write.
	writer := NewPrePostWriteCallback(wc, func(_ []byte) error {
		select {
		case <-done:
			return ErrAlreadyClosed
		default:
			now := time.Now()

			// Locks are costly; we can redue the cost by only locking if sufficient time has passed since the last timer reset
			if now.Sub(lastReset) >= maxIdleTime/20 { // Allow 5 % skew in the timer for some performance
				m.Lock()
				lastReset = now
				m.Unlock()
			}

			return nil
		}
		return nil
	})

	writeCloser := NewWriteCloser(writer, func() error {
		select {
		case <-done:
			return ErrAlreadyClosed
		default:
			close(done)
			return wc.Close()
		}
	})

	go func() {
		ch := time.After(maxIdleTime)
		for {
			select {
			case <-ch:
				m.RLock()
				if tSinceLastReset := time.Now().Sub(lastReset); tSinceLastReset >= maxIdleTime {
					writeCloser.Close()
					return
				} else {
					ch = time.After(maxIdleTime - tSinceLastReset)
				}
				m.RUnlock()
			}
		}
	}()

	return writeCloser
}
*/
