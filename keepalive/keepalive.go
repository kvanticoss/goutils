package keepalive

import (
	"context"
	"errors"
	"sync"
	"time"
)

// ErrAlreadyClosed is returned when any operation id done after the TTL has expired
var ErrAlreadyClosed = errors.New("writer is closed")

// KeepAlive allows users to implement keep alive patterns where after a certain amount of inactivty callbacks are triggerd
type KeepAlive struct {
	ctx       context.Context
	ctxCancel func()
	mutex     sync.RWMutex

	lastPing      time.Time
	callbackOnCtx bool
	maxIdle       time.Duration
	callbacks     []func()
	done          bool
}

// New returns a KeepAlive object which will call each callback sequencially after maxIdle time has passed
// withouth KeepAlive.Ping() being called.
// callbacks will not be invoked on ctx.Done unless callbackOnCtxDone == true
// Please note that due to how golang manages channels, there is a risk that Ping can be called and directly after each of
// the callbacks are invoked (even though the maxIdle hasn't been reached since the last ping).
func New(ctx context.Context, maxIdle time.Duration, callbackOnCtxDone bool, callbacks ...func()) *KeepAlive {
	ctx, cancel := context.WithCancel(ctx)
	res := &KeepAlive{
		ctx:           ctx,
		ctxCancel:     cancel,
		mutex:         sync.RWMutex{},
		lastPing:      time.Now(),
		callbackOnCtx: callbackOnCtxDone,
		maxIdle:       maxIdle,
		callbacks:     callbacks,
		done:          false,
	}

	go res.monitorIdle()

	return res
}

func (ka *KeepAlive) monitorIdle() {
	ch := time.After(ka.maxIdle)
	for {
		select {
		case <-ch:
			ka.mutex.RLock()
			tSinceLastReset := time.Now().Sub(ka.lastPing)
			ka.mutex.RUnlock()

			if tSinceLastReset >= ka.maxIdle {
				ka.Close()
				return
			}
			ch = time.After(ka.maxIdle - tSinceLastReset)

		case <-ka.ctx.Done():
			if ka.callbackOnCtx {
				ka.Close()
			}
			return
		}
	}
}

// Close will terminate the keepalive; call the callbacks and frees up resources.
func (ka *KeepAlive) Close() {
	ka.mutex.Lock()
	defer ka.mutex.Unlock()

	if ka.done {
		return
	}

	for _, f := range ka.callbacks {
		f()
	}

	ka.ctxCancel()
	ka.done = true
}

// LastPing holds the timestamp of the last ping / timer done
func (ka *KeepAlive) LastPing() time.Time {
	return ka.lastPing
}

func (ka *KeepAlive) Done() (res bool) {
	ka.mutex.RLock()
	res = ka.done
	ka.mutex.RUnlock()
	return res
}

// TimeRemainging returns the duration until the callbacks will be triggerd if no more Ping() are called
func (ka *KeepAlive) TimeRemainging() time.Duration {
	return ka.maxIdle - time.Now().Sub(ka.lastPing)
}

// Ping dones the idle timer to zero; non blocking
func (ka *KeepAlive) Ping() {
	now := time.Now()

	// Locks are costly; we can redue the cost by only locking if sufficient time has passed since the last timer reset
	ka.mutex.RLock()
	timeCopy := ka.lastPing
	ka.mutex.RUnlock()

	if now.Sub(timeCopy) >= ka.maxIdle/100 { // Allow 1 % skew in the timer for some performance
		ka.mutex.Lock()
		ka.lastPing = now
		ka.mutex.Unlock()
	}
}
