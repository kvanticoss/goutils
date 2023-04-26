package eioutil

import "sync"

type syncedWriteCloser struct {
	WriteCloser

	isClosed bool
	mutex    sync.Mutex
}

var _ WriteCloser = &syncedWriteCloser{}

// NewSyncedWriteCloser ensures that Write and Close calls are protected with mutexes
// to be safe for concurrent writes. It is up to the consumer to ensure that atomic
// writes can be interlaced. After the first Call to close all future calls to Close
// will return ErrAlreadyClosed.
func NewSyncedWriteCloser(wc WriteCloser) WriteCloser {
	return &syncedWriteCloser{wc, false, sync.Mutex{}}
}

// Write implements the io.Writer but each Write() or Close() call is mutex protected
func (swc *syncedWriteCloser) Write(p []byte) (n int, err error) {
	swc.mutex.Lock()
	defer swc.mutex.Unlock()
	if swc.isClosed {
		return 0, ErrAlreadyClosed
	}
	return swc.WriteCloser.Write(p)
}

// Write implements the io.Writer but each Write() or Close() call is mutex protected
func (swc *syncedWriteCloser) Close() error {
	swc.mutex.Lock()
	defer swc.mutex.Unlock()
	if swc.isClosed {
		return ErrAlreadyClosed
	}
	swc.isClosed = true
	return swc.WriteCloser.Close()
}
