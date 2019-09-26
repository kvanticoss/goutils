package eioutil

import "sync"

type syncedWriteCloser struct {
	WriteCloser

	mutex sync.Mutex
}

var _ WriteCloser = &syncedWriteCloser{}

// NewCyncedWriteCloser ensures that Write and Close calls are protected with mutexes
func NewSyncedWriteCloser(wc WriteCloser) WriteCloser {
	return &syncedWriteCloser{wc, sync.Mutex{}}
}

// Write implements the io.Writer interface but defferes the write to the callback.
func (swc *syncedWriteCloser) Write(p []byte) (n int, err error) {
	swc.mutex.Lock()
	defer swc.mutex.Unlock()
	return swc.WriteCloser.Write(p)
}

// Write implements the io.Writer interface but defferes the write to the callback.
func (swc *syncedWriteCloser) Close() error {
	swc.mutex.Lock()
	defer swc.mutex.Unlock()
	return swc.WriteCloser.Close()
}
