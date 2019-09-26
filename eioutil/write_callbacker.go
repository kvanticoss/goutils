package eioutil

import (
	"io"
)

type callbackWriter struct {
	callback func(p []byte) (err error)
}

var _ io.Writer = &callbackWriter{}

// NewPreWriteCallback spins up a new io.MulitiWriter where callbacks are invoked before the
// underlying writer. Errors in the callback will block/stop writes to the underlying writer.
// Can be use to block/delay writes as well as conditionally abort writes.
func NewPreWriteCallback(w io.Writer, callbacks ...func([]byte) error) io.Writer {
	writers := make([]io.Writer, len(callbacks)+1)
	for index, cb := range callbacks {
		writers[index] = &callbackWriter{cb}
	}
	writers[len(callbacks)] = w
	return io.MultiWriter(writers...)
}

// NewPostWriteCallback spins up a new io.MulitiWriter where the callbacks are invoked after the
// underlying writer. Errors in the underlying writer.Write() will not trigger a call to callbacks.
func NewPostWriteCallback(w io.Writer, callbacks ...func([]byte) error) io.Writer {
	writers := make([]io.Writer, len(callbacks)+1)
	writers[0] = w
	for index, cb := range callbacks {
		writers[index+1] = &callbackWriter{cb}
	}
	return io.MultiWriter(writers...)
}

// NewPrePostWriteCallback spins up a new io.MulitiWriter where the callbacks are invoked before AND after the
// underlying writer. Errors in the underlying writer.Write() will not trigger a call to callbacks.
func NewPrePostWriteCallback(w io.Writer, callbacks ...func([]byte) error) io.Writer {
	numOfCallbacks := len(callbacks)
	writers := make([]io.Writer, numOfCallbacks*2+1)
	for index, cb := range callbacks {
		writers[index] = &callbackWriter{cb}
	}
	writers[numOfCallbacks] = w
	for index, cb := range callbacks {
		writers[numOfCallbacks+1+index] = &callbackWriter{cb}
	}
	return io.MultiWriter(writers...)
}

// Write implements the io.Writer interface but defferes the write to the callback.
func (iwc *callbackWriter) Write(p []byte) (n int, err error) {
	return len(p), iwc.callback(p)
}
