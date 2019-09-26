package eioutil

// NewWriterCloserWithSelfDestructAfterMaxBytes will add a timeout that when reached the writecloser has been inactive (no writes)
// for the duration, it will be automatically closed and any future writes will yeild ErrDestructed
func NewWriterCloserWithSelfDestructAfterMaxBytes(maxBytes int, wc WriteCloser) WriteCloser {
	bytesWritten := 0
	closed := false

	writer := NewPreWriteCallback(wc, func(p []byte) error {
		if closed {
			return ErrAlreadyClosed
		}
		return nil
	})

	writer = NewPostWriteCallback(writer, func(p []byte) error {
		bytesWritten += len(p)
		if bytesWritten >= maxBytes {
			closed = true
			return wc.Close()
		}
		return nil
	})

	return NewWriteCloser(writer, func() error {
		closed = true
		return wc.Close()
	})
}
