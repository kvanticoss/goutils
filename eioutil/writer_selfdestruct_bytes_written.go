package eioutil

// NewWriterCloserWithSelfDestructAfterMaxBytes will automatically close a file once
// maxBytes have been written to it. Any further writes will return an eioutil.ErrAlreadyClosed
// if the underlying writer returns an error on Close(); that error will be returned on a write
// which would have forced a file to be closed.
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
