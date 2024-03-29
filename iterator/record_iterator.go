package iterator

// RecordIterator is a function which yield any golang data struct each time called
// Where there are no more records; ErrIteratorStop should be returned and should not
// be treated as an error (compare it to io.EOF)
type RecordIterator[T any] func() (T, error)
