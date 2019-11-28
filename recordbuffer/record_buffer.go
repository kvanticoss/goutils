package recordbuffer

import (
	"encoding/json"

	"github.com/kvanticoss/goutils/iterator"
)

var jsonRecordDelimiter = []byte("\n")

// ReadWriteResetterFactory should return a RecordBuffer. The simplest implementation of this factory is func(_ []byte) *bytes.Buffer {return &bytes.NewBuffer{}}
// but theoretically (not yet tested) wrapping https://github.com/ShoshinNikita/go-disk-buffer should also do.
type ReadWriteResetterFactory func() ReadWriteResetter

// ReadWriteResetter is the bare minimum needed from the bytes.Buffer interface to implement a record cache
type ReadWriteResetter interface {
	Read(p []byte) (n int, err error)
	Write(p []byte) (n int, err error)
	Reset()
}

type recordBuffer struct {
	ReadWriteResetter
}

func (bl *recordBuffer) WriteRecord(record interface{}) (int, error) {
	d, err := json.Marshal(record)
	if err != nil {
		return 0, err
	}
	return bl.ReadWriteResetter.Write(append(d, jsonRecordDelimiter...))
}

// ReadRecord returns an iterator; not save for concurrent use
func (bl *recordBuffer) GetRecordIt(new func() interface{}) iterator.RecordIterator {
	dec := json.NewDecoder(bl)
	return func() (interface{}, error) {
		dst := new()
		if !dec.More() {
			return nil, iterator.ErrIteratorStop
		}
		return dst, dec.Decode(dst)
	}
}

// ReadRecord returns an iterator; not save for concurrent use
func (bl *recordBuffer) GetLesserIt(new func() iterator.Lesser) iterator.LesserIterator {
	dec := json.NewDecoder(bl)
	return func() (iterator.Lesser, error) {
		dst := new()
		if !dec.More() {
			return nil, iterator.ErrIteratorStop
		}
		return dst, dec.Decode(dst)
	}
}
