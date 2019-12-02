package recordbuffer

import (
	"encoding/gob"
	"io"

	"github.com/kvanticoss/goutils/iterator"
)

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

	bytesWritten int
	gobEnc       *gob.Encoder
	gobDec       *gob.Decoder
}

func (bl *recordBuffer) Write(p []byte) (n int, err error) {
	n, err = bl.ReadWriteResetter.Write(p)
	bl.bytesWritten += n
	return n, err
}

func (bl *recordBuffer) WriteRecord(record interface{}) (int, error) {
	if bl.gobEnc == nil {
		bl.gobEnc = gob.NewEncoder(bl)
	}

	if record == nil {
		panic(record)
	}

	prevByteCount := bl.bytesWritten
	err := bl.gobEnc.Encode(record)
	return bl.bytesWritten - prevByteCount, err
}

// ReadRecord returns an iterator; not save for concurrent use
func (bl *recordBuffer) GetRecordIt(new func() interface{}) iterator.RecordIterator {
	if bl.gobDec == nil {
		bl.gobDec = gob.NewDecoder(bl)
	}

	return func() (interface{}, error) {
		dst := new()
		err := bl.gobDec.Decode(dst)
		if err == io.EOF {
			return nil, iterator.ErrIteratorStop
		}
		return dst, err
	}
}

// ReadRecord returns an iterator; not save for concurrent use
func (bl *recordBuffer) GetLesserIt(new func() iterator.Lesser) iterator.LesserIterator {
	if bl.gobDec == nil {
		bl.gobDec = gob.NewDecoder(bl)
	}

	return func() (iterator.Lesser, error) {
		dst := new()
		err := bl.gobDec.Decode(dst)
		if err == io.EOF {
			return nil, iterator.ErrIteratorStop
		}
		return dst, err
	}
}
