package keyvaluelist

import (
	"fmt"
	"net/url"
	"strings"
)

// ErrInvalidFormat is returned when FromHadoopPartition fails to parse the input
var ErrInvalidFormat = fmt.Errorf("invalid format for hadoop partitions; must be QueryEscape({key})=QueryEscape({val})")

// KeyValue is a string tuple used to represent an order list of KeyValue pairs (unlike a map which is un-ordered)
type KeyValue struct {
	Key, Value string
}

// ToHadoopPartition encodes the KeyValue pair into a hadoop style partition string with the key and value QueryEncoded
func (kv KeyValue) ToHadoopPartition() string {
	return url.QueryEscape(kv.Key) + "=" + url.QueryEscape(kv.Value)
}

// FromHadoopPartition encodes the KeyValue pair into a hadoop style partition string with the key and value QueryEncoded
func FromHadoopPartition(in string) (KeyValue, error) {
	elems := strings.Split(in, "=")
	if len(elems) != 2 {
		return KeyValue{}, ErrInvalidFormat
	}

	var err error
	elems[0], err = url.QueryUnescape(elems[0])
	if err != nil {
		return KeyValue{}, ErrInvalidFormat
	}

	elems[1], err = url.QueryUnescape(elems[1])
	if err != nil {
		return KeyValue{}, ErrInvalidFormat
	}

	return KeyValue{elems[0], elems[1]}, nil
}
