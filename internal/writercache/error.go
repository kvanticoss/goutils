package writercache

import "errors"

var ErrInvalidRecord = errors.New("record must be non nil and comparable (implement lesser interface)")
var ErrTooManyPartitions = errors.New("can not write new record, would create too many partions in the cache")
