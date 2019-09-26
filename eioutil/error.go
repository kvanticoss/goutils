package eioutil

import "errors"

// ErrAlreadyClosed is returned when any operation id done on a already closed entity
var ErrAlreadyClosed = errors.New("writer is closed")
