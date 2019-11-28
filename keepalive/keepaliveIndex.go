package keepalive

import (
	"context"
	"sync"
	"time"
)

// Index manages multiple keep alive instances indexed by a string. Will cleanup itself once an index is killed.
type Index struct {
	index map[string]*KeepAlive
	mu    sync.Mutex
}

// NewIndex returns a new Index
func NewIndex() *Index {
	return &Index{
		index: map[string]*KeepAlive{},
	}
}

// GetOrCreate will return the KeepAlive object situation at the index or create it using the arguements
func (ka *Index) GetOrCreate(ctx context.Context, indexKey string, maxIdle time.Duration, callbackOnCtxDone bool, callbacks ...func()) *KeepAlive {
	ka.mu.Lock()
	defer ka.mu.Unlock()

	if k, ok := ka.index[indexKey]; ok {
		return k
	}

	ka.index[indexKey] = New(ctx, maxIdle, callbackOnCtxDone, append(callbacks, func() {
		ka.mu.Lock()
		defer ka.mu.Unlock()
		delete(ka.index, indexKey)
	})...)

	return ka.index[indexKey]
}

// CloseAll terminates all keep alives
func (ka *Index) CloseAll() {
	for _, ka := range ka.index {
		ka.Close()
	}
}
