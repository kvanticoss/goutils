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

// GetOrCreate will return the KeepAlive object situation at the index or create it using the arguments
// Due to the async nature of this package it is possible that a KeepAlive object is returned which is already
// done; as such the user should do ka := index.GetOrCreate(....); ka.Ping(); if ka.Done() { retry... }.
// This is automatically handled by the PingOrCreate method. This is the preferred method.
func (ka *Index) GetOrCreate(ctx context.Context, indexKey string, maxIdle time.Duration, callbackOnCtxDone bool, callbacks ...func()) *KeepAlive {
	ka.mu.Lock()
	defer ka.mu.Unlock()

	if k, ok := ka.index[indexKey]; ok {
		return k
	}

	ka.index[indexKey] = New(ctx, maxIdle, callbackOnCtxDone, append(callbacks, ka.getIndexCleanupHook(indexKey))...)

	return ka.index[indexKey]
}

// PingOrCreate does the same as GetOrCreate but handles the case when the KeepAlive object is killed while we return it.
// in such cases; PingOrCreate will recreate a new KeepAlive Object.
func (ka *Index) PingOrCreate(ctx context.Context, indexKey string, maxIdle time.Duration, callbackOnCtxDone bool, callbacks ...func()) *KeepAlive {
	k := ka.GetOrCreate(ctx, indexKey, maxIdle, callbackOnCtxDone, callbacks...)
	k.Ping()
	if k.Done() { // It got cancelled before we could run our .Ping()
		return ka.GetOrCreate(ctx, indexKey, maxIdle, callbackOnCtxDone, callbacks...)
	}
	return k
}

// CloseAll terminates all keep alives
func (ka *Index) CloseAll() {
	for _, ka := range ka.index {
		ka.Close()
	}
}

func (ka *Index) getIndexCleanupHook(indexKey string) func() {
	return func() {
		ka.mu.Lock()
		defer ka.mu.Unlock()
		delete(ka.index, indexKey)
	}
}
