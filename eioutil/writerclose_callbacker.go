package eioutil

import (
	"sync"
)

// HookedWriteCloser adds the ability to get callbacks priror and after a close call.
type HookedWriteCloser struct {
	WriteCloser

	isClosed bool
	mutex    sync.Mutex

	preCloseHooks  []func()
	postCloseHooks []func(error)
}

// NewWriterCloseCallback returns a new HookedWriteCloser that wraps the closer.
func NewWriterCloseCallback(wc WriteCloser) *HookedWriteCloser {
	return &HookedWriteCloser{
		WriteCloser: wc,

		isClosed:       false,
		mutex:          sync.Mutex{},
		preCloseHooks:  []func(){},
		postCloseHooks: []func(error){},
	}
}

// Close calls each pre-hook in order then closes the stream and calls each post-hook in order.
// PostClosehooks will have the result of the close operation forward to them.
func (h *HookedWriteCloser) Close() error {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	if h.isClosed {
		return ErrAlreadyClosed
	}

	for _, closer := range h.preCloseHooks {
		closer()
	}

	res := h.WriteCloser.Close()
	h.isClosed = true

	for _, closer := range h.postCloseHooks {
		closer(res)
	}

	return res
}

// AddPreCloseHooks adds one or more Pre-hooks to a close command. Each will be called in order before a close call
func (h *HookedWriteCloser) AddPreCloseHooks(f ...func()) *HookedWriteCloser {
	h.mutex.Lock()
	defer h.mutex.Unlock()
	h.preCloseHooks = append(h.preCloseHooks, f...)
	return h
}

// AddPostCloseHooks adds one or more Post-hooks to a close command. Each will be called in order before a close call
func (h *HookedWriteCloser) AddPostCloseHooks(f ...func(error)) *HookedWriteCloser {
	h.mutex.Lock()
	defer h.mutex.Unlock()
	h.postCloseHooks = append(h.postCloseHooks, f...)
	return h
}
