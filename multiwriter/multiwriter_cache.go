package multiwriter

import (
	"context"
	"fmt"
	"io"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/kvanticoss/goutils/eioutil"

	multierror "github.com/hashicorp/go-multierror"
)

// Cache is a utillity that keeps an index of multiple writers, indexed by a string (most often path)
// if a writer is requested and doesn't exist it gets created (using the provided factory). Writers that aren't
// used for long enough are automatically closed. All writers are also closed on the ctx.Done.
type Cache struct {
	ctx            context.Context
	cxtCancel      func()
	mutex          *sync.RWMutex
	writerFactory  WriterFactory
	writers        map[string]eioutil.WriteCloser
	ttl            time.Duration
	writersCreated int
	maxCacheSize   int
}

// NewCache will dynamically open writers for writing and close them on inactivity
func NewCache(ctx context.Context, writerFactory WriterFactory, ttl time.Duration, maxCacheSize int) *Cache {
	ctx, cancel := context.WithCancel(ctx)
	c := &Cache{
		ctx:            ctx,
		cxtCancel:      cancel,
		mutex:          &sync.RWMutex{},
		writerFactory:  nil,
		writers:        map[string]eioutil.WriteCloser{},
		ttl:            ttl,
		writersCreated: 0,
		maxCacheSize:   maxCacheSize,
	}

	c.writerFactory = func(path string) (wc eioutil.WriteCloser, err error) {
		wc, err = writerFactory(path)
		if err != nil {
			return wc, err
		}

		// Make this writer self destruct with some cleanup code bofore doing so.
		return eioutil.NewWriterCloserWithSelfDestructAfterIdle(
			ttl,
			eioutil.NewWriterCloseCallback(wc).AddPreCloseHooks(func() {
				c.mutex.Lock()
				defer c.mutex.Unlock()
				delete(c.writers, path)
			}),
		), nil
	}

	return c
}

// Close closes all opened writers; will continue on error and return all (if any) errors
func (mfw *Cache) Close() error {
	var allErrors *multierror.Error

	// Avoid races since we will be clearing keys form  mfw.writers in the Cloase calls
	writercopy := map[string]eioutil.WriteCloser{}
	mfw.mutex.Lock()
	for k, v := range mfw.writers {
		writercopy[k] = v
	}
	mfw.mutex.Unlock()

	for path := range writercopy {
		if err := mfw.ClosePath(path); err != nil { // && err != eioutil.ErrAlreadyClosed { // since we don't want to have a mutex here, chances are that ErrAlreadyClosed can happen
			allErrors = multierror.Append(allErrors, err)
		}
	}
	return allErrors.ErrorOrNil()
}

// ClosePath closes one specific path; will return nil if the path doesn't exist or is already closed; Otherwise the reuslt of the writer Close function.
func (mfw *Cache) ClosePath(path string) error {
	mfw.mutex.Lock()

	writer, ok := mfw.writers[path]
	if !ok {
		mfw.mutex.Unlock()
		return nil
	}
	mfw.mutex.Unlock()

	return writer.Close()
}

func (mfw *Cache) getWriter(path string) (io.Writer, error) {
	// Let's start with a lightweight read lock
	mfw.mutex.RLock()
	writer, ok := mfw.writers[path]
	if ok {
		mfw.mutex.RUnlock()
		return writer, nil
	}
	mfw.mutex.RUnlock()

	// OK let's grab a write lock
	mfw.mutex.Lock()
	defer mfw.mutex.Unlock()
	// Double check that a writer hasn't been created after we released the lock
	writer, ok = mfw.writers[path]
	if ok {
		return writer, nil
	}

	// Suffix should be an always lexographically increasing id
	suffix := fmt.Sprintf("%d_%d", mfw.writersCreated, time.Now().Unix())
	newSuffixedPath := strings.Replace(path, "{suffix}", suffix, -1)

	// Create new writer and save for later
	writer, err := mfw.writerFactory(newSuffixedPath)
	if err != nil {
		return nil, err
	}
	mfw.writers[path] = writer
	mfw.writersCreated++

	return writer, nil
}

// GetWriter gets an existing writers for the path or creates one and saves it for later re-use. If the path contains {suffix}
// it will be replaced by a unique counter + timestamp.
func (mfw *Cache) Write(path string, p []byte) (int, error) {
	select {
	case <-mfw.ctx.Done():
		return 0, context.Canceled
	default:
	}

	writer, err := mfw.getWriter(path)
	if err != nil {
		return 0, err
	}

	if n, err := writer.Write(p); err == nil {
		return n, nil
	} else if err == eioutil.ErrAlreadyClosed { // Make once race condition less likely
		log.Printf("Retrying write as ErrAlreadyClosed")
		return mfw.Write(path, p)
	} else {
		return n, err
	}
}
