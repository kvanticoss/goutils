package writercache

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/kvanticoss/goutils/eioutil"
	"github.com/kvanticoss/goutils/writerfactory"

	multierror "github.com/hashicorp/go-multierror"
)

// Cache is a utillity that keeps an index of multiple writers, indexed by a string (most often path)
// if a writer is requested and doesn't exist it gets created (using the provided factory). Writers that aren't
// used for long enough are automatically closed. All writers are also closed on the ctx.Done.
type Cache struct {
	ctx             context.Context
	cxtCancel       func()
	mutex           *sync.RWMutex
	writerFactory   writerfactory.WriterFactory
	writers         map[string]*timedWriter
	ttl             time.Duration
	writersCreated  int
	maxCacheEntires int
}

type timedWriter struct {
	eioutil.WriteCloser

	lastAccessMutex sync.RWMutex
	lastAccess      time.Time
}

func (tw *timedWriter) updateTs() {
	if tw == nil {
		return
	}

	tw.lastAccessMutex.Lock()
	defer tw.lastAccessMutex.Unlock()
	tw.lastAccess = time.Now()
}

func (tw *timedWriter) getTs() time.Time {
	tw.lastAccessMutex.RLock()
	defer tw.lastAccessMutex.RUnlock()
	return tw.lastAccess
}

// NewCache will dynamically open writers for writing and close them on inactivity or when maxCacheEntires has been populated into the cache, at which point the oldest item will be closed.
func NewCache(ctx context.Context, writerFactory writerfactory.WriterFactory, ttl time.Duration, maxCacheEntires int) *Cache {
	ctx, cancel := context.WithCancel(ctx)
	c := &Cache{
		ctx:             ctx,
		cxtCancel:       cancel,
		mutex:           &sync.RWMutex{},
		writerFactory:   nil,
		writers:         map[string]*timedWriter{},
		ttl:             ttl,
		writersCreated:  0,
		maxCacheEntires: maxCacheEntires,
	}

	hostname, err := os.Hostname()
	if err != nil {
		hostname = ""
	}

	c.writerFactory = func(path string) (wc eioutil.WriteCloser, err error) {
		// Suffix should be an always lexographically increasing id
		suffix := fmt.Sprintf("%s_%d_%04d", hostname, time.Now().Unix(), c.writersCreated)
		newSuffixedPath := strings.Replace(path, "{suffix}", suffix, -1)

		wc, err = writerFactory(newSuffixedPath)
		if err != nil {
			return wc, err
		}

		if maxCacheEntires > 0 && len(c.writers) > maxCacheEntires {
			//log.Printf("Cache is %d and max is %d; pruing", len(c.writers), maxCacheEntires)
			go c.pruneCache()
		}

		wc = eioutil.NewSyncedWriteCloser(wc)

		// Make this writer self destruct with some cleanup code bofore doing so.
		return eioutil.NewWriterCloserWithSelfDestructAfterIdle(
			ctx,
			ttl,
			eioutil.NewWriterCloseCallback(wc).AddPreCloseHooks(func() {
				c.mutex.Lock()
				defer c.mutex.Unlock()
				if _, ok := c.writers[path]; ok {
					delete(c.writers, path)
				}
			}),
		), nil
	}

	return c
}

func (mfw *Cache) pruneCache() {
	path := ""
	lastUpdated := time.Now()

	mfw.mutex.Lock()
	for p, tw2 := range mfw.writers {
		if tw2.getTs().Before(lastUpdated) {
			lastUpdated = tw2.getTs()
			path = p
		}
	}
	mfw.mutex.Unlock()

	mfw.ClosePath(path)
	return
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

	wg := sync.WaitGroup{}
	for path := range writercopy {
		wg.Add(1)
		go func(p string) {
			if err := mfw.ClosePath(p); err != nil { // && err != eioutil.ErrAlreadyClosed { // since we don't want to have a mutex here, chances are that ErrAlreadyClosed can happen
				mfw.mutex.Lock()
				allErrors = multierror.Append(allErrors, err)
				mfw.mutex.Unlock()

			}
			wg.Done()
		}(path)

	}
	wg.Wait()
	return allErrors.ErrorOrNil()
}

// RequiresNewWriter checks if the record will fit withing an existing partition
func (mfw *Cache) RequiresNewWriter(path string) bool {
	mfw.mutex.Lock()
	defer mfw.mutex.Unlock()

	_, ok := mfw.writers[path]
	return !ok
}

// FreeWriterbuffers checks how many more writers can be opened within the current limit
func (mfw *Cache) FreeWriterbuffers() int {
	if mfw.maxCacheEntires <= 0 {
		return 1
	}

	mfw.mutex.Lock()
	defer mfw.mutex.Unlock()

	return mfw.maxCacheEntires - len(mfw.writers)
}

// ClosePath closes one specific path; will return nil if the path doesn't exist or is already closed; Otherwise the reuslt of the writer Close function.
func (mfw *Cache) ClosePath(path string) error {
	//log.Println("Closing cache path :" + path)
	mfw.mutex.Lock()
	writer, ok := mfw.writers[path]
	if !ok {
		mfw.mutex.Unlock()
		return nil
	}
	mfw.mutex.Unlock()

	return writer.Close()
}

func (mfw *Cache) getWriter(path string) (*timedWriter, error) {
	// Let's start with a lightweight read lock
	mfw.mutex.RLock()

	writer, ok := mfw.writers[path]
	defer writer.updateTs()
	if ok {
		mfw.mutex.RUnlock()
		return writer, nil
	}
	mfw.mutex.RUnlock()

	// OK let's grab a write lock
	mfw.mutex.Lock()
	defer mfw.mutex.Unlock()
	// Double check that a writer hasn't been created after we released (and regrabbed) the lock
	writer, ok = mfw.writers[path]
	if ok {
		return writer, nil
	}

	// Create new writer and save for later
	w, err := mfw.writerFactory(path)
	if err != nil {
		return nil, err
	}

	writer = &timedWriter{WriteCloser: w, lastAccess: time.Now()}
	mfw.writers[path] = writer
	mfw.writersCreated++

	return writer, nil
}

// GetWriter returns a synced write closer. The method is a WriterFactory
func (mfw *Cache) GetWriter(path string) (eioutil.WriteCloser, error) {
	return mfw.getWriter(path)
}

// Write gets an existing writers for the path or creates one and saves it for later re-use. If the path contains {suffix}
// it will be replaced by a unique counter + timestamp.
func (mfw *Cache) Write(path string, p []byte) (int, error) {
	//log.Println("Cache.Write @ path:" + path)
	if err := mfw.ctx.Err(); err != nil {
		return 0, err
	}

	writer, err := mfw.getWriter(path)
	if err != nil {
		return 0, err
	}

	if n, err := writer.Write(p); err == nil {
		return n, nil
	} else if err == eioutil.ErrAlreadyClosed { // Make once race condition less likely
		return mfw.Write(path, p)
	} else {
		return n, err
	}
}
