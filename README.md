# goutils
Various, commonly reused, go helpers and patterns; WIP

## backoff

Provides simple backoff mechanics on the basis of `max(minDuration, minDuration*rand(0,1)*2^(attempt+1)*scaling)`

Example usage
```golang

bo := (&backoff.RandExpBackoff{}).WithMinBackoff(time.Second*5).WithMaxAttempts(10)
for someActionErr := someAction(); someActionErr != nil; someActionErr = someAction() {
  if bo, err := bo.SleepAndIncr(); err != backoff.ErrTooManyAttempts {
    return fmt.Errorf("someAction(): giving up after %d attempts: %w", bo.Attempt(), someActionErr)
  }
}

```

The pattern above where bo is re-initialized from `SleepAndIncr()` is advised as it gracefully
handles null instances of &RandExpBackoff{} using sane defaults.


## eioutil

extended ioutil for handling streams.

`NewReadCloser` - Add a custom close hook on a reader.
```golang
readCloser := eioutil.NewReadCloser(r, func() error {
  // custom close hook on a reader.
})

```

`NewWriteCloser` - Add a custom close hook on a Writer. `NewWriteNOPCloser(w)` adds an empty close hook.
```golang
writeCloser := eioutil.NewWriteCloser(w, func() error {
  // custom close hook on the writer.
  // useful to in the case of gzip writers where two writers have to be cancelled but you might only
  // be able to return 1 WriteCloser
})

```

`NewSyncedWriteCloser(writeCloser io.WriteCloser)` - Make a WriteCloser safe for concurrent writes.
each write will be mutex protected but it is up to the consumer to ensure that atomic writes can be
interlaced.

```golang
mutexWriteCloser := eioutil.NewWriteCloser(writeCloser)
```

`NewPreWriteCallback(w, callbacks ...func([]byte) error)` - Pre write Hook

`NewPostWriteCallback(w, callbacks ...func([]byte) error)` - Post write hook

`NewPrePostWriteCallback(w, callbacks ...func([]byte) error)` - Pre and Post write hook

Spins up a new io.MulitiWriter where the callbacks are invoked before the underlying writer. Errors in the callback will block/stop writes to the underlying writer.

Can be use to block/delay writes as well as conditionally abort writes.

Example with limiting the number of bytes written.
```golang
limit := 1000
written := 0
limitWriter := eioutil.NewPreWriteCallback(writeCloser, func(b []byte) error {
  size := len(b)
  if size + writte > limit {
    return fmt.Error("too large; won't write")
  }
  return nil
})
```

Example with limiting the number of bytes written per second.
```golang
bps := 1024 // 1 kb/s
limiter := rate.NewLimiter(rate.Limit(bps), bps) // "golang.org/x/time/rate"

limitWriter := eioutil.NewPreWriteCallback(writeCloser, func(b []byte) error {
  limiter.WaitN(context.TODO(), len(b)); err != nil {
		return n, err
	}
  return nil
})
```


`NewWriterCloserWithSelfDestructAfterMaxBytes(w, maxBytes)` - Create a writer which
after maxBytes automatically closes the Writer after which new Write()-calls will
return `ErrAlreadyClosed`


`NewWriterCloserWithSelfDestructAfterIdle(ctx, maxIdle, writeCloser)` - Create a writer which
after after some inactivity automatically closes the writer. after which, new Write()-calls will
return `ErrAlreadyClosed`. A timer is reset on each write before a Write()-call. Any Close()
call is blocked during the write.

## fdbtuple

## gzip

## iterator

Utilities around iterators

`type RecordIterator func() (interface{}, error)` - base type iterator

`ErrIteratorStop = errors.New("iterator stop")` - error returned after last record has been emitted.

`func CombineIterators(iterators ...RecordIterator) RecordIterator`


`func NewRecordPipe() (RecordWriter, RecordIterator)` - Multithreaded iterators using channels under the hood.


`func JSONRecordIterator(new func() interface{}, r io.Reader) RecordIterator` - Get an iterator from a reader pointing to an json array or new line delimited json.


### Lesser iterators

Some more complex operations can be performed if we can compare two records. Comparing can be done if the records implement the Lesser interface

```
type Lesser interface {
	Less(other interface{}) bool
}

```



## keepalive

Setup a liveness check that cancels once if no "ping" has been observed for a given period of time.

```golang

ka := keepalive.New(ctx, time.Second*30, true, func(){
  log.Printf("30 seconds passed since last ka.Ping(); or ctx.Done()" )
})
time.Sleep(time.Second*20)
ka.Ping()
time.Sleep(time.Second*20)
ka.Ping()
time.Sleep(time.Second*20)
ka.Ping()
time.Sleep(time.Second*20)
ka.Ping()

ka.Close() // terminates the keep alive an calls callbacks.

```

## keyvaluelist

an ordered key-value set. Useful to build hive-style partition filters (e.g. `/date=2023-01-01/age=54`)

`NewKeyValuesFromPath("foo=bar/key=value) => []KeyValues`

`kvl := []KeyValues{{"foo","bar"},{"key","value"}}`

`kvl.ToPartitionKey() => "foo=bar/key=value"`

`kvl.AsMap() => map[string]string{"foo":"bar", "key":"value"}`


## recordwriter

## writercache

## writerfactory

Abstraction for creating names writers; used to create writes under specific paths.

```golang
// WriterFactory should yield a new WriteCloser under the given path.
type WriterFactory func(path string) (wc eioutil.WriteCloser, err error)


wf := writerfactory.GetLocalWriterFactory("/tmp/")
//mapOfBuffers, wf := writerfactory.GetMemoryWriterFactory()

wf = wf.WithPrefix("PREFIX").WithGzip()
...

writer, err := wf("foo/bar.txt.gz") // creates a path and opens a file @ /tmp/PREFIX/foo/bar.txt.gz
writer
if err != nil {
  return err
}
_, err = writer.Write("hello world") // writes to /tmp/PREFIX/foo/bar.txt.gz
if err != nil {¨
  return err
}
err = writer.Close()
if err != nil {
  return err
}

```


# Highly Experimental

## recordbuffer

Store records in any byte stream.


## writercache

Keeps tracks of multiple writer factories and closes them based on timeouts or LRU.

## concurrent

WIP/Playground code - DO NOT USE.

Concurrent ordered execution of jobs in one or more steps. Similar to concurrent map in functional languages
