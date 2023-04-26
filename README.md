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


## concurrent

WIP - DO NOT USE.

## eioutil

## fdbtuple

## gzip

## iterator

## keepalive

## keyvaluelist

## recordbuffer

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
