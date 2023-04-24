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
