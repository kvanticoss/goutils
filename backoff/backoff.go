package backoff

import (
	"errors"
	"math"
	"math/rand"
	"time"
)

// ErrTooManyAttempts is returned if attempt > maxAttempts
var ErrTooManyAttempts = errors.New("too many attempts with failed backoffs")

// RandExpBackoff implement random exponentioal backoff logic for network requests
type RandExpBackoff struct {
	attempt     int
	limit       int
	maxAttempts int
	scale       float64

	minBackoff time.Duration
}

func New() *RandExpBackoff {
	return (&RandExpBackoff{}).getOrCreateWithDefaults() // Ensure nil cases are graceful
}

// Attempt returns the current attempt number
func (bo *RandExpBackoff) Attempt() int {
	bo = bo.getOrCreateWithDefaults() // Ensure nil cases are graceful
	return bo.attempt
}

// WithMinBackoff returns a new RandExpBackoff with the minimum backof time set to minBackoff; this backoff will always be added to the sleep duration
func (bo *RandExpBackoff) WithMinBackoff(minBackoff time.Duration) *RandExpBackoff {
	bo = bo.getOrCreateWithDefaults() // Ensure nil cases are graceful
	bo.minBackoff = minBackoff
	return bo
}

// WithStartAttmpt returns a new RandExpBackoff with the attempt number set ot attempt
func (bo *RandExpBackoff) WithStartAttempt(attempt int) *RandExpBackoff {
	bo = bo.getOrCreateWithDefaults() // Ensure nil cases are graceful
	bo.attempt = attempt
	return bo
}

// WithMaxAttempts returns a new RandExpBackoff with the max attempts limit set ot limit
func (bo *RandExpBackoff) WithMaxAttempts(maxAttempts int) *RandExpBackoff {
	bo = bo.getOrCreateWithDefaults() // Ensure nil cases are graceful
	bo.maxAttempts = maxAttempts
	return bo
}

// WithScale returns a new RandExpBackoff that scales the sleep duration (base + rand(0, 2^attempts -1) with scaleFator.
func (bo *RandExpBackoff) WithScale(scaleFator float64) *RandExpBackoff {
	bo = bo.getOrCreateWithDefaults() // Ensure nil cases are graceful
	bo.scale = scaleFator
	return bo
}

// SleepAndIncr will sleep according to the ccurrent backoff timer and incremnt the attempt counter.
// if attempt > maxAttempts SleepAndIncr will immediately return ErrTooManyAttempts, this is the only error it will return
func (bo *RandExpBackoff) SleepAndIncr() (*RandExpBackoff, error) {
	bo = bo.getOrCreateWithDefaults() // Ensure nil cases are graceful
	if bo.attempt > bo.maxAttempts {
		return bo, ErrTooManyAttempts
	}

	time.Sleep(bo.getBackoffDuration())
	bo.attempt++

	return bo, nil
}

func (bo *RandExpBackoff) getBackoffDuration() time.Duration {
	rSrc := rand.New(rand.NewSource(time.Now().UnixNano()))
	r := int(rSrc.Float64()*math.Pow(2, float64(bo.attempt+1))*bo.scale) - 1 // Random number in [0, 2^n * scale-1]

	return (time.Duration(r)*bo.minBackoff + bo.minBackoff)
}

func (bo *RandExpBackoff) getOrCreateWithDefaults() *RandExpBackoff {
	if bo != nil {
		return bo
	}

	return &RandExpBackoff{
		scale:       1,
		maxAttempts: 100,
	}
}
