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

	baseSleep time.Duration
}

// WithMinBackoff returns a new RandExpBackoff with the minimum backof time set to minBackoff; this backoff will always be added to the sleep duration
func (bo *RandExpBackoff) WithMinBackoff(minBackoff time.Duration) *RandExpBackoff {
	if bo == nil {
		bo = &RandExpBackoff{
			scale:       1,
			maxAttempts: 100,
		}
	}
	bo.baseSleep = minBackoff
	return bo
}

// WithStartAttmpt returns a new RandExpBackoff with the attempt number set ot attempt
func (bo *RandExpBackoff) WithStartAttmpt(attempt int) *RandExpBackoff {
	if bo == nil {
		bo = &RandExpBackoff{
			scale:       1,
			maxAttempts: 100,
		}
	}
	bo.attempt = attempt
	return bo
}

// WithMaxAttempts returns a new RandExpBackoff with the max attempts limit set ot limit
func (bo *RandExpBackoff) WithMaxAttempts(maxAttempts int) *RandExpBackoff {
	if bo == nil {
		bo = &RandExpBackoff{
			scale:       1,
			maxAttempts: 100,
		}
	}
	bo.maxAttempts = maxAttempts
	return bo
}

// WithScale returns a new RandExpBackoff that scales the sleep duration (base + rand(0, 2^attempts -1) with scaleFator.
func (bo *RandExpBackoff) WithScale(scaleFator float64) *RandExpBackoff {
	if bo == nil {
		bo = &RandExpBackoff{
			scale:       scaleFator,
			maxAttempts: 100,
		}
	}
	return bo
}

func (bo *RandExpBackoff) getBackoffDuration() time.Duration {
	rSrc := rand.New(rand.NewSource(time.Now().Unix()))
	r := int(rSrc.Float64()*math.Pow(2, float64(bo.attempt+1))*bo.scale) - 1 // Random number in [0, 2^n-1]

	return (time.Second*time.Duration(r) + bo.baseSleep)
}

// SleepAndIncr will sleep according to the ccurrent backoff timer and incremnt the attempt counter.
// if attempt > maxAttempts SleepAndIncr will immediately return ErrTooManyAttempts, this is the only error it will return
func (bo *RandExpBackoff) SleepAndIncr() (*RandExpBackoff, error) {
	if bo == nil {
		bo = &RandExpBackoff{
			scale:       1,
			maxAttempts: 100,
		}
	}

	if bo.attempt > bo.maxAttempts {
		return bo, ErrTooManyAttempts
	}

	time.Sleep(bo.getBackoffDuration())
	bo.attempt++

	return bo, nil
}
