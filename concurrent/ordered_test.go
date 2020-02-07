package concurrent

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type testRecord struct {
	i         int
	parse1Res string
	parse2Res string
	parse3Res string
}

func getGerator(records int) chan interface{} {
	input := make(chan interface{})
	go func() {
		for i := 0; i < records; i++ {
			input <- testRecord{
				i: i,
			}
		}
		close(input)
	}()
	return input
}

func TestNewOrderedProcessor(t *testing.T) {
	ctx := context.TODO()

	workers := 2
	records := 1000

	out := NewOrderedProcessor(
		ctx, workers, getGerator(records), ErrorsAbort,
		func(ctx context.Context, input interface{}) (interface{}, error) {
			r := input.(testRecord)
			r.parse1Res = fmt.Sprintf("01 - %d", r.i)
			return r, nil
		},
	)

	prev := testRecord{
		i: -1,
	}
	for r := range out {
		tr := r.Res.(testRecord)
		assert.Nil(t, r.Err)
		assert.Greater(t, tr.i, prev.i)
		assert.Equal(t, tr.parse1Res, fmt.Sprintf("01 - %d", r.Index))
		assert.Equal(t, tr.parse2Res, "")
		assert.Equal(t, tr.parse2Res, "")
		prev = tr
	}

	assert.Equal(t, records-1, prev.i)
}

func TestNewOrderedProcessors(t *testing.T) {
	ctx := context.TODO()

	workers := 2
	records := 100

	out := NewOrderedProcessors(
		ctx, workers, getGerator(records), ErrorsAbort,
		func(ctx context.Context, input interface{}) (interface{}, error) {
			r := input.(testRecord)
			r.parse1Res = fmt.Sprintf("01 - %d", r.i)
			return r, nil
		},
		func(ctx context.Context, input interface{}) (interface{}, error) {
			r := input.(testRecord)
			assert.Equal(t, r.parse1Res, fmt.Sprintf("01 - %d", r.i))
			r.parse2Res = fmt.Sprintf("02 - %d", r.i)
			return r, nil
		},
	)

	prev := testRecord{
		i: -1,
	}
	for r := range out {
		tr := r.Res.(testRecord)
		assert.Nil(t, r.Err)
		assert.Greater(t, tr.i, prev.i)

		assert.Equal(t, tr.i, r.Index)
		assert.Equal(t, tr.parse1Res, fmt.Sprintf("01 - %d", r.Index))
		assert.Equal(t, tr.parse2Res, fmt.Sprintf("02 - %d", r.Index))
		assert.Equal(t, tr.parse3Res, "")
		prev = tr
	}

	assert.Equal(t, records-1, prev.i)
}

func TestNewOrderedProcessorsErrorsDrop(t *testing.T) {
	ctx := context.TODO()

	rand.Seed(86)

	workers := 10
	records := 100

	out := NewOrderedProcessors(
		ctx, workers, getGerator(records),
		ErrorsDrop,
		func(ctx context.Context, input interface{}) (interface{}, error) {
			r := input.(testRecord)
			time.Sleep(time.Microsecond * time.Duration(rand.Intn(100)))
			if r.i > 50 {
				return nil, fmt.Errorf("virtual error")
			}
			return r, nil
		},
		func(ctx context.Context, input interface{}) (interface{}, error) {
			r := input.(testRecord)
			assert.LessOrEqual(t, r.i, 50) // no records after errors should be emitted
			return r, nil
		},
	)

	errorsFound := 0
	for o := range out {
		if o.Err != nil {
			errorsFound++
		}
		if o.Index > 50 {
			assert.NotNil(t, o.Err)
		} else {
			assert.Nil(t, o.Err)
		}
	}
	assert.Equal(t, 0, errorsFound)
}

func TestNewOrderedProcessorsErrorsIgnore(t *testing.T) {
	ctx := context.TODO()

	rand.Seed(86)

	workers := 10
	records := 100

	out := NewOrderedProcessors(
		ctx, workers, getGerator(records),
		ErrorsIgnore,
		func(ctx context.Context, input interface{}) (interface{}, error) {
			r := input.(testRecord)
			time.Sleep(time.Microsecond * time.Duration(rand.Intn(100)))
			if r.i == 50 {
				return nil, fmt.Errorf("virtual error")
			}
			return r, nil
		},
		func(ctx context.Context, input interface{}) (interface{}, error) {
			r := input.(testRecord)
			assert.NotEqual(t, r.i, 50) // position 50 should have been filtered
			return r, nil
		},
	)

	errorsFound := 0
	recordsFound := 0
	for o := range out {
		recordsFound++
		if o.Err != nil {
			errorsFound++
		}
		if o.Index != 50 {
			assert.Nil(t, o.Err)
		} else {
			assert.NotNil(t, o.Err)
		}
	}
	assert.Equal(t, records, recordsFound) // 100 good, 1 bad
	assert.Equal(t, 1, errorsFound)
}

func TestNewOrderedProcessorsCtxCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.TODO())

	workers := 5
	records := 10
	out := NewOrderedProcessors(
		ctx, workers, getGerator(records),
		ErrorsIgnore,
		func(ctx context.Context, input interface{}) (interface{}, error) {
			select {
			case <-ctx.Done():
			case <-time.After(time.Second):
				t.Error("Timeout should'nt happend, ctx should handle it")
			}
			return input, nil
		},
	)

	cancel()
	var lastErr error
	for o := range out {
		if o.Err != nil {
			lastErr = o.Err
		}
	}
	assert.Equal(t, lastErr, ctx.Err())
}

func TestNewOrderedProcessorsErrorsAbort(t *testing.T) {
	ctx := context.TODO()

	rand.Seed(86)

	workers := 10
	records := 100

	out := NewOrderedProcessors(
		ctx, workers, getGerator(records),
		ErrorsAbort,
		func(ctx context.Context, input interface{}) (interface{}, error) {
			r := input.(testRecord)
			time.Sleep(time.Microsecond * time.Duration(rand.Intn(100)))
			if r.i == 50 {
				return nil, fmt.Errorf("virtual error")
			}
			return r, nil
		},
		func(ctx context.Context, input interface{}) (interface{}, error) {
			r := input.(testRecord)
			assert.LessOrEqual(t, r.i, 50) // no records after errors should be emitted
			return r, nil
		},
	)

	errorsFound := 0
	recordsFound := 0
	for o := range out {
		recordsFound++
		if o.Err != nil {
			errorsFound++
		}
		if o.Index != 50 {
			assert.Nil(t, o.Err)
		} else {
			assert.NotNil(t, o.Err)
		}
	}
	assert.Equal(t, 51, recordsFound) // 50 good, 1 bad
	assert.Equal(t, 1, errorsFound)
}

func ExampleOrderedProcessors() {

	src := make(chan interface{})
	go func() {
		for i := 0; i < 10; i++ {
			src <- i
		}
		close(src) //important!
	}()

	start := time.Now()
	workers := 10
	out := NewOrderedProcessors(context.TODO(), workers, src, ErrorsAbort,
		func(ctx context.Context, input interface{}) (interface{}, error) {
			time.Sleep(time.Duration(10-input.(int)) * time.Millisecond)
			return input.(int) * 2, nil
		},
	)

	res := []int{}
	for i := range out {
		res = append(res, i.Res.(int))
	}

	// if this would have been evaluated sequencially the process would have taken
	// sum(1..10) miliseconds and if order were defined by completion time should
	// the result would be in reverse order (18, 16... 0). Neither is the case.
	// only the longest jobtime defines the total time.
	duration := time.Now().Sub(start)
	fmt.Printf("%v in %s", res, duration.Truncate(10*time.Millisecond))
	// Output: [0 2 4 6 8 10 12 14 16 18] in 10ms
}

func BenchmarkNewOrderedProcessors(b *testing.B) {
	ctx := context.TODO()

	tests := []struct {
		workers, records int
	}{
		{workers: 2, records: 100},    // Theoretically ~5 seconds
		{workers: 10, records: 100},   // Theoretically ~1 seconds
		{workers: 50, records: 100},   // Theoretically ~0.2 seconds
		{workers: 50, records: 1000},  // Theoretically ~2 seconds
		{workers: 100, records: 100},  // Theoretically ~0.1 seconds
		{workers: 100, records: 1000}, // Theoretically ~1 seconds
	}

	for _, test := range tests {
		b.Run(
			fmt.Sprintf("bench_%d_workers_%d_records", test.workers, test.records),
			func(workers, records int) func(b *testing.B) {
				return func(b *testing.B) {
					for i := 0; i < b.N; i++ {
						out := NewOrderedProcessors(
							ctx, workers, getGerator(records), ErrorsAbort,
							func(ctx context.Context, input interface{}) (interface{}, error) {
								time.Sleep(100 * time.Millisecond)
								return input, nil
							},
						)
						for _ = range out {
						}
					}

				}
			}(test.workers, test.records))
	}
}
