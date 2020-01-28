package concurrent

import (
	"context"
	"sync"
)

// Processor defines a function which takes an input and yeilds an  output + error tuple
type Processor func(ctx context.Context, intput interface{}) (interface{}, error)

// ErrorStrategy contains various methods for respoinding to errors during a concurrent execution
type ErrorStrategy int

const (
	// ErrorsIgnore strategy will ignore errors from the processor and continue processing
	// input records until the input chan is closed or context is cancelled. Errors will be
	// forwarded to the error chan
	ErrorsIgnore ErrorStrategy = iota + 1

	// ErrorsAbortAll strategy will abort all processing directly on the first error
	ErrorsAbortAll

	// ErrorsAbort strategy will abort all processing after all successfull entires
	// ahead of the error causing entry has been flushed to the chan.
	ErrorsAbort

	// ErrorsDrop strategy will ignore errors from the processor and continue processing
	// input records until the input chan is closed or context is cancelled. Errors will be
	// NOT forwarded to the error chan but dropped
	ErrorsDrop
)

type StreamOutput struct {
	index int
	res   interface{}
	err   error
}

// NewOrderedProcessor will read from input and run all the processors, in order,
// on top of the input; where possible in parralle with upp to [workers] of parrallel
// threads and emiit the output in preserved order.
// Is similar to an concurrent ordered .map() call in other lanugages.
// Designed for unbounded streams of data. The consumer is responsible to read out
// all items from output + errors chan to not end up with memory leaks
// Since the implementation makes heavy use of channels it is NOT meant for high throughput
// scenarios but rather when some processing in a pipeline can be done in parallel but order
// is still required.
func NewOrderedProcessor(
	ctx context.Context,
	workers int,
	input chan interface{},
	errorStrategy ErrorStrategy,
	ps Processor,
) (
	output chan StreamOutput,
) {
	return newOrderedProcessor(ctx, workers, interfaceChanToStreamOutputChan(input), errorStrategy, ps)
}

// NewOrderedProcessor will read from input and run all the processors, in order,
// on top of the input; where possible in parralle with upp to [workers] of parrallel
// threads and emiit the output in preserved order.
// Is similar to an ordered concurrent .map() call in other lanugages.
// Designed for unbounded streams of data. The consumer is responsible to read out
// all items from output + errors chan to not end up with memory leaks
func newOrderedProcessor(
	ctx context.Context,
	workers int,
	inputChanWithIndex chan StreamOutput,
	errorStrategy ErrorStrategy,
	ps Processor,
) (
	output chan StreamOutput,
) {
	ctx, cancel := context.WithCancel(ctx)

	// Prepare our syncer pool
	syncers := map[int]chan bool{}
	for i := 0; i < workers; i++ {
		syncers[i] = make(chan bool, 1)
	}
	// Place the inital relay token
	syncers[0] <- true

	outputCh := make(chan StreamOutput)

	wg := sync.WaitGroup{}
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(workerId int) {
			defer wg.Done()
			for {

				putOnQueueInOrder := func(index int, out *StreamOutput) {
					select {
					case <-syncers[index%workers]:
					case <-ctx.Done():
						outputCh <- StreamOutput{
							err: ctx.Err(),
						}
						return
					}

					if out != nil {
						outputCh <- *out
					}

					select {
					case syncers[(index+1)%workers] <- true:
					case <-ctx.Done():
					}
				}

				select {
				case <-ctx.Done():
					return
				case queuedItem, ok := <-inputChanWithIndex:
					if !ok {
						return
					}
					var out StreamOutput // Todo, maybe reuse queuedItem

					if queuedItem.err != nil {
						out = queuedItem
					} else {
						out.res, out.err = ps(ctx, queuedItem.res)
						out.index = queuedItem.index
					}

					if out.err == nil {
						putOnQueueInOrder(queuedItem.index, &out)
					} else {
						switch errorStrategy {
						case ErrorsDrop:
							putOnQueueInOrder(queuedItem.index, nil)
						case ErrorsIgnore:
							putOnQueueInOrder(queuedItem.index, &out)
						case ErrorsAbort:
							putOnQueueInOrder(queuedItem.index, &out)
							cancel()
							return
						}
					}
				}
			}
		}(i)
	}

	// filter out records that are emitted after first error
	outputChCleared := make(chan StreamOutput)
	go func() {
		var errorFound bool
		for r := range outputCh {
			if errorFound {
				continue
			}
			if r.err != nil && errorStrategy != ErrorsIgnore {
				errorFound = true
			}
			outputChCleared <- r
		}
		close(outputChCleared)
	}()

	// Cleanup channels
	go func() {
		wg.Wait()
		cancel()
		close(outputCh)

		for i := 0; i < workers; i++ {
			close(syncers[i])
			for _ = range syncers[i] {
			}
		}
		// Todo: Maybe write 1 contextDone error
	}()

	return outputChCleared
}

// NewOrderedProcessors works like NewOrderedProcessor but where each processor have thier
// own work queue and thread pools, the next reading from the previous.
func NewOrderedProcessors(
	ctx context.Context,
	workers int,
	input chan interface{},
	errorStrategy ErrorStrategy,
	ps ...Processor,
) (
	output chan StreamOutput,
) {
	output = interfaceChanToStreamOutputChan(input)
	for _, p := range ps {
		output = newOrderedProcessor(ctx, workers, output, errorStrategy, p)
	}
	return output
}

func interfaceChanToStreamOutputChan(input chan interface{}) chan StreamOutput {
	// Decorate our input stream with the orderid they are recieved in
	inputChanWithIndex := make(chan StreamOutput)
	go func() {
		index := 0
		for i := range input {
			inputChanWithIndex <- StreamOutput{
				index: index,
				res:   i,
				err:   nil,
			}
			index++
		}
		close(inputChanWithIndex)
	}()

	return inputChanWithIndex
}
