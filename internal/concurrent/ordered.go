package concurrent

import (
	"context"
	"sync"
)

// Processor defines a function which takes an input and yields an  output + error tuple
type Processor func(ctx context.Context, intput interface{}) (interface{}, error)

// ErrorStrategy contains various methods for responding to errors during a concurrent execution
type ErrorStrategy int

const (
	// ErrorsIgnore strategy will ignore errors from the processor and continue processing
	// input records until the input chan is closed or context is cancelled. Errors will be
	// forwarded to the error chan
	ErrorsIgnore ErrorStrategy = iota + 1

	// ErrorsAbort strategy will abort all processing after all successfull entires
	// ahead of the error causing entry has been flushed to the chan.
	ErrorsAbort

	// ErrorsDrop strategy will ignore errors from the processor and continue processing
	// input records until the input chan is closed or context is cancelled. Errors will be
	// NOT forwarded to the error chan but dropped
	ErrorsDrop
)

type StreamOutput struct {
	// Index is the index (starting at 0) counting from the source input.
	Index int

	// order is used internally for guarranteeing record order. is not guarranteed to
	// be the same as Index if there are multiple processors and ErrorsDrop is used.
	order int

	// Res holds the last return value of the last processor, or the value of the first
	// processor which returned a non nil error.
	Res interface{}

	// Error of the first processor which returned an non nil error
	Err error
}

// NewOrderedProcessor will read from input and run all the processors, in order,
// on top of the input; where possible in parrallell with upp to [workers] of parrallell
// threads and emit the output in preserved order.
// Is similar to an concurrent ordered .map() call in other languages.
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
// on top of the input; where possible in parrallell with upp to [workers] of parrallell
// threads and emit the output in preserved order.
// Is similar to an ordered concurrent .map() call in other languages.
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

	inputChanWithIndex = resetOrder(inputChanWithIndex)

	// Prepare our token ring pool for sync
	tokenRing := map[int]chan bool{}
	for i := 0; i < workers; i++ {
		tokenRing[i] = make(chan bool, 1)
	}
	// Place the initial relay token
	tokenRing[0] <- true

	outputCh := make(chan StreamOutput, 1)
	putOnQueueInOrder := func(item StreamOutput) {
		//Wait for our token
		select {
		case <-tokenRing[item.order%workers]: // maps are safe for concurrent reads
		case <-ctx.Done():
			outputCh <- StreamOutput{
				Err: ctx.Err(),
			}
			return
		}

		// Only case when we doesn't return the value is if it is an error and we want to drop it
		if item.Err == nil || errorStrategy != ErrorsDrop {
			outputCh <- item
		}

		// Hand the token over to next worker
		select {
		case tokenRing[(item.order+1)%workers] <- true:
		case <-ctx.Done():
			select {
			case outputCh <- StreamOutput{Err: ctx.Err()}:
			default:
			}
		}

		// But it there was an abort strategy, stop everything else.
		if item.Err != nil && errorStrategy == ErrorsAbort {
			cancel()
		}
	}

	wg := sync.WaitGroup{}
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					select {
					case outputCh <- StreamOutput{Err: ctx.Err()}:
					default:
					}
					return
				case queuedItem, ok := <-inputChanWithIndex:
					if !ok {
						return
					}
					if queuedItem.Err == nil {
						queuedItem.Res, queuedItem.Err = ps(ctx, queuedItem.Res)
					}
					putOnQueueInOrder(queuedItem)
				}
			}
		}()
	}

	// Cleanup channels
	go func() {
		wg.Wait()
		cancel()
		close(outputCh)

		for i := 0; i < workers; i++ {
			close(tokenRing[i])
			for _ = range tokenRing[i] {
			}
		}
	}()

	return maybeRemoveRecAfterFirstError(errorStrategy, outputCh)
}

// NewOrderedProcessors works like NewOrderedProcessor but where each processor have their
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

func resetOrder(in chan StreamOutput) chan StreamOutput {
	inputChanWithIndexCleandOrder := make(chan StreamOutput)
	go func() {
		order := 0
		for input := range in {
			inputChanWithIndexCleandOrder <- StreamOutput{
				order: order,

				Index: input.Index,
				Res:   input.Res,
				Err:   input.Err,
			}
			order++
		}
		close(inputChanWithIndexCleandOrder)
	}()
	return inputChanWithIndexCleandOrder
}

func maybeRemoveRecAfterFirstError(errorStrategy ErrorStrategy, in chan StreamOutput) chan StreamOutput {
	outputChCleared := make(chan StreamOutput)
	go func() {
		var errorFound bool
		for r := range in {
			if errorFound {
				continue
			}
			if r.Err != nil && errorStrategy != ErrorsIgnore {
				errorFound = true
			}
			outputChCleared <- r
		}
		close(outputChCleared)
	}()
	return outputChCleared
}

func interfaceChanToStreamOutputChan(input chan interface{}) chan StreamOutput {
	// Decorate our input stream with the orderid they are received in
	inputChanWithIndex := make(chan StreamOutput)
	go func() {
		index := 0
		for i := range input {
			inputChanWithIndex <- StreamOutput{
				Index: index,
				Res:   i,
				Err:   nil,
			}
			index++
		}
		close(inputChanWithIndex)
	}()

	return inputChanWithIndex
}
