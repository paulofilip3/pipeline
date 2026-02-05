// Package pipeline provides a lightweight, generic, concurrent pipeline library for Go.
//
// A pipeline chains a series of stages, each of which processes items using a
// [WorkerFunc]. Stages run concurrently and communicate through channels,
// providing natural backpressure without internal buffering.
//
// Two shutdown modes are supported:
//
//   - Drain mode: close the input channel and every stage will finish processing
//     remaining items, then close its output channel, cascading through the pipeline.
//   - Daemon mode: cancel the context and all goroutines exit promptly.
//
// Callers must drain both the output and error channels returned by [Pipeline.Run].
package pipeline

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// WorkerFunc processes a single item. It is called once per item by each stage.
// Returning a non-nil error sends the error to the pipeline's error channel and
// drops the item. The context is cancelled when the pipeline shuts down.
type WorkerFunc[T any] func(ctx context.Context, item T) (T, error)

// Stage describes a named processing step in a pipeline.
type Stage[T any] struct {
	// Name identifies the stage. Must be non-empty and unique within a pipeline.
	Name string
	// Worker is the function that processes each item.
	Worker WorkerFunc[T]
	// Concurrency is the number of goroutines that run Worker in parallel.
	// Values <= 0 are normalized to 1.
	Concurrency int
}

// Pipeline is a chain of stages that process items of type T.
type Pipeline[T any] struct {
	stages []Stage[T]
}

// New creates a pipeline with the given stages. It validates that there is at
// least one stage, all stages have non-empty unique names, and all workers are
// non-nil. Concurrency values <= 0 are normalized to 1. New never panics;
// invalid input is reported via the returned error.
func New[T any](stages ...Stage[T]) (*Pipeline[T], error) {
	if len(stages) == 0 {
		return nil, errors.New("pipeline: at least one stage is required")
	}

	seen := make(map[string]struct{}, len(stages))
	normalized := make([]Stage[T], len(stages))

	for i, s := range stages {
		if s.Name == "" {
			return nil, fmt.Errorf("pipeline: stage %d has an empty name", i)
		}
		if s.Worker == nil {
			return nil, fmt.Errorf("pipeline: stage %q has a nil worker", s.Name)
		}
		if _, dup := seen[s.Name]; dup {
			return nil, fmt.Errorf("pipeline: duplicate stage name %q", s.Name)
		}
		seen[s.Name] = struct{}{}

		if s.Concurrency <= 0 {
			s.Concurrency = 1
		}
		normalized[i] = s
	}

	return &Pipeline[T]{stages: normalized}, nil
}

// Run starts the pipeline. Items are read from in, processed through each stage
// in order, and sent to the returned output channel. Errors from any stage are
// sent to the returned error channel.
//
// Both returned channels are closed when the pipeline finishes (either because
// in was closed and all items have been processed, or because ctx was cancelled).
//
// The caller must drain both returned channels to avoid goroutine leaks. A
// common pattern for discarding errors is:
//
//	go func() { for range errs {} }()
func (p *Pipeline[T]) Run(ctx context.Context, in <-chan T) (out <-chan T, errs <-chan error) {
	errChans := make([]<-chan error, len(p.stages))

	ch := in
	for i, s := range p.stages {
		var errCh <-chan error
		ch, errCh = runStage(ctx, s, ch)
		errChans[i] = errCh
	}

	return ch, Merge(ctx, errChans...)
}

// runStage launches a single stage. It returns the stage's output channel and
// an error channel. Both are closed when the stage completes.
func runStage[T any](ctx context.Context, s Stage[T], in <-chan T) (<-chan T, <-chan error) {
	out := make(chan T)
	errCh := make(chan error)

	var wg sync.WaitGroup
	wg.Add(s.Concurrency)

	for i := 0; i < s.Concurrency; i++ {
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case item, ok := <-in:
					if !ok {
						return
					}
					result, err := s.Worker(ctx, item)
					if err != nil {
						select {
						case errCh <- err:
						case <-ctx.Done():
							return
						}
						continue
					}
					select {
					case out <- result:
					case <-ctx.Done():
						return
					}
				}
			}
		}()
	}

	// Cleanup goroutine: wait for all workers, then close output channels.
	go func() {
		wg.Wait()
		close(out)
		close(errCh)
	}()

	return out, errCh
}
