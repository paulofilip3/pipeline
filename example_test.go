package pipeline_test

import (
	"context"
	"fmt"
	"sort"

	"github.com/paulofilip3/pipeline"
)

func ExamplePipeline_Run() {
	// Build a two-stage pipeline that increments then doubles each value.
	p, err := pipeline.New(
		pipeline.Stage[int]{
			Name:   "increment",
			Worker: func(_ context.Context, v int) (int, error) { return v + 1, nil },
		},
		pipeline.Stage[int]{
			Name:        "double",
			Worker:      func(_ context.Context, v int) (int, error) { return v * 2, nil },
			Concurrency: 2,
		},
	)
	if err != nil {
		panic(err)
	}

	// Feed input.
	in := make(chan int)
	go func() {
		for _, v := range []int{1, 2, 3} {
			in <- v
		}
		close(in)
	}()

	// Run the pipeline.
	out, errs := p.Run(context.Background(), in)

	// Always drain errors to avoid goroutine leaks.
	go func() { for range errs {} }()

	// Collect results (order may vary with concurrency > 1).
	var results []int
	for v := range out {
		results = append(results, v)
	}
	sort.Ints(results)

	for _, v := range results {
		fmt.Println(v)
	}
	// Output:
	// 4
	// 6
	// 8
}
