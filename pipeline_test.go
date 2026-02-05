package pipeline_test

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/paulofilip3/pipeline"
)

// --- Construction ---

func TestNew_valid(t *testing.T) {
	p, err := pipeline.New(pipeline.Stage[int]{
		Name:   "double",
		Worker: func(_ context.Context, v int) (int, error) { return v * 2, nil },
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if p == nil {
		t.Fatal("expected non-nil pipeline")
	}
}

func TestNew_multipleStages(t *testing.T) {
	_, err := pipeline.New(
		pipeline.Stage[int]{Name: "a", Worker: func(_ context.Context, v int) (int, error) { return v, nil }},
		pipeline.Stage[int]{Name: "b", Worker: func(_ context.Context, v int) (int, error) { return v, nil }},
		pipeline.Stage[int]{Name: "c", Worker: func(_ context.Context, v int) (int, error) { return v, nil }},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestNew_noStages(t *testing.T) {
	_, err := pipeline.New[int]()
	if err == nil {
		t.Fatal("expected error for no stages")
	}
}

func TestNew_nilWorker(t *testing.T) {
	_, err := pipeline.New(pipeline.Stage[int]{Name: "bad"})
	if err == nil {
		t.Fatal("expected error for nil worker")
	}
}

func TestNew_emptyName(t *testing.T) {
	_, err := pipeline.New(pipeline.Stage[int]{
		Worker: func(_ context.Context, v int) (int, error) { return v, nil },
	})
	if err == nil {
		t.Fatal("expected error for empty name")
	}
}

func TestNew_duplicateNames(t *testing.T) {
	w := func(_ context.Context, v int) (int, error) { return v, nil }
	_, err := pipeline.New(
		pipeline.Stage[int]{Name: "dup", Worker: w},
		pipeline.Stage[int]{Name: "dup", Worker: w},
	)
	if err == nil {
		t.Fatal("expected error for duplicate names")
	}
}

func TestNew_concurrencyDefaults(t *testing.T) {
	// Concurrency <= 0 should be normalized to 1 (not cause error).
	p, err := pipeline.New(pipeline.Stage[int]{
		Name:        "low",
		Worker:      func(_ context.Context, v int) (int, error) { return v, nil },
		Concurrency: -5,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if p == nil {
		t.Fatal("expected non-nil pipeline")
	}
}

// --- Drain mode ---

func TestRun_singleStage(t *testing.T) {
	p, _ := pipeline.New(pipeline.Stage[int]{
		Name:   "double",
		Worker: func(_ context.Context, v int) (int, error) { return v * 2, nil },
	})

	in := make(chan int)
	go func() {
		for _, v := range []int{1, 2, 3} {
			in <- v
		}
		close(in)
	}()

	ctx := context.Background()
	out, errs := p.Run(ctx, in)

	go func() { for range errs {} }()

	var results []int
	for v := range out {
		results = append(results, v)
	}

	sort.Ints(results)
	expected := []int{2, 4, 6}
	if len(results) != len(expected) {
		t.Fatalf("got %v, want %v", results, expected)
	}
	for i := range expected {
		if results[i] != expected[i] {
			t.Fatalf("got %v, want %v", results, expected)
		}
	}
}

func TestRun_multiStageChaining(t *testing.T) {
	p, _ := pipeline.New(
		pipeline.Stage[int]{
			Name:   "add1",
			Worker: func(_ context.Context, v int) (int, error) { return v + 1, nil },
		},
		pipeline.Stage[int]{
			Name:   "double",
			Worker: func(_ context.Context, v int) (int, error) { return v * 2, nil },
		},
	)

	in := make(chan int)
	go func() {
		for _, v := range []int{1, 2, 3} {
			in <- v
		}
		close(in)
	}()

	out, errs := p.Run(context.Background(), in)
	go func() { for range errs {} }()

	var results []int
	for v := range out {
		results = append(results, v)
	}
	sort.Ints(results)

	expected := []int{4, 6, 8} // (1+1)*2, (2+1)*2, (3+1)*2
	if len(results) != len(expected) {
		t.Fatalf("got %v, want %v", results, expected)
	}
	for i := range expected {
		if results[i] != expected[i] {
			t.Fatalf("got %v, want %v", results, expected)
		}
	}
}

func TestRun_emptyInput(t *testing.T) {
	p, _ := pipeline.New(pipeline.Stage[int]{
		Name:   "noop",
		Worker: func(_ context.Context, v int) (int, error) { return v, nil },
	})

	in := make(chan int)
	close(in)

	out, errs := p.Run(context.Background(), in)
	go func() { for range errs {} }()

	count := 0
	for range out {
		count++
	}
	if count != 0 {
		t.Fatalf("expected 0 items, got %d", count)
	}
}

func TestRun_concurrentWorkers(t *testing.T) {
	var mu sync.Mutex
	seen := make(map[int]bool)

	p, _ := pipeline.New(pipeline.Stage[int]{
		Name:        "track",
		Concurrency: 4,
		Worker: func(_ context.Context, v int) (int, error) {
			mu.Lock()
			seen[v] = true
			mu.Unlock()
			return v, nil
		},
	})

	in := make(chan int)
	go func() {
		for i := 0; i < 100; i++ {
			in <- i
		}
		close(in)
	}()

	out, errs := p.Run(context.Background(), in)
	go func() { for range errs {} }()
	for range out {
	}

	mu.Lock()
	defer mu.Unlock()
	if len(seen) != 100 {
		t.Fatalf("expected 100 unique items, got %d", len(seen))
	}
}

func TestRun_orderingWithConcurrency1(t *testing.T) {
	p, _ := pipeline.New(pipeline.Stage[int]{
		Name:        "passthrough",
		Concurrency: 1,
		Worker:      func(_ context.Context, v int) (int, error) { return v, nil },
	})

	in := make(chan int)
	go func() {
		for i := 0; i < 50; i++ {
			in <- i
		}
		close(in)
	}()

	out, errs := p.Run(context.Background(), in)
	go func() { for range errs {} }()

	var results []int
	for v := range out {
		results = append(results, v)
	}

	for i, v := range results {
		if v != i {
			t.Fatalf("order mismatch at index %d: got %d", i, v)
		}
	}
}

// --- Daemon mode ---

func TestRun_contextCancel(t *testing.T) {
	p, _ := pipeline.New(pipeline.Stage[int]{
		Name: "slow",
		Worker: func(ctx context.Context, v int) (int, error) {
			select {
			case <-ctx.Done():
				return 0, ctx.Err()
			case <-time.After(10 * time.Second):
				return v, nil
			}
		},
	})

	in := make(chan int)
	go func() {
		for i := 0; ; i++ {
			select {
			case in <- i:
			case <-time.After(time.Second):
				return
			}
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	out, errs := p.Run(ctx, in)

	// Let it run briefly, then cancel.
	time.Sleep(50 * time.Millisecond)
	cancel()

	// Drain — both channels must close.
	for range out {
	}
	for range errs {
	}
}

func TestRun_alreadyCancelledContext(t *testing.T) {
	p, _ := pipeline.New(pipeline.Stage[int]{
		Name:   "noop",
		Worker: func(_ context.Context, v int) (int, error) { return v, nil },
	})

	in := make(chan int, 5)
	for i := 0; i < 5; i++ {
		in <- i
	}
	close(in)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel before Run

	out, errs := p.Run(ctx, in)
	for range out {
	}
	for range errs {
	}
	// Test passes if channels close without hanging.
}

// --- Error handling ---

func TestRun_partialErrors(t *testing.T) {
	p, _ := pipeline.New(pipeline.Stage[int]{
		Name: "evenOnly",
		Worker: func(_ context.Context, v int) (int, error) {
			if v%2 != 0 {
				return 0, fmt.Errorf("odd: %d", v)
			}
			return v, nil
		},
	})

	in := make(chan int)
	go func() {
		for i := 0; i < 6; i++ {
			in <- i
		}
		close(in)
	}()

	out, errs := p.Run(context.Background(), in)

	var results []int
	var errList []error
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for e := range errs {
			errList = append(errList, e)
		}
	}()

	for v := range out {
		results = append(results, v)
	}
	wg.Wait()

	sort.Ints(results)
	expectedResults := []int{0, 2, 4}
	if len(results) != len(expectedResults) {
		t.Fatalf("results: got %v, want %v", results, expectedResults)
	}
	for i := range expectedResults {
		if results[i] != expectedResults[i] {
			t.Fatalf("results: got %v, want %v", results, expectedResults)
		}
	}

	if len(errList) != 3 {
		t.Fatalf("expected 3 errors, got %d", len(errList))
	}
}

func TestRun_allErrors(t *testing.T) {
	errBoom := errors.New("boom")
	p, _ := pipeline.New(pipeline.Stage[int]{
		Name:   "fail",
		Worker: func(_ context.Context, v int) (int, error) { return 0, errBoom },
	})

	in := make(chan int)
	go func() {
		for i := 0; i < 5; i++ {
			in <- i
		}
		close(in)
	}()

	out, errs := p.Run(context.Background(), in)

	var results []int
	var errCount int
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range errs {
			errCount++
		}
	}()
	for v := range out {
		results = append(results, v)
	}
	wg.Wait()

	if len(results) != 0 {
		t.Fatalf("expected 0 results, got %d", len(results))
	}
	if errCount != 5 {
		t.Fatalf("expected 5 errors, got %d", errCount)
	}
}

func TestRun_errorInMiddleStage(t *testing.T) {
	errMiddle := errors.New("middle failure")
	p, _ := pipeline.New(
		pipeline.Stage[int]{
			Name:   "add1",
			Worker: func(_ context.Context, v int) (int, error) { return v + 1, nil },
		},
		pipeline.Stage[int]{
			Name: "failOdd",
			Worker: func(_ context.Context, v int) (int, error) {
				if v%2 != 0 {
					return 0, errMiddle
				}
				return v, nil
			},
		},
		pipeline.Stage[int]{
			Name:   "double",
			Worker: func(_ context.Context, v int) (int, error) { return v * 2, nil },
		},
	)

	in := make(chan int)
	go func() {
		for i := 0; i < 4; i++ {
			in <- i // add1 produces 1,2,3,4; failOdd drops 1,3
		}
		close(in)
	}()

	out, errs := p.Run(context.Background(), in)

	var results []int
	var errCount int
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range errs {
			errCount++
		}
	}()
	for v := range out {
		results = append(results, v)
	}
	wg.Wait()

	sort.Ints(results)
	expected := []int{4, 8} // 2*2, 4*2
	if len(results) != len(expected) {
		t.Fatalf("results: got %v, want %v", results, expected)
	}
	for i := range expected {
		if results[i] != expected[i] {
			t.Fatalf("results: got %v, want %v", results, expected)
		}
	}
	if errCount != 2 {
		t.Fatalf("expected 2 errors, got %d", errCount)
	}
}

// --- Stress ---

func TestRun_highConcurrency(t *testing.T) {
	p, _ := pipeline.New(
		pipeline.Stage[int]{
			Name:        "a",
			Concurrency: 16,
			Worker:      func(_ context.Context, v int) (int, error) { return v + 1, nil },
		},
		pipeline.Stage[int]{
			Name:        "b",
			Concurrency: 16,
			Worker:      func(_ context.Context, v int) (int, error) { return v * 2, nil },
		},
	)

	const n = 10000
	in := make(chan int)
	go func() {
		for i := 0; i < n; i++ {
			in <- i
		}
		close(in)
	}()

	out, errs := p.Run(context.Background(), in)
	go func() { for range errs {} }()

	count := 0
	for range out {
		count++
	}
	if count != n {
		t.Fatalf("expected %d items, got %d", n, count)
	}
}

func TestRun_noGoroutineLeak(t *testing.T) {
	// Give any goroutines from other tests time to settle.
	runtime.GC()
	time.Sleep(50 * time.Millisecond)
	before := runtime.NumGoroutine()

	p, _ := pipeline.New(
		pipeline.Stage[int]{
			Name:        "a",
			Concurrency: 4,
			Worker:      func(_ context.Context, v int) (int, error) { return v, nil },
		},
		pipeline.Stage[int]{
			Name:        "b",
			Concurrency: 4,
			Worker:      func(_ context.Context, v int) (int, error) { return v, nil },
		},
	)

	in := make(chan int)
	go func() {
		for i := 0; i < 100; i++ {
			in <- i
		}
		close(in)
	}()

	out, errs := p.Run(context.Background(), in)
	go func() { for range errs {} }()
	for range out {
	}

	// Wait for goroutines to wind down.
	time.Sleep(100 * time.Millisecond)
	runtime.GC()
	time.Sleep(50 * time.Millisecond)
	after := runtime.NumGoroutine()

	// Allow a small margin for runtime goroutines.
	if after > before+2 {
		t.Fatalf("goroutine leak: before=%d, after=%d", before, after)
	}
}
