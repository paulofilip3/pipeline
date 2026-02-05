package pipeline_test

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/paulofilip3/pipeline"
)

func TestMerge_multipleSources(t *testing.T) {
	ch1 := make(chan int, 3)
	ch2 := make(chan int, 3)
	ch3 := make(chan int, 3)

	ch1 <- 1
	ch1 <- 2
	close(ch1)
	ch2 <- 3
	ch2 <- 4
	close(ch2)
	ch3 <- 5
	close(ch3)

	out := pipeline.Merge(context.Background(), ch1, ch2, ch3)

	var results []int
	for v := range out {
		results = append(results, v)
	}
	sort.Ints(results)

	expected := []int{1, 2, 3, 4, 5}
	if len(results) != len(expected) {
		t.Fatalf("got %v, want %v", results, expected)
	}
	for i := range expected {
		if results[i] != expected[i] {
			t.Fatalf("got %v, want %v", results, expected)
		}
	}
}

func TestMerge_singleSource(t *testing.T) {
	ch := make(chan int, 2)
	ch <- 10
	ch <- 20
	close(ch)

	out := pipeline.Merge(context.Background(), ch)

	var results []int
	for v := range out {
		results = append(results, v)
	}
	sort.Ints(results)

	if len(results) != 2 || results[0] != 10 || results[1] != 20 {
		t.Fatalf("got %v, want [10 20]", results)
	}
}

func TestMerge_zeroSources(t *testing.T) {
	out := pipeline.Merge[int](context.Background())

	count := 0
	for range out {
		count++
	}
	if count != 0 {
		t.Fatalf("expected 0 items, got %d", count)
	}
}

func TestMerge_contextCancel(t *testing.T) {
	ch := make(chan int) // unbuffered, will block

	ctx, cancel := context.WithCancel(context.Background())

	out := pipeline.Merge(ctx, ch)

	// Let merge goroutine start.
	time.Sleep(20 * time.Millisecond)
	cancel()

	// Output channel must close after cancel.
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	for {
		select {
		case _, ok := <-out:
			if !ok {
				return // success
			}
		case <-timer.C:
			t.Fatal("timed out waiting for merge output to close after cancel")
		}
	}
}

func TestMerge_emptySources(t *testing.T) {
	ch1 := make(chan int)
	ch2 := make(chan int)
	close(ch1)
	close(ch2)

	out := pipeline.Merge(context.Background(), ch1, ch2)

	count := 0
	for range out {
		count++
	}
	if count != 0 {
		t.Fatalf("expected 0 items, got %d", count)
	}
}
