[![Go Reference](https://pkg.go.dev/badge/github.com/paulofilip3/pipeline.svg)](https://pkg.go.dev/github.com/paulofilip3/pipeline)
[![Go Report Card](https://goreportcard.com/badge/github.com/paulofilip3/pipeline)](https://goreportcard.com/report/github.com/paulofilip3/pipeline)
![Build](https://github.com/paulofilip3/pipeline/actions/workflows/go.yml/badge.svg?branch=main)

# pipeline

A lightweight, generic, concurrent pipeline library for Go.

Build streaming data pipelines where each stage processes items concurrently,
errors are captured (not swallowed), and completion propagates naturally through
channel close cascading.

## Features

- **Type-safe generics** — `Pipeline[T]`, `WorkerFunc[T]`, no `interface{}`
- **Concurrent stages** — set `Concurrency` per stage for parallel processing
- **Two shutdown modes** — drain (close input) or daemon (cancel context)
- **Error channel** — per-item errors are collected, not dropped
- **Fan-in with `Merge`** — composable helper to merge multiple channels
- **Zero dependencies** — only the Go standard library
- **~150 LOC** — small and auditable

## Install

```
go get github.com/paulofilip3/pipeline
```

## Quick start

```go
package main

import (
	"context"
	"fmt"
	"sort"

	"github.com/paulofilip3/pipeline"
)

func main() {
	p, _ := pipeline.New(
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

	in := make(chan int)
	go func() {
		for _, v := range []int{1, 2, 3} {
			in <- v
		}
		close(in)
	}()

	out, errs := p.Run(context.Background(), in)
	go func() { for range errs {} }() // drain errors

	var results []int
	for v := range out {
		results = append(results, v)
	}
	sort.Ints(results)

	for _, v := range results {
		fmt.Println(v) // 4, 6, 8
	}
}
```

## Patterns

### Drain mode

Close the input channel and every stage finishes processing remaining items,
then closes its output channel in turn:

```go
close(in) // triggers cascading shutdown
for v := range out { /* ... */ }
// both out and errs are closed when done
```

### Daemon mode

Cancel the context and all goroutines exit promptly:

```go
ctx, cancel := context.WithCancel(context.Background())
out, errs := p.Run(ctx, in)
// ...
cancel() // all stages stop
```

### Fan-in with Merge

Combine multiple pipelines or channels into one:

```go
merged := pipeline.Merge(ctx, chanA, chanB, chanC)
for v := range merged { /* ... */ }
```

### Discarding errors

If you don't care about errors, drain the channel in a goroutine:

```go
go func() { for range errs {} }()
```

Both `out` and `errs` **must** be drained to prevent goroutine leaks.

## Documentation

Full API reference on [pkg.go.dev](https://pkg.go.dev/github.com/paulofilip3/pipeline).
