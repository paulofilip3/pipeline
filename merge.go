package pipeline

import (
	"context"
	"sync"
)

// Merge fans-in multiple channels into a single output channel. The returned
// channel is closed when all input channels have been drained or ctx is
// cancelled. Merge is safe to call with zero channels, in which case the
// returned channel is closed immediately.
func Merge[T any](ctx context.Context, channels ...<-chan T) <-chan T {
	out := make(chan T)

	var wg sync.WaitGroup
	wg.Add(len(channels))

	for _, ch := range channels {
		go func(c <-chan T) {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case v, ok := <-c:
					if !ok {
						return
					}
					select {
					case out <- v:
					case <-ctx.Done():
						return
					}
				}
			}
		}(ch)
	}

	go func() {
		wg.Wait()
		close(out)
	}()

	return out
}
