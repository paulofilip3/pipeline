package pipeline

import (
	"context"
	"fmt"
	"sync"
)

// ChannelId can be used to reference channels added to a Pipeline.
// They can be passed to any Stage as input or output channel ids
// to enable chaining.
type ChannelId int

// Pipeline is the main type of this package. It is used to combine a series
// of Stages and run them.
type Pipeline struct {
	Channels  map[ChannelId]chan interface{}
	Stages    map[string]Stage
	WaitGroup *sync.WaitGroup
}

// Stage represents a specific data processing task. It has an underlying
// worker and an In and Out ChannelId. The Concurrency field determines how
// many goroutines will be launched.
type Stage struct {
	Name        string
	Worker      Worker
	In          ChannelId
	Out         ChannelId
	Concurrency int
}

// Worker interface represents a Stage's underlying data processor.
// It exposes In() and Out() methods that return generic write and read channels
// respectively. The Loop() method is expected to read from the channel exposed
// by In(), process the data and write it to the channel exposed by Out(). The
// Loop() method should only return when the passed context is closed or when
// the Worker's input channel is closed. A Worker must be safe to use concurrently.
type Worker interface {
	Loop(ctx context.Context, wg *sync.WaitGroup)
	In() chan<- interface{}
	Out() <-chan interface{}
}

// NewPipeline should be used to initialize a new Pipeline.
func NewPipeline() *Pipeline {
	return &Pipeline{
		Channels: map[ChannelId]chan interface{}{
			// channel on id=0 is a dummy channel to be used whenever a step
			// does not need to listen on any channel
			ChannelId(0): make(chan interface{}),
		},
		Stages:    make(map[string]Stage),
		WaitGroup: &sync.WaitGroup{},
	}
}

// AddStages adds n number of stages to the Pipeline.
func (p *Pipeline) AddStages(stages ...Stage) {
	for _, stage := range stages {
		if stage.Worker == nil {
			panic("Stages need a Worker")
		}
		if stage.Concurrency < 1 {
			stage.Concurrency = 1
		}
		p.Stages[stage.Name] = stage
	}
}

// AddChannels adds n number of ChannelIds to the Pipeline.
func (p *Pipeline) AddChannels(ids ...ChannelId) {
	for _, id := range ids {
		if id <= 0 {
			panic("Workflow channels need to have an id bigger than 0")
		}
		p.Channels[id] = make(chan interface{})
	}

}

// Run is used to start the Pipeline. It will launch every Stage in a
// new goroutine.
func (p *Pipeline) Run(ctx context.Context) {
	for name, stage := range p.Stages {
		fmt.Printf("Launching %s\n", name)
		go p.runStage(ctx, stage)
	}
}

// runStage is used internally to run each Stage. It will read data from the
// Stage's input channel, write it to the underlying worker input channel and
// read data from the worker output channel and write it to the Stage's output
// channel.
func (p *Pipeline) runStage(ctx context.Context, stage Stage) {
	p.WaitGroup.Add(1)
	defer close(stage.Worker.In())
	defer p.WaitGroup.Done()

	// start the workers in the background
	for w := 0; w < stage.Concurrency; w++ {
		// Worker methods shall have value receivers so that a new Worker
		// instance is created on every call of Loop()
		go stage.Worker.Loop(ctx, p.WaitGroup)
	}

	// queues at stage level allows for the buffering
	// of results in and out of each stage
	// note: buffered channels between Stages could have eliminated
	// the need for Stage queues, but they wouldn't be as flexible as
	// they would always have a cap that once reached could cause a deadlock
	var inQueue, outQueue []interface{}

	// the case for accepting new input is always active
	stageIn := p.Channels[stage.In]
	workerOut := stage.Worker.Out()

	// a nil channel always blocks, so we can selectively enable and
	// disable select cases at each iteration
	for {

		// pointers to the next in and out values
		var nextIn, nextOut interface{}

		// worker's input channel and stage's output channels
		// are nil by default
		var workerIn chan<- interface{}
		var stageOut chan<- interface{}

		// if we have data in the input queue, enable the worker's input channel
		// and set the pointer to the next value in the queue
		if len(inQueue) > 0 {
			workerIn = stage.Worker.In()
			nextIn = inQueue[0]
		}

		// if we have data in the output queue, enable the stage's output
		// channel and set the pointer to the next value in the queue
		if len(outQueue) > 0 {
			stageOut = p.Channels[stage.Out]
			nextOut = outQueue[0]
		}

		// run until shutdown signal comes
		select {
		// do we need to shut down?
		case <-ctx.Done():
			fmt.Printf("Shutting down %s\n", stage.Name)
			return
		// if stageOut is not nil, we send data to the next Stage and advance
		// the queue forward
		case stageOut <- nextOut:
			outQueue = outQueue[1:]
		// if workerIn is not nil, we send data to the Worker and advance
		// the queue forward
		case workerIn <- nextIn:
			inQueue = inQueue[1:]
		// if data comes into the Stage, append it to the input queue
		case msg := <-stageIn:
			inQueue = append(inQueue, msg)
		// if data comes out of the Worker, append it to the output queue.
		case msg := <-workerOut:
			outQueue = append(outQueue, msg)
		}
	}
}
