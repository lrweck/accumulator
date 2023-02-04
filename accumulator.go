package main

import (
	"context"
	"errors"
	"sync"
	"time"
)


type Batched[T any] struct {
	input     <-chan T
	maxsize   uint
	timeout   time.Duration
	drain     bool
	batchPool sync.Pool
}

const (
	defaultSize    uint = 100
	defaultTimeout      = time.Duration(defaultSize) * time.Millisecond
)

// New creates a new batcher. 
// When either maxsize or a timeout is reached, a batch is completed.
func New[T any](input <-chan T, maxsize uint, timeout time.Duration) *Batched[T] {

	size := defaultSize
	timeO := defaultTimeout

	if maxsize > 0 {
		size = maxsize
	}
	if timeout > 0 {
		timeO = timeout
	}

	return &Batched[T]{
		input:   input,
		maxsize: size,
		timeout: timeO,
		batchPool: sync.Pool{
			New: func() any {
				ss := make([]T, 0, size)
				return &ss
			},
		},
	}
}

func (a *Batched[T]) getNewSlice() *[]T {
	ssAny := a.batchPool.Get()
	if ssAny == nil {
		panic("slice is nil")
	}
	ss, ok := ssAny.(*[]T)
	if !ok {
		panic("failed to asset slice")
	}
	return ss
}

func (a *Batched[T]) giveSliceBack(s *[]T) {
	a.batchPool.Put(s)
}

func (a *Batched[T]) drainChan() []T {
	slice := make([]T, 0, len(a.input))
	for v := range a.input {
		slice = append(slice, v)
	}
	return slice
}

// CallOrigin defines what is the origin of the batch
type CallOrigin uint

const (
	OriginSize CallOrigin = iota
	OriginTimeout
	OriginRemaining
)

func (c CallOrigin) String() string {
	switch c {
	case OriginTimeout:
		return "timeout"
	case OriginSize:
		return "size"
	case OriginRemaining:
		return "remaining"
	}
	return "unknown"
}

// Accumulate will read from `chan input` until it is closed or `ctx` is `Done()`
// When the either a timeout or the max size is reached, it will call `fn` with the items
// and a CallOrigin.
// Can return the errors:
// 	"fn cannot be nil"
//	context.DeadlineExceeded
//	context.Canceled
//
// When context is `Done()`, no further reads to the channel will be made
func (a *Batched[T]) Accumulate(ctx context.Context, fn func(CallOrigin, []T)) error {

	if fn == nil {
		return errors.New("fn cannot be nil")
	}

	var batchTimeout chan struct{} = nil

	startBatchTimeout := func() {
		if batchTimeout != nil {
			return
		}
		batchTimeout = make(chan struct{})
		go func() {
			time.Sleep(a.timeout)
			batchTimeout <- struct{}{}
			batchTimeout = nil
		}()
	}
	batch := a.getNewSlice()

	processBatch := func(o CallOrigin) {
		if len(*batch) > 0 {
			fn(o, *batch)
			a.giveSliceBack(batch)
			batch = a.getNewSlice()
		}
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-batchTimeout:
			processBatch(OriginTimeout)
		case req, open := <-a.input:
			if !open {
				processBatch(OriginRemaining)
				return nil
			}
			*batch = append(*batch, req)
			startBatchTimeout()
			if uint(len(*batch)) >= a.maxsize {
				processBatch(OriginSize)
			}
		}
	}
}
