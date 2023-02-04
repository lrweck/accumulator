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

func New[T any](input <-chan T, maxsize uint, timeout time.Duration, drain bool) *Batched[T] {

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
		drain:   drain,
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
				if a.drain {
					*batch = append(*batch, a.drainChan()...)
				}
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
