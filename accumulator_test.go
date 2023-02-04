package accumulator

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func BenchmarkAccumulatorNoDelay(b *testing.B) {

	ch := make(chan uint, 9000000)

	feedChan(ch, 9000000, 0)
	close(ch)

	acc := NewAccumulator(ch, 300, time.Millisecond/500, false)

	var total = 0
	calls := make(map[CallOrigin]int, 3)
	b.ReportAllocs()
	b.ResetTimer()

	acc.Accumulate(context.Background(), func(c CallOrigin, t []uint) {
		total += len(t)
		calls[c]++
	})

	b.StopTimer()

	fmt.Println("total", total)
	fmt.Println("calls", calls)

}

func BenchmarkAccumulatorDelay(b *testing.B) {

	ch := make(chan uint, 10000)

	acc := NewAccumulator(ch, 50, time.Millisecond, false)

	var total = 0
	calls := make(map[CallOrigin]int, 3)

	go func() {
		feedChan(ch, 500, time.Second)
		close(ch)
	}()

	b.ReportAllocs()
	b.ResetTimer()

	acc.Accumulate(context.Background(), func(c CallOrigin, t []uint) {
		total += len(t)
		calls[c]++
	})

	b.StopTimer()

	fmt.Println("total", total)
	fmt.Println("calls", calls)

}

func feedChan(c chan uint, qty uint, during time.Duration) {

	sleepFor := during / time.Duration(qty)

	for i := uint(0); i < qty; i++ {
		c <- i
		if during > 0 {
			time.Sleep(sleepFor)
		}
	}

}
