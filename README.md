# accumulator

Allows you to receive input values from a channel, accumulate them (over a time period or size limit) and run a func on them.

Really easy to use:

```go
package main

import (
  _ "github.com/lrweck/accumulator"
)

func main(){
  
  inputChan := make(chan int, 10)
  
  batch := New(inputChan, 100, time.Second, true)
  
  go func() {
    for i := 0; i < 1000; i++ {
      inputChan <- i
    }
    close(inputChan)
  }()
  
  err := batch.Accumulate(context.Background(), func(o CallOrigin, items []int){
    fmt.Printf("received %d items via %q", len(items), o)
  })
  if err != nil {
    panic(err)
  }
  
}
```
It is able to achieve high performance by minimizing channel communication, and avoiding busy-looping.
