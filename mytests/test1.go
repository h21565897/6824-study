package main

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type cc struct {
	cc1 int
}

var mu sync.Mutex
var counter int

func G1() {

}

func main() {
	ch1 := make(chan int)
	ctx1, _ := context.WithTimeout(context.Background(), 2000*time.Millisecond)
	select {
	case <-ch1:
	case <-ctx1.Done():
		fmt.Printf("Done\n")
	}
}
