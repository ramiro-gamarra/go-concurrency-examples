package main

import (
	"context"
	"math/rand"
	"sync"
	"testing"
	"time"
)

func TestSignals_And_Propagation(t *testing.T) {
	t.Run("closing channels", func(t *testing.T) {
		ch := make(chan struct{})

		go func() {
			t.Log("worker 1 started")
			<-ch
			t.Log("worker 1 stopped")
		}()

		go func() {
			t.Log("worker 2 started")
			<-ch
			t.Log("worker 2 stopped")
		}()

		time.Sleep(5 * time.Second)
		close(ch)
		time.Sleep(time.Second)
	})

	t.Run("with context", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
		defer cancel()

		go func() {
			t.Log("worker 1 started")
			<-ctx.Done()
			t.Logf("worker 1 stopped due to %v", ctx.Err())
		}()

		go func() {
			t.Log("worker 2 started")
			<-ctx.Done()
			t.Logf("worker 2 stopped due to %v", ctx.Err())
		}()

		time.Sleep(5 * time.Second)
	})

	t.Run("context propagation", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
		defer cancel()

		go func() {
			t.Log("worker A started")
			childCtx, _ := context.WithTimeout(ctx, time.Second*100)

			go func() {
				t.Log("worker B started")
				<-childCtx.Done()
				t.Logf("worker B stopped due to %v", childCtx.Err())
			}()

			childCtx2, _ := context.WithTimeout(ctx, time.Second)
			go func() {
				t.Log("worker C started")
				<-childCtx2.Done()
				t.Logf("worker C stopped due to %v", childCtx2.Err())
			}()

			<-ctx.Done()
			t.Logf("worker A stopped due to %v", ctx.Err())
		}()

		go func() {
			t.Log("worker D started")
			<-ctx.Done()
			t.Logf("worker D stopped due to %v", ctx.Err())
		}()

		time.Sleep(10 * time.Second)
	})
}

func TestProducerConsumerProblems(t *testing.T) {
	t.Run("fan out", func(t *testing.T) {
		workCh := make(chan int)

		go func() {
			for i := 0; i < 1000; i++ {
				workCh <- i
			}
			close(workCh)
		}()

		workerCount := 10

		var wg sync.WaitGroup
		wg.Add(workerCount)
		for i := 0; i < workerCount; i++ {
			go func(workerID int) {
				agg := 0
				for work := range workCh {
					agg += work
				}
				t.Logf("worker %d, aggregate: %d", workerID, agg)
				wg.Done()
			}(i)
		}

		t.Log("waiting for workers to complete")
		wg.Wait()
		t.Log("workers done")
	})

	t.Run("fan in, multiplex channels", func(t *testing.T) {
		f := func() chan int {
			out := make(chan int)
			go func() {
				defer close(out)
				out <- rand.Int() // or a number that was really difficult to compute
			}()
			return out
		}

		// collect channels
		var futures []chan int
		for i := 0; i < 30; i++ {
			future := f()
			futures = append(futures, future)
		}

		// if we have variables shared by goroutines, they must be thread-safe
		var wg sync.WaitGroup
		fanInChan := make(chan int)

		// something must signal that fanning in is complete
		wg.Add(len(futures))
		go func() {
			wg.Wait()
			close(fanInChan)
		}()

		ctx := context.TODO()
		for _, ch := range futures {
			go pipe(ctx, &wg, ch, fanInChan)
		}

		for item := range fanInChan {
			t.Logf("got %d", item)
		}
	})
}

func pipe(ctx context.Context, wg *sync.WaitGroup, in chan int, out chan int) {
	defer wg.Done()

	// use context when you need to preempt blocking operations
	select {
	case <-ctx.Done():
		return
	case n := <-in:
		out <- n
	}
}
