package actor_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/dexisback/chaab/actor"
)

// TestWorkerPoolDispatcher_Contract runs the standard Dispatcher
// conformance suite against WorkerPoolDispatcher.
func TestWorkerPoolDispatcher_Contract(t *testing.T) {
	RunDispatcherContractTests(t, func() actor.Dispatcher {
		return actor.NewWorkerPoolDispatcher(actor.WorkerPoolConfig{
			Workers:       4,
			MessageBudget: 10,
		})
	})
}

// TestWorkerPoolDispatcher_WorkerLimit verifies that no more than
// the configured number of actors execute simultaneously.
func TestWorkerPoolDispatcher_WorkerLimit(t *testing.T) {
	d := actor.NewWorkerPoolDispatcher(actor.WorkerPoolConfig{
		Workers:       2,
		MessageBudget: 10,
	})

	sys := actor.NewSystem(actor.WithDispatcher(d))

	release := make(chan struct{})
	var releaseOnce sync.Once

	defer releaseOnce.Do(func() {
		close(release)
	})

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = d.Shutdown(ctx)
	})

	const actorCount = 5

	entered := make(chan struct{}, actorCount)

	for i := 0; i < actorCount; i++ {
		ref, err := sys.CreateActor(&testConsumerActor{
			onMessage: func(msg any) {
				entered <- struct{}{}
				<-release
			},
		})
		if err != nil {
			t.Fatalf("create actor %d: %v", i, err)
		}

		if err := ref.Tell(context.Background(), i); err != nil {
			t.Fatalf("send message to actor %d: %v", i, err)
		}
	}

	// Both workers should become occupied.
	for i := 0; i < 2; i++ {
		select {
		case <-entered:
		case <-time.After(2 * time.Second):
			t.Fatalf("only %d workers entered Receive", i)
		}
	}

	// No third actor should execute while both workers are occupied.
	select {
	case <-entered:
		t.Fatal("more than 2 actors executed simultaneously")
	case <-time.After(100 * time.Millisecond):
	}

	// Release the two occupied workers.
	releaseOnce.Do(func() {
		close(release)
	})

	// The remaining actors should eventually execute.
	for i := 0; i < actorCount-2; i++ {
		select {
		case <-entered:
		case <-time.After(2 * time.Second):
			t.Fatalf("only %d actors entered Receive", i+2)
		}
	}
}

// TestWorkerPoolDispatcher_SequentialExecution verifies that
// two messages belonging to the same actor are never processed concurrently.
func TestWorkerPoolDispatcher_SequentialExecution(t *testing.T) {
	d := actor.NewWorkerPoolDispatcher(actor.WorkerPoolConfig{
		Workers:       4,
		MessageBudget: 10,
	})

	sys := actor.NewSystem(actor.WithDispatcher(d))

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = d.Shutdown(ctx)
	})

	var (
		mu        sync.Mutex
		active    int
		maxActive int
		processed int
		received  []int
	)

	const messageCount = 20

	processedAll := make(chan struct{})

	ref, err := sys.CreateActor(&testConsumerActor{
		onMessage: func(msg any) {
			val := msg.(int)

			mu.Lock()
			active++
			if active > maxActive {
				maxActive = active
			}
			received = append(received, val)
			mu.Unlock()

			// Yield to encourage concurrency if scheduler is faulty
			time.Sleep(2 * time.Millisecond)

			mu.Lock()
			active--
			processed++

			if processed == messageCount {
				close(processedAll)
			}
			mu.Unlock()
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < messageCount; i++ {
		if err := ref.Tell(context.Background(), i); err != nil {
			t.Fatalf("send message %d: %v", i, err)
		}
	}

	select {
	case <-processedAll:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for messages to be processed")
	}

	mu.Lock()
	defer mu.Unlock()

	if maxActive != 1 {
		t.Fatalf("expected maximum concurrent executions of 1, got %d", maxActive)
	}

	// Check FIFO message ordering
	for i, v := range received {
		if v != i {
			t.Fatalf("message out of order at %d: expected %d, got %d", i, i, v)
		}
	}
}

// TestWorkerPoolDispatcher_MessageBudgetYielding verifies that an actor with
// many messages yields its worker after MessageBudget turns, allowing other queued
// actors to make progress.
func TestWorkerPoolDispatcher_MessageBudgetYielding(t *testing.T) {
	// 1 worker so that execution turns must be shared sequentially
	d := actor.NewWorkerPoolDispatcher(actor.WorkerPoolConfig{
		Workers:       1,
		MessageBudget: 2,
	})

	sys := actor.NewSystem(actor.WithDispatcher(d))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = d.Shutdown(ctx)
	})

	executionOrder := make([]string, 0, 10)
	var mu sync.Mutex
	done := make(chan struct{})
	aStarted := make(chan struct{}, 1)
	bSent := make(chan struct{})

	// Actor A will receive 6 messages
	refA, err := sys.CreateActor(&testConsumerActor{
		onMessage: func(msg any) {
			if val, ok := msg.(int); ok && val == 0 {
				select {
				case aStarted <- struct{}{}:
				default:
				}
				<-bSent
			}

			mu.Lock()
			executionOrder = append(executionOrder, "A")
			if len(executionOrder) == 7 {
				close(done)
			}
			mu.Unlock()
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Actor B will receive 1 message
	refB, err := sys.CreateActor(&testConsumerActor{
		onMessage: func(msg any) {
			mu.Lock()
			executionOrder = append(executionOrder, "B")
			if len(executionOrder) == 7 {
				close(done)
			}
			mu.Unlock()
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	// Enqueue 6 messages for A
	for i := 0; i < 6; i++ {
		_ = refA.Tell(ctx, i)
	}

	// Wait for A to begin processing message 0
	<-aStarted

	// Enqueue message for B while A is in its first budget turn
	_ = refB.Tell(ctx, "b-msg")
	close(bSent)

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for execution")
	}

	mu.Lock()
	defer mu.Unlock()

	// Verify that B ran BEFORE A finished all 6 messages (because A yielded after budget of 2)
	bIndex := -1
	for idx, name := range executionOrder {
		if name == "B" {
			bIndex = idx
			break
		}
	}

	if bIndex == -1 {
		t.Fatal("Actor B never executed")
	}
	if bIndex >= 6 {
		t.Fatalf("Actor B ran at index %d (after all of A); MessageBudget did not yield worker", bIndex)
	}
}

// TestWorkerPoolDispatcher_IdleActorsDoNotOccupyWorkers verifies that actors
// with no pending messages do not hold onto worker threads.
func TestWorkerPoolDispatcher_IdleActorsDoNotOccupyWorkers(t *testing.T) {
	// 1 worker pool
	d := actor.NewWorkerPoolDispatcher(actor.WorkerPoolConfig{
		Workers:       1,
		MessageBudget: 10,
	})

	sys := actor.NewSystem(actor.WithDispatcher(d))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = d.Shutdown(ctx)
	})

	// Create 10 idle actors (no messages sent to them)
	for i := 0; i < 10; i++ {
		_, err := sys.CreateActor(&testConsumerActor{})
		if err != nil {
			t.Fatalf("create idle actor %d: %v", i, err)
		}
	}

	// Now create active actor and send message
	processed := make(chan struct{})
	activeRef, err := sys.CreateActor(&testConsumerActor{
		onMessage: func(msg any) {
			close(processed)
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := activeRef.Tell(context.Background(), "ping"); err != nil {
		t.Fatal(err)
	}

	select {
	case <-processed:
		// Succeeded: active actor executed immediately on the 1 available worker
	case <-time.After(time.Second):
		t.Fatal("timed out: idle actors are starving active actor on worker pool")
	}
}
