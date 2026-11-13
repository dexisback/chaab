package actor_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dexisback/chaab/actor"
)

func TestDeadLetters_PublishOnStoppedActor(t *testing.T) {
	sys := actor.NewSystem()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = sys.Shutdown(ctx)
	})

	var mu sync.Mutex
	captured := make([]actor.DeadLetter, 0)
	done := make(chan struct{})

	unsubscribe := sys.SubscribeDeadLetters(func(letter actor.DeadLetter) {
		mu.Lock()
		captured = append(captured, letter)
		if len(captured) == 1 {
			close(done)
		}
		mu.Unlock()
	})
	defer unsubscribe()

	ref, err := sys.CreateActor(&testConsumerActor{})
	if err != nil {
		t.Fatalf("create actor: %v", err)
	}

	// Stop actor
	if err := sys.Stop(ref.ID()); err != nil {
		t.Fatalf("stop actor: %v", err)
	}

	// Tell on stopped actor
	ctx := context.Background()
	err = ref.Tell(ctx, "undeliverable-payload")
	if !errors.Is(err, actor.ErrActorStopped) {
		t.Fatalf("expected ErrActorStopped, got: %v", err)
	}

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for dead letter publication")
	}

	mu.Lock()
	defer mu.Unlock()

	if len(captured) != 1 {
		t.Fatalf("expected 1 dead letter, got %d", len(captured))
	}
	dl := captured[0]
	if dl.RecipientID != ref.ID() {
		t.Fatalf("expected recipient %s, got %s", ref.ID(), dl.RecipientID)
	}
	if dl.Message != "undeliverable-payload" {
		t.Fatalf("expected message 'undeliverable-payload', got %v", dl.Message)
	}
	if !errors.Is(dl.Reason, actor.ErrActorStopped) {
		t.Fatalf("expected ErrActorStopped reason, got %v", dl.Reason)
	}
}

func TestDeadLetters_PublishOnMailboxOverflowFailFast(t *testing.T) {
	sys := actor.NewSystem()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = sys.Shutdown(ctx)
	})

	var mu sync.Mutex
	captured := make([]actor.DeadLetter, 0)
	done := make(chan struct{})

	sys.SubscribeDeadLetters(func(letter actor.DeadLetter) {
		mu.Lock()
		captured = append(captured, letter)
		if len(captured) == 1 {
			close(done)
		}
		mu.Unlock()
	})

	activeStarted := make(chan struct{})
	proceed := make(chan struct{})
	defer close(proceed)

	ref, err := sys.CreateActor(&testConsumerActor{
		onMessage: func(msg any) {
			if msg == "active-msg" {
				close(activeStarted)
				<-proceed
			}
		},
	}, actor.WithMailboxCapacity(1), actor.WithOverflowStrategy(actor.OverflowFailFast))
	if err != nil {
		t.Fatalf("create actor: %v", err)
	}

	ctx := context.Background()
	// Fill active worker turn and wait until it is dequeued into the worker
	_ = ref.Tell(ctx, "active-msg")
	<-activeStarted

	// Fill the single mailbox slot
	if err := ref.Tell(ctx, "buffered-msg"); err != nil {
		t.Fatalf("buffered tell failed: %v", err)
	}

	// Next message overflows FailFast
	err = ref.Tell(ctx, "overflow-msg")
	if !errors.Is(err, actor.ErrMailboxFull) {
		t.Fatalf("expected ErrMailboxFull, got: %v", err)
	}

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for dead letter on fail-fast overflow")
	}

	mu.Lock()
	defer mu.Unlock()

	if len(captured) != 1 {
		t.Fatalf("expected 1 dead letter, got %d", len(captured))
	}
	if captured[0].Message != "overflow-msg" {
		t.Fatalf("expected message 'overflow-msg', got %v", captured[0].Message)
	}
	if !errors.Is(captured[0].Reason, actor.ErrMailboxFull) {
		t.Fatalf("expected ErrMailboxFull reason, got %v", captured[0].Reason)
	}
}

func TestDeadLetters_PublishOnMailboxOverflowDropOldest(t *testing.T) {
	sys := actor.NewSystem()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = sys.Shutdown(ctx)
	})

	var mu sync.Mutex
	captured := make([]actor.DeadLetter, 0)
	done := make(chan struct{})

	sys.SubscribeDeadLetters(func(letter actor.DeadLetter) {
		mu.Lock()
		captured = append(captured, letter)
		if len(captured) == 1 {
			close(done)
		}
		mu.Unlock()
	})

	activeStarted := make(chan struct{})
	hold := make(chan struct{})
	defer close(hold)

	ref, err := sys.CreateActor(&testConsumerActor{
		onMessage: func(msg any) {
			if msg == "turn-1" {
				close(activeStarted)
				<-hold
			}
		},
	}, actor.WithMailboxCapacity(1), actor.WithOverflowStrategy(actor.OverflowDropOldest))
	if err != nil {
		t.Fatalf("create actor: %v", err)
	}

	ctx := context.Background()
	_ = ref.Tell(ctx, "turn-1")
	<-activeStarted // wait until turn-1 is dequeued into the worker

	// Fill slot 1
	if err := ref.Tell(ctx, "slot-1"); err != nil {
		t.Fatalf("slot-1 tell failed: %v", err)
	}

	// Slot-2 evicts slot-1
	if err := ref.Tell(ctx, "slot-2"); err != nil {
		t.Fatalf("slot-2 tell failed: %v", err)
	}

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for dead letter on drop-oldest eviction")
	}

	mu.Lock()
	defer mu.Unlock()

	if len(captured) != 1 {
		t.Fatalf("expected 1 dead letter, got %d", len(captured))
	}
	// The evicted message was "slot-1"
	if captured[0].Message != "slot-1" {
		t.Fatalf("expected evicted message 'slot-1', got %v", captured[0].Message)
	}
	if !errors.Is(captured[0].Reason, actor.ErrMailboxFull) {
		t.Fatalf("expected ErrMailboxFull reason, got %v", captured[0].Reason)
	}
}

func TestDeadLetters_PublishOnMailboxOverflowDropNewest(t *testing.T) {
	sys := actor.NewSystem()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = sys.Shutdown(ctx)
	})

	var mu sync.Mutex
	captured := make([]actor.DeadLetter, 0)
	done := make(chan struct{})

	sys.SubscribeDeadLetters(func(letter actor.DeadLetter) {
		mu.Lock()
		captured = append(captured, letter)
		if len(captured) == 1 {
			close(done)
		}
		mu.Unlock()
	})

	activeStarted := make(chan struct{})
	hold := make(chan struct{})
	defer close(hold)

	ref, err := sys.CreateActor(&testConsumerActor{
		onMessage: func(msg any) {
			if msg == "turn-1" {
				close(activeStarted)
				<-hold
			}
		},
	}, actor.WithMailboxCapacity(1), actor.WithOverflowStrategy(actor.OverflowDropNewest))
	if err != nil {
		t.Fatalf("create actor: %v", err)
	}

	ctx := context.Background()
	_ = ref.Tell(ctx, "turn-1")
	<-activeStarted

	if err := ref.Tell(ctx, "slot-1"); err != nil {
		t.Fatalf("slot-1 tell failed: %v", err)
	}

	// drop-me is discarded
	if err := ref.Tell(ctx, "drop-me"); err != nil {
		t.Fatalf("drop-me tell failed: %v", err)
	}

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for dead letter on drop-newest discard")
	}

	mu.Lock()
	defer mu.Unlock()

	if len(captured) != 1 {
		t.Fatalf("expected 1 dead letter, got %d", len(captured))
	}
	if captured[0].Message != "drop-me" {
		t.Fatalf("expected dropped message 'drop-me', got %v", captured[0].Message)
	}
	if !errors.Is(captured[0].Reason, actor.ErrMailboxFull) {
		t.Fatalf("expected ErrMailboxFull reason, got %v", captured[0].Reason)
	}
}

func TestDeadLetters_Unsubscribe(t *testing.T) {
	sys := actor.NewSystem()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = sys.Shutdown(ctx)
	})

	count := atomic.Int32{}
	unsubscribe := sys.SubscribeDeadLetters(func(letter actor.DeadLetter) {
		count.Add(1)
	})

	ref, _ := sys.CreateActor(&testConsumerActor{})
	_ = sys.Stop(ref.ID())

	// First dead letter: received
	_ = ref.Tell(context.Background(), "msg1")
	if count.Load() != 1 {
		t.Fatalf("expected count 1, got %d", count.Load())
	}

	// Unsubscribe
	unsubscribe()

	// Second dead letter: should NOT be received
	_ = ref.Tell(context.Background(), "msg2")
	if count.Load() != 1 {
		t.Fatalf("expected count to remain 1 after unsubscribe, got %d", count.Load())
	}
}

func TestDeadLetters_ConcurrentSubscribers(t *testing.T) {
	sys := actor.NewSystem()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = sys.Shutdown(ctx)
	})

	const numSubscribers = 10
	const numMessages = 50
	counts := make([]atomic.Int32, numSubscribers)

	unsubs := make([]func(), numSubscribers)
	for i := 0; i < numSubscribers; i++ {
		idx := i
		unsubs[i] = sys.SubscribeDeadLetters(func(letter actor.DeadLetter) {
			counts[idx].Add(1)
		})
	}

	ref, _ := sys.CreateActor(&testConsumerActor{})
	_ = sys.Stop(ref.ID())

	// Concurrently send messages to stopped actor
	var wg sync.WaitGroup
	ctx := context.Background()

	for i := 0; i < numMessages; i++ {
		wg.Add(1)
		go func(val int) {
			defer wg.Done()
			_ = ref.Tell(ctx, val)
		}(i)
	}

	wg.Wait()

	// Verify all subscribers received all 50 dead letters
	for i := 0; i < numSubscribers; i++ {
		if c := counts[i].Load(); c != numMessages {
			t.Fatalf("subscriber %d received %d dead letters, expected %d", i, c, numMessages)
		}
		unsubs[i]()
	}
}
