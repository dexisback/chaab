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

// RunDispatcherContractTests executes the canonical conformance test suite for any actor.Dispatcher implementation.
// Every execution strategy (GoroutineDispatcher, WorkerPoolDispatcher, WorkStealingDispatcher) must pass this suite.
func RunDispatcherContractTests(t *testing.T, newDispatcher func() actor.Dispatcher) {
	t.Run("RegistrationAndDelivery", func(t *testing.T) {
		d := newDispatcher()
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_ = d.Shutdown(ctx)
		})

		mb := actor.NewMailBox(5)
		got := make(chan any, 5)
		sys := actor.NewSystem(actor.WithDispatcher(d))

		ref, err := sys.CreateActor(&testConsumerActor{
			onMessage: func(msg any) { got <- msg },
		})
		if err != nil {
			t.Fatalf("create actor failed: %v", err)
		}

		ctx := context.Background()
		if err := ref.Tell(ctx, "hello"); err != nil {
			t.Fatalf("tell failed: %v", err)
		}

		select {
		case msg := <-got:
			if msg != "hello" {
				t.Fatalf("expected 'hello', got %v", msg)
			}
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for message delivery")
		}
		_ = mb
	})

	t.Run("DuplicateRegistrationRejection", func(t *testing.T) {
		d := newDispatcher()
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_ = d.Shutdown(ctx)
		})

		sys := actor.NewSystem(actor.WithDispatcher(d))
		ref, err := sys.CreateActor(&testConsumerActor{})
		if err != nil {
			t.Fatalf("create actor failed: %v", err)
		}

		// Re-registering same cell on dispatcher directly
		cell := &actor.ActorCell{
			Actor:   &testConsumerActor{},
			Ref:     ref,
			Mailbox: ref.Mailbox(),
		}
		err = d.Register(cell)
		if !errors.Is(err, actor.ErrActorAlreadyExists) {
			t.Fatalf("expected ErrActorAlreadyExists, got: %v", err)
		}
	})

	t.Run("InvalidActorCellRejection", func(t *testing.T) {
		d := newDispatcher()
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_ = d.Shutdown(ctx)
		})

		// Nil cell
		if err := d.Register(nil); !errors.Is(err, actor.ErrInvalidActorCell) {
			t.Fatalf("expected ErrInvalidActorCell for nil cell, got: %v", err)
		}

		// Nil Actor
		sys := actor.NewSystem(actor.WithDispatcher(d))
		ref, _ := sys.CreateActor(&testConsumerActor{})
		cellNoActor := &actor.ActorCell{Actor: nil, Ref: ref, Mailbox: ref.Mailbox()}
		if err := d.Register(cellNoActor); !errors.Is(err, actor.ErrInvalidActorCell) {
			t.Fatalf("expected ErrInvalidActorCell for nil actor, got: %v", err)
		}

		// Nil Ref
		cellNoRef := &actor.ActorCell{Actor: &testConsumerActor{}, Ref: nil, Mailbox: actor.NewMailBox(1)}
		if err := d.Register(cellNoRef); !errors.Is(err, actor.ErrInvalidActorCell) {
			t.Fatalf("expected ErrInvalidActorCell for nil ref, got: %v", err)
		}
	})

	t.Run("SequentialProcessingWithinActor", func(t *testing.T) {
		d := newDispatcher()
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_ = d.Shutdown(ctx)
		})

		const count = 50
		done := make(chan struct{})
		act := &sequentialVerificationActor{expected: count, done: done}

		sys := actor.NewSystem(actor.WithDispatcher(d))
		ref, err := sys.CreateActor(act)
		if err != nil {
			t.Fatalf("create actor failed: %v", err)
		}

		ctx := context.Background()
		for i := 0; i < count; i++ {
			if err := ref.Tell(ctx, i); err != nil {
				t.Fatalf("tell failed: %v", err)
			}
		}

		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for sequential processing")
		}

		if max := act.maxOverlap.Load(); max != 1 {
			t.Fatalf("expected max overlap 1, got %d", max)
		}
	})

	t.Run("ConcurrentExecutionAcrossDifferentActors", func(t *testing.T) {
		d := newDispatcher()
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_ = d.Shutdown(ctx)
		})

		// Use 2 actors to prove concurrent execution between actors without
		// starving bounded worker pools with small worker counts.
		const numActors = 2
		var barrier sync.WaitGroup
		barrier.Add(numActors)
		proceed := make(chan struct{})

		sys := actor.NewSystem(actor.WithDispatcher(d))
		for i := 0; i < numActors; i++ {
			act := &testConsumerActor{
				onMessage: func(msg any) {
					barrier.Done()
					<-proceed
				},
			}
			ref, err := sys.CreateActor(act)
			if err != nil {
				t.Fatalf("create actor %d failed: %v", i, err)
			}
			_ = ref.Tell(context.Background(), "sync")
		}

		barrierDone := make(chan struct{})
		go func() {
			barrier.Wait()
			close(barrierDone)
		}()

		select {
		case <-barrierDone:
			// All actors are executing concurrently!
			close(proceed)
		case <-time.After(2 * time.Second):
			close(proceed)
			t.Fatal("timed out: actors not executing concurrently")
		}
	})

	t.Run("GracefulUnregistrationAndDrainage", func(t *testing.T) {
		d := newDispatcher()
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_ = d.Shutdown(ctx)
		})

		processed := atomic.Int32{}
		sys := actor.NewSystem(actor.WithDispatcher(d))

		ref, err := sys.CreateActor(&testConsumerActor{
			onMessage: func(msg any) {
				processed.Add(1)
			},
		})
		if err != nil {
			t.Fatalf("create actor: %v", err)
		}

		ctx := context.Background()
		for i := 0; i < 5; i++ {
			_ = ref.Tell(ctx, i)
		}

		// Unregister must wait for all 5 accepted messages to drain
		if err := sys.Unregister(ref.ID()); err != nil {
			t.Fatalf("unregister failed: %v", err)
		}

		if processed.Load() != 5 {
			t.Fatalf("expected all 5 messages drained before unregister returns, got %d", processed.Load())
		}

		// Post-unregister sends rejected
		if err := ref.Tell(ctx, 6); !errors.Is(err, actor.ErrActorStopped) {
			t.Fatalf("expected ErrActorStopped on post-unregister send, got: %v", err)
		}

		// Duplicate unregister returns ErrActorNotFound
		if err := sys.Unregister(ref.ID()); !errors.Is(err, actor.ErrActorNotFound) {
			t.Fatalf("expected ErrActorNotFound on second unregister, got: %v", err)
		}
	})

	t.Run("GracefulShutdownAndDrainage", func(t *testing.T) {
		d := newDispatcher()

		processed := atomic.Int32{}
		sys := actor.NewSystem(actor.WithDispatcher(d))

		ref, err := sys.CreateActor(&testConsumerActor{
			onMessage: func(msg any) {
				processed.Add(1)
			},
		})
		if err != nil {
			t.Fatalf("create actor: %v", err)
		}

		ctx := context.Background()
		for i := 0; i < 5; i++ {
			_ = ref.Tell(ctx, i)
		}

		shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		if err := sys.Shutdown(shutdownCtx); err != nil {
			t.Fatalf("shutdown failed: %v", err)
		}

		if processed.Load() != 5 {
			t.Fatalf("expected 5 drained messages during shutdown, got %d", processed.Load())
		}

		// CreateActor rejected after shutdown
		if _, err := sys.CreateActor(&testConsumerActor{}); !errors.Is(err, actor.ErrDispatcherClosed) {
			t.Fatalf("expected ErrDispatcherClosed for CreateActor after shutdown, got: %v", err)
		}

		// Direct dispatcher register rejected after shutdown
		mb := actor.NewMailBox(1)
		cell := &actor.ActorCell{
			Actor:   &testConsumerActor{},
			Ref:     ref,
			Mailbox: mb,
		}
		if err := d.Register(cell); !errors.Is(err, actor.ErrDispatcherClosed) {
			t.Fatalf("expected ErrDispatcherClosed for Register after shutdown, got: %v", err)
		}

		// Direct dispatcher unregister rejected after shutdown
		if err := d.Unregister(ref.ID()); !errors.Is(err, actor.ErrDispatcherClosed) {
			t.Fatalf("expected ErrDispatcherClosed for Unregister after shutdown, got: %v", err)
		}

		// Shutdown idempotency
		if err := d.Shutdown(shutdownCtx); err != nil {
			t.Fatalf("idempotent shutdown failed: %v", err)
		}
	})

	t.Run("ShutdownNilContext", func(t *testing.T) {
		d := newDispatcher()
		if err := d.Shutdown(nil); !errors.Is(err, actor.ErrNilContext) {
			t.Fatalf("expected ErrNilContext for nil ctx shutdown, got: %v", err)
		}
	})

	t.Run("ProcessingErrorsDoNotTerminateActor", func(t *testing.T) {
		d := newDispatcher()
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_ = d.Shutdown(ctx)
		})

		done := make(chan struct{})
		act := &contractErrorResilientActor{
			expected: 4,
			done:     done,
		}

		sys := actor.NewSystem(actor.WithDispatcher(d))
		ref, err := sys.CreateActor(act)
		if err != nil {
			t.Fatalf("create actor: %v", err)
		}

		ctx := context.Background()
		_ = ref.Tell(ctx, "ok1")
		_ = ref.Tell(ctx, "err")
		_ = ref.Tell(ctx, "err")
		_ = ref.Tell(ctx, "ok2")

		select {
		case <-done:
			if act.errCount.Load() != 2 {
				t.Fatalf("expected 2 errors, got %d", act.errCount.Load())
			}
			if act.totalCount.Load() != 4 {
				t.Fatalf("expected 4 total processed messages, got %d", act.totalCount.Load())
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out: actor stopped early on error, only processed %d/4", act.totalCount.Load())
		}
	})

	t.Run("ConcurrentUnregisterShutdownAndTell", func(t *testing.T) {
		d := newDispatcher()
		sys := actor.NewSystem(actor.WithDispatcher(d))

		const actorCount = 10
		refs := make([]*actor.ActorRef, actorCount)
		for i := 0; i < actorCount; i++ {
			ref, err := sys.CreateActor(&testConsumerActor{})
			if err != nil {
				t.Fatalf("create actor %d: %v", i, err)
			}
			refs[i] = ref
		}

		var wg sync.WaitGroup
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()

		// Continuous concurrent tells
		for i := 0; i < actorCount; i++ {
			wg.Add(1)
			go func(r *actor.ActorRef) {
				defer wg.Done()
				for j := 0; j < 50; j++ {
					_ = r.Tell(ctx, j)
				}
			}(refs[i])
		}

		// Concurrent unregisters of some actors
		for i := 0; i < actorCount/2; i++ {
			wg.Add(1)
			go func(id string) {
				defer wg.Done()
				_ = sys.Unregister(id)
			}(refs[i].ID())
		}

		// Concurrent shutdown
		wg.Add(1)
		go func() {
			defer wg.Done()
			shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer shutdownCancel()
			_ = sys.Shutdown(shutdownCtx)
		}()

		wg.Wait()
	})
}

type contractErrorResilientActor struct {
	errCount   atomic.Int32
	totalCount atomic.Int32
	done       chan struct{}
	expected   int32
}

func (e *contractErrorResilientActor) Receive(ctx context.Context, msg any) error {
	str, _ := msg.(string)
	if str == "err" {
		e.errCount.Add(1)
		if e.totalCount.Add(1) == e.expected {
			close(e.done)
		}
		return errors.New("simulated error")
	}
	if e.totalCount.Add(1) == e.expected {
		close(e.done)
	}
	return nil
}

