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

type statefulRestartActor struct {
	instanceID int
	received   []any
	panicked   chan struct{}
	done       chan struct{}
	expected   int
}

func (a *statefulRestartActor) Receive(ctx context.Context, message any) error {
	str, ok := message.(string)
	if ok && str == "panic" {
		if a.panicked != nil {
			select {
			case <-a.panicked:
			default:
				close(a.panicked)
			}
		}
		panic("simulated state crash")
	}

	a.received = append(a.received, message)
	if str == "msg2" && a.done != nil {
		close(a.done)
	}
	return nil
}

func TestSupervision_RestartCreatesFreshInstanceAndPreservesMailbox(t *testing.T) {
	dispatchers := []struct {
		name string
		newD func() actor.Dispatcher
	}{
		{"Goroutine", func() actor.Dispatcher { return actor.NewGoroutineDispatcher() }},
		{"WorkerPool", func() actor.Dispatcher {
			return actor.NewWorkerPoolDispatcher(actor.WorkerPoolConfig{Workers: 2, MessageBudget: 5})
		}},
		{"WorkStealing", func() actor.Dispatcher {
			return actor.NewWorkStealingDispatcher(&actor.WorkStealingConfig{MaxWorkers: 2, MaxMessageBudget: 5})
		}},
	}

	for _, tc := range dispatchers {
		t.Run(tc.name, func(t *testing.T) {
			sys := actor.NewSystem(actor.WithDispatcher(tc.newD()))
			t.Cleanup(func() {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				_ = sys.Shutdown(ctx)
			})

			instanceCounter := atomic.Int32{}
			done := make(chan struct{})
			panicked := make(chan struct{})

			strategy := actor.NewOneForOneStrategy(3, time.Second, nil)

			ref, err := sys.CreateActorFromFactory(func() (actor.Actor, error) {
				id := int(instanceCounter.Add(1))
				return &statefulRestartActor{
					instanceID: id,
					panicked:   panicked,
					done:       done,
					expected:   2, // expects msg1 and msg2
				}, nil
			}, actor.WithSupervisor(strategy))
			if err != nil {
				t.Fatalf("create actor: %v", err)
			}

			originalRefID := ref.ID()
			ctx := context.Background()

			// Send valid message 1
			if err := ref.Tell(ctx, "msg1"); err != nil {
				t.Fatalf("tell msg1 failed: %v", err)
			}

			// Send poisoned message that triggers panic
			if err := ref.Tell(ctx, "panic"); err != nil {
				t.Fatalf("tell panic failed: %v", err)
			}

			// Send valid message 2 (queued behind panic)
			if err := ref.Tell(ctx, "msg2"); err != nil {
				t.Fatalf("tell msg2 failed: %v", err)
			}

			select {
			case <-panicked:
			case <-time.After(time.Second):
				t.Fatal("timed out waiting for panic")
			}

			// Wait for restart and completion of remaining queued messages
			select {
			case <-done:
			case <-time.After(2 * time.Second):
				t.Fatal("timed out waiting for actor to process messages after restart")
			}

			// Verify that an actor restart occurred (instanceCounter must be 2)
			if count := instanceCounter.Load(); count != 2 {
				t.Fatalf("expected 2 actor instances created, got %d", count)
			}

			// Verify that ActorRef ID remained unchanged across restarts
			if ref.ID() != originalRefID {
				t.Fatalf("ActorRef ID mutated: expected %s, got %s", originalRefID, ref.ID())
			}

			// Verify mailbox is still alive and accepting sends
			if err := ref.Tell(ctx, "post-restart-msg"); err != nil {
				t.Fatalf("post-restart tell failed: %v", err)
			}
		})
	}
}

func TestSupervision_MaxRestartsExceededTerminatesActor(t *testing.T) {
	dispatchers := []struct {
		name string
		newD func() actor.Dispatcher
	}{
		{"Goroutine", func() actor.Dispatcher { return actor.NewGoroutineDispatcher() }},
		{"WorkerPool", func() actor.Dispatcher {
			return actor.NewWorkerPoolDispatcher(actor.WorkerPoolConfig{Workers: 2, MessageBudget: 5})
		}},
		{"WorkStealing", func() actor.Dispatcher {
			return actor.NewWorkStealingDispatcher(&actor.WorkStealingConfig{MaxWorkers: 2, MaxMessageBudget: 5})
		}},
	}

	for _, tc := range dispatchers {
		t.Run(tc.name, func(t *testing.T) {
			sys := actor.NewSystem(actor.WithDispatcher(tc.newD()))
			t.Cleanup(func() {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				_ = sys.Shutdown(ctx)
			})

			// Max 2 restarts allowed
			strategy := actor.NewOneForOneStrategy(2, 5*time.Second, nil)

			ref, err := sys.CreateActorFromFactory(func() (actor.Actor, error) {
				return &statefulRestartActor{}, nil
			}, actor.WithSupervisor(strategy))
			if err != nil {
				t.Fatalf("create actor: %v", err)
			}

			ctx := context.Background()

			// Trigger 1st panic (Restart 1)
			_ = ref.Tell(ctx, "panic")
			time.Sleep(30 * time.Millisecond)

			// Trigger 2nd panic (Restart 2)
			_ = ref.Tell(ctx, "panic")
			time.Sleep(30 * time.Millisecond)

			// Trigger 3rd panic (Exceeds limit -> DirectiveStop)
			_ = ref.Tell(ctx, "panic")
			time.Sleep(50 * time.Millisecond)

			// Subsequent tell must be rejected with ErrActorStopped
			err = ref.Tell(ctx, "msg-after-death")
			if !errors.Is(err, actor.ErrActorStopped) {
				t.Fatalf("expected ErrActorStopped after exceeding max restarts, got: %v", err)
			}
		})
	}
}

func TestSupervision_PoisonedMessageNotRetried(t *testing.T) {
	sys := actor.NewSystem()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = sys.Shutdown(ctx)
	})

	var mu sync.Mutex
	received := make([]string, 0)
	done := make(chan struct{})

	strategy := actor.NewOneForOneStrategy(3, time.Second, nil)

	ref, err := sys.CreateActorFromFactory(func() (actor.Actor, error) {
		return &testConsumerActor{
			onMessage: func(msg any) {
				str := msg.(string)
				mu.Lock()
				received = append(received, str)
				if str == "after-panic" {
					close(done)
				}
				mu.Unlock()

				if str == "poison" {
					panic("poison pill")
				}
			},
		}, nil
	}, actor.WithSupervisor(strategy))
	if err != nil {
		t.Fatalf("create actor: %v", err)
	}

	ctx := context.Background()
	_ = ref.Tell(ctx, "before")
	_ = ref.Tell(ctx, "poison")
	_ = ref.Tell(ctx, "after-panic")

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for after-panic message")
	}

	mu.Lock()
	defer mu.Unlock()

	// Verify "poison" was received only ONCE (never retried!)
	poisonCount := 0
	for _, m := range received {
		if m == "poison" {
			poisonCount++
		}
	}
	if poisonCount != 1 {
		t.Fatalf("expected poison message to be delivered exactly once, got %d", poisonCount)
	}
}
