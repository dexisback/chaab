package actor_test

import (
	"context"
	"testing"
	"time"

	"github.com/dexisback/chaab/actor"
)

// TestGoroutineDispatcher_Contract runs the standard Dispatcher conformance suite
// against GoroutineDispatcher.
func TestGoroutineDispatcher_Contract(t *testing.T) {
	RunDispatcherContractTests(t, func() actor.Dispatcher {
		return actor.NewGoroutineDispatcher()
	})
}

// TestGoroutineDispatcher_DedicatedGoroutine verifies that each actor runs on its own dedicated goroutine.
func TestGoroutineDispatcher_DedicatedGoroutine(t *testing.T) {
	d := actor.NewGoroutineDispatcher()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = d.Shutdown(ctx)
	})

	sys := actor.NewSystem(actor.WithDispatcher(d))

	const actorCount = 5
	entered := make(chan struct{}, actorCount)
	release := make(chan struct{})

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
		_ = ref.Tell(context.Background(), i)
	}

	// If each actor has a dedicated goroutine, all 5 will enter their Receive concurrently.
	for i := 0; i < actorCount; i++ {
		select {
		case <-entered:
		case <-time.After(2 * time.Second):
			close(release)
			t.Fatalf("actor %d failed to enter: not running on dedicated goroutines", i)
		}
	}
	close(release)
}
