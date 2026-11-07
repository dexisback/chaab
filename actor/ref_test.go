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

type echoActor struct {
	received chan actor.Message
}

func (e *echoActor) Receive(ctx context.Context, message any) error {
	select {
	case e.received <- message:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func TestActorRef_TellDeliversMessages(t *testing.T) {
	sys := actor.NewSystem()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = sys.Shutdown(ctx)
	})

	received := make(chan actor.Message, 5)
	ref, err := sys.CreateActor(&echoActor{received: received})
	if err != nil {
		t.Fatalf("create actor failed: %v", err)
	}

	ctx := context.Background()
	for i := 1; i <= 3; i++ {
		if err := ref.Tell(ctx, i); err != nil {
			t.Fatalf("tell %d failed: %v", i, err)
		}
	}

	for i := 1; i <= 3; i++ {
		select {
		case msg := <-received:
			if msg != i {
				t.Fatalf("expected %d, got %v", i, msg)
			}
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for message %d", i)
		}
	}
}

func TestActorRef_InvalidInputs(t *testing.T) {
	sys := actor.NewSystem()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = sys.Shutdown(ctx)
	})

	ref, err := sys.CreateActor(&echoActor{received: make(chan actor.Message, 1)})
	if err != nil {
		t.Fatalf("create actor failed: %v", err)
	}

	// Nil context
	if err := ref.Tell(nil, "payload"); !errors.Is(err, actor.ErrNilContext) {
		t.Fatalf("expected ErrNilContext, got: %v", err)
	}

	// Nil message
	if err := ref.Tell(context.Background(), nil); !errors.Is(err, actor.ErrInvalidMessage) {
		t.Fatalf("expected ErrInvalidMessage, got: %v", err)
	}

	// Nil ActorRef
	var nilRef *actor.ActorRef
	if err := nilRef.Tell(context.Background(), "msg"); !errors.Is(err, actor.ErrActorStopped) {
		t.Fatalf("expected ErrActorStopped for nil ref, got: %v", err)
	}
}

func TestActorRef_SendingAfterTerminationReturnsErrActorStopped(t *testing.T) {
	sys := actor.NewSystem()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = sys.Shutdown(ctx)
	})

	received := make(chan actor.Message, 1)
	ref, err := sys.CreateActor(&echoActor{received: received})
	if err != nil {
		t.Fatalf("create actor: %v", err)
	}

	if err := sys.Unregister(ref.ID()); err != nil {
		t.Fatalf("unregister failed: %v", err)
	}

	// Tell on unregistered actor must return ErrActorStopped
	err = ref.Tell(context.Background(), "post-stop")
	if !errors.Is(err, actor.ErrActorStopped) {
		t.Fatalf("expected ErrActorStopped after unregister, got: %v", err)
	}
}

func TestActorRef_ConcurrentTells(t *testing.T) {
	sys := actor.NewSystem()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = sys.Shutdown(ctx)
	})

	receivedCount := atomic.Int64{}
	done := make(chan struct{})
	const totalMessages = 200

	ref, err := sys.CreateActor(&testConsumerActor{
		onMessage: func(msg any) {
			if receivedCount.Add(1) == totalMessages {
				close(done)
			}
		},
	})
	if err != nil {
		t.Fatalf("create actor: %v", err)
	}

	const goroutines = 10
	const perGoroutine = totalMessages / goroutines
	var wg sync.WaitGroup
	ctx := context.Background()

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(gID int) {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				if err := ref.Tell(ctx, gID*1000+i); err != nil {
					t.Errorf("tell failed: %v", err)
					return
				}
			}
		}(g)
	}

	wg.Wait()

	select {
	case <-done:
		// Succeeded
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out: processed %d out of %d messages", receivedCount.Load(), totalMessages)
	}
}

type testConsumerActor struct {
	onMessage func(msg any)
}

func (t *testConsumerActor) Receive(ctx context.Context, message any) error {
	if t.onMessage != nil {
		t.onMessage(message)
	}
	return nil
}
