package actor_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/dexisback/chaab/actor"
)

func TestSystem_CreateActorAndLookup(t *testing.T) {
	sys := actor.NewSystem()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = sys.Shutdown(ctx)
	})

	ref, err := sys.CreateActor(&testConsumerActor{})
	if err != nil {
		t.Fatalf("create actor: %v", err)
	}

	if ref == nil || ref.ID() == "" {
		t.Fatalf("invalid ref: %v", ref)
	}

	// Lookup existing
	foundRef, ok := sys.GetActor(ref.ID())
	if !ok || foundRef == nil {
		t.Fatalf("failed to look up actor %s", ref.ID())
	}
	if foundRef.ID() != ref.ID() {
		t.Fatalf("expected ID %s, got %s", ref.ID(), foundRef.ID())
	}

	// Lookup non-existent
	_, ok = sys.GetActor("missing-id")
	if ok {
		t.Fatal("expected GetActor to return false for missing actor")
	}
}

func TestSystem_CreateActor_NilActor(t *testing.T) {
	sys := actor.NewSystem()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = sys.Shutdown(ctx)
	})

	_, err := sys.CreateActor(nil)
	if !errors.Is(err, actor.ErrNilActor) {
		t.Fatalf("expected ErrNilActor, got: %v", err)
	}
}

func TestSystem_UniqueActorIDs(t *testing.T) {
	sys := actor.NewSystem()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = sys.Shutdown(ctx)
	})

	const count = 100
	ids := sync.Map{}
	var wg sync.WaitGroup
	wg.Add(count)

	for i := 0; i < count; i++ {
		go func() {
			defer wg.Done()
			ref, err := sys.CreateActor(&testConsumerActor{})
			if err != nil {
				t.Errorf("create actor: %v", err)
				return
			}
			if _, loaded := ids.LoadOrStore(ref.ID(), true); loaded {
				t.Errorf("duplicate actor ID detected: %s", ref.ID())
			}
		}()
	}

	wg.Wait()
}

// mockExternalDispatcher implements actor.Dispatcher to verify external extensibility.
type mockExternalDispatcher struct {
	mu            sync.Mutex
	registeredIDs []string
	closed        bool
}

func (m *mockExternalDispatcher) Register(cell *actor.ActorCell) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return actor.ErrDispatcherClosed
	}
	m.registeredIDs = append(m.registeredIDs, cell.Ref.ID())
	return nil
}

func (m *mockExternalDispatcher) Unregister(id string) error {
	return nil
}

func (m *mockExternalDispatcher) Shutdown(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	return nil
}

func TestSystem_CustomDispatcherSupport(t *testing.T) {
	mock := &mockExternalDispatcher{}
	sys := actor.NewSystem(actor.WithDispatcher(mock))

	ref, err := sys.CreateActor(&testConsumerActor{})
	if err != nil {
		t.Fatalf("create actor with custom dispatcher: %v", err)
	}

	mock.mu.Lock()
	registered := mock.registeredIDs
	mock.mu.Unlock()

	if len(registered) != 1 || registered[0] != ref.ID() {
		t.Fatalf("expected custom dispatcher to receive registration for %s, got %v", ref.ID(), registered)
	}

	// NewSystemWithDispatcher convenience constructor
	mock2 := &mockExternalDispatcher{}
	sys2 := actor.NewSystemWithDispatcher(mock2)
	ref2, err := sys2.CreateActor(&testConsumerActor{})
	if err != nil {
		t.Fatalf("create actor with NewSystemWithDispatcher: %v", err)
	}
	if ref2 == nil {
		t.Fatal("expected non-nil ref")
	}
}

func TestSystem_UnregisterUpdatesRegistry(t *testing.T) {
	sys := actor.NewSystem()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = sys.Shutdown(ctx)
	})

	ref, err := sys.CreateActor(&testConsumerActor{})
	if err != nil {
		t.Fatalf("create actor: %v", err)
	}

	if err := sys.Unregister(ref.ID()); err != nil {
		t.Fatalf("unregister: %v", err)
	}

	// Must be removed from registry
	if _, ok := sys.GetActor(ref.ID()); ok {
		t.Fatalf("actor %s still present in registry after unregister", ref.ID())
	}

	// Unregistering unknown actor returns ErrActorNotFound
	if err := sys.Unregister("missing"); !errors.Is(err, actor.ErrActorNotFound) {
		t.Fatalf("expected ErrActorNotFound for missing actor, got: %v", err)
	}
}

func TestSystem_StopAndStopActor(t *testing.T) {
	sys := actor.NewSystem()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = sys.Shutdown(ctx)
	})

	// Test Stop
	ref1, err := sys.CreateActor(&testConsumerActor{})
	if err != nil {
		t.Fatalf("create actor 1: %v", err)
	}

	if err := sys.Stop(ref1.ID()); err != nil {
		t.Fatalf("Stop failed: %v", err)
	}

	if _, ok := sys.GetActor(ref1.ID()); ok {
		t.Fatalf("expected actor %s to be removed after Stop", ref1.ID())
	}

	// Test StopActor
	ref2, err := sys.CreateActor(&testConsumerActor{})
	if err != nil {
		t.Fatalf("create actor 2: %v", err)
	}

	if err := sys.StopActor(ref2.ID()); err != nil {
		t.Fatalf("StopActor failed: %v", err)
	}

	if _, ok := sys.GetActor(ref2.ID()); ok {
		t.Fatalf("expected actor %s to be removed after StopActor", ref2.ID())
	}
}

type systemPanicActor struct {
	panicked chan struct{}
}

func (p *systemPanicActor) Receive(ctx context.Context, message any) error {
	if message == "panic" {
		close(p.panicked)
		panic("simulated panic in actor")
	}
	return nil
}

func TestSystem_UnregisterPanickedActor(t *testing.T) {
	// Verify that if an actor panics and terminates in the dispatcher,
	// calling sys.Unregister cleanly purges it from System.actors rather
	// than leaving a zombie entry.
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

			panicked := make(chan struct{})
			ref, err := sys.CreateActor(&systemPanicActor{panicked: panicked})
			if err != nil {
				t.Fatalf("create actor: %v", err)
			}

			if err := ref.Tell(context.Background(), "panic"); err != nil {
				t.Fatalf("tell panic: %v", err)
			}

			select {
			case <-panicked:
			case <-time.After(time.Second):
				t.Fatal("timed out waiting for actor to panic")
			}

			// Allow background dispatcher teardown to finish
			time.Sleep(50 * time.Millisecond)

			// Unregister must succeed and clean up registry
			if err := sys.Unregister(ref.ID()); err != nil {
				t.Fatalf("unregister on panicked actor failed: %v", err)
			}

			if _, ok := sys.GetActor(ref.ID()); ok {
				t.Fatalf("expected panicked actor %s to be removed from registry", ref.ID())
			}

			// Second unregister should fail with ErrActorNotFound
			if err := sys.Unregister(ref.ID()); !errors.Is(err, actor.ErrActorNotFound) {
				t.Fatalf("expected ErrActorNotFound on second unregister, got: %v", err)
			}
		})
	}
}

func TestSystem_ShutdownRejectsNewActorCreation(t *testing.T) {
	sys := actor.NewSystem()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	if err := sys.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}

	_, err := sys.CreateActor(&testConsumerActor{})
	if !errors.Is(err, actor.ErrDispatcherClosed) {
		t.Fatalf("expected ErrDispatcherClosed on CreateActor after shutdown, got: %v", err)
	}
}

func TestSystem_ShutdownClearsRegistry(t *testing.T) {
	sys := actor.NewSystem()
	ref, err := sys.CreateActor(&testConsumerActor{})
	if err != nil {
		t.Fatalf("create actor: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	if err := sys.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}

	if _, ok := sys.GetActor(ref.ID()); ok {
		t.Fatalf("expected registry to be cleared after shutdown, but actor %s was found", ref.ID())
	}
}

// slowShutdownDispatcher simulates a dispatcher whose shutdown exceeds context deadline.
type slowShutdownDispatcher struct {
	actor.Dispatcher
	delay time.Duration
}

func (s *slowShutdownDispatcher) Shutdown(ctx context.Context) error {
	select {
	case <-time.After(s.delay):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func TestSystem_ShutdownTimeoutDoesNotFalselyReportCompletion(t *testing.T) {
	slow := &slowShutdownDispatcher{
		Dispatcher: actor.NewGoroutineDispatcher(),
		delay:      500 * time.Millisecond,
	}
	sys := actor.NewSystem(actor.WithDispatcher(slow))

	ref, err := sys.CreateActor(&testConsumerActor{})
	if err != nil {
		t.Fatalf("create actor: %v", err)
	}

	// Fast timeout to force failure
	shortCtx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	err = sys.Shutdown(shortCtx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context.DeadlineExceeded, got: %v", err)
	}

	// Registry must NOT be falsely cleared on failure
	if _, ok := sys.GetActor(ref.ID()); !ok {
		t.Fatal("registry was falsely cleared despite failed shutdown")
	}
}

func TestSystem_ConcurrentCreateActorUnregisterTellShutdown(t *testing.T) {
	sys := actor.NewSystem()
	const count = 30
	var wg sync.WaitGroup

	refs := make([]*actor.ActorRef, count)
	for i := 0; i < count; i++ {
		ref, err := sys.CreateActor(&testConsumerActor{})
		if err != nil {
			t.Fatalf("create actor %d: %v", i, err)
		}
		refs[i] = ref
	}

	// Concurrently send messages
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(r *actor.ActorRef, idx int) {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				_ = r.Tell(context.Background(), fmt.Sprintf("%d-%d", idx, j))
			}
		}(refs[i], i)
	}

	// Concurrently unregister a subset
	for i := 0; i < count/3; i++ {
		wg.Add(1)
		go func(r *actor.ActorRef) {
			defer wg.Done()
			_ = sys.Unregister(r.ID())
		}(refs[i])
	}

	// Concurrently create new actors
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = sys.CreateActor(&testConsumerActor{})
		}()
	}

	// Concurrently initiate shutdown
	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(2 * time.Millisecond)
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = sys.Shutdown(shutdownCtx)
	}()

	wg.Wait()
}
