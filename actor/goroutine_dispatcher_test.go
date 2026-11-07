package actor

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type recordingActor struct {
	got chan Message
}

func (a recordingActor) Receive(ctx context.Context, message any) error {
	select {
	case a.got <- message:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func TestGoroutineDispatcherDeliversMessages(t *testing.T) {
	dispatcher := NewGoroutineDispatcher()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := dispatcher.Shutdown(ctx); err != nil {
			t.Errorf("shutdown: %v", err)
		}
	})

	mailbox := NewMailBox(4)
	got := make(chan Message, 2)
	cell := &ActorCell{
		Actor: recordingActor{got: got},
		Ref: &ActorRef{
			id:      "actor-1",
			mailbox: mailbox,
		},
		Mailbox: mailbox,
	}

	if err := dispatcher.Register(cell); err != nil {
		t.Fatalf("register: %v", err)
	}
	if err := dispatcher.Register(cell); !errors.Is(err, ErrActorAlreadyExists) {
		t.Fatalf("second register: got %v", err)
	}

	ctx := context.Background()
	if err := mailbox.Send(ctx, "one"); err != nil {
		t.Fatalf("send one: %v", err)
	}
	if err := mailbox.Send(ctx, "two"); err != nil {
		t.Fatalf("send two: %v", err)
	}

	gotMessages := map[Message]bool{}
	timeout := time.After(2 * time.Second)
	for len(gotMessages) < 2 {
		select {
		case msg := <-got:
			gotMessages[msg] = true
		case <-timeout:
			t.Fatalf("timed out waiting for messages, got %v", gotMessages)
		}
	}

	if err := dispatcher.Unregister("missing"); !errors.Is(err, ErrActorNotFound) {
		t.Fatalf("unregister missing: got %v", err)
	}
	if err := dispatcher.Unregister(cell.Ref.ID()); err != nil {
		t.Fatalf("unregister: %v", err)
	}
}

func TestGoroutineDispatcherShutdownNilContext(t *testing.T) {
	dispatcher := NewGoroutineDispatcher()
	if err := dispatcher.Shutdown(nil); !errors.Is(err, ErrNilContext) {
		t.Fatalf("shutdown nil context: got %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := dispatcher.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
}

func TestActorSystemCreationAndRegistration(t *testing.T) {
	system := NewSystem()
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = system.Shutdown(ctx)
	}()

	// Nil actor validation
	if _, err := system.CreateActor(nil); !errors.Is(err, ErrNilActor) {
		t.Fatalf("expected ErrNilActor, got: %v", err)
	}

	got := make(chan Message, 1)
	ref, err := system.CreateActor(recordingActor{got: got})
	if err != nil {
		t.Fatalf("create actor: %v", err)
	}
	if ref == nil || ref.ID() == "" {
		t.Fatalf("expected valid ref, got %v", ref)
	}

	// Lookup actor
	retrieved, found := system.GetActor(ref.ID())
	if !found || retrieved.ID() != ref.ID() {
		t.Fatalf("failed to retrieve actor %s", ref.ID())
	}

	// Message delivery via Tell
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
		t.Fatal("timed out waiting for actor to receive message")
	}

	// Unregister actor
	if err := system.Unregister(ref.ID()); err != nil {
		t.Fatalf("system unregister: %v", err)
	}
	if _, found := system.GetActor(ref.ID()); found {
		t.Fatalf("expected actor %s to be removed from registry", ref.ID())
	}
	if err := system.Unregister(ref.ID()); !errors.Is(err, ErrActorNotFound) {
		t.Fatalf("expected ErrActorNotFound on second unregister, got: %v", err)
	}
}

type sequentialOrderActor struct {
	mu       sync.Mutex
	received []int
	done     chan struct{}
	expected int
}

func (s *sequentialOrderActor) Receive(ctx context.Context, message any) error {
	val := message.(int)
	s.mu.Lock()
	s.received = append(s.received, val)
	if len(s.received) == s.expected {
		close(s.done)
	}
	s.mu.Unlock()
	// Simulate minor processing time to ensure sequentiality is strictly maintained
	time.Sleep(time.Millisecond)
	return nil
}

func TestSequentialMessageProcessingPerActor(t *testing.T) {
	system := NewSystem()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	defer func() { _ = system.Shutdown(ctx) }()

	const total = 50
	orderActor := &sequentialOrderActor{
		received: make([]int, 0, total),
		done:     make(chan struct{}),
		expected: total,
	}

	ref, err := system.CreateActor(orderActor)
	if err != nil {
		t.Fatalf("failed to create actor: %v", err)
	}

	for i := 0; i < total; i++ {
		if err := ref.Tell(ctx, i); err != nil {
			t.Fatalf("tell %d failed: %v", i, err)
		}
	}

	select {
	case <-orderActor.done:
	case <-ctx.Done():
		t.Fatalf("timeout waiting for messages; processed %d", len(orderActor.received))
	}

	orderActor.mu.Lock()
	defer orderActor.mu.Unlock()
	for i, val := range orderActor.received {
		if val != i {
			t.Fatalf("out of order message at index %d: expected %d, got %d", i, i, val)
		}
	}
}

type barrierActor struct {
	barrier *sync.WaitGroup
	proceed chan struct{}
}

func (b *barrierActor) Receive(ctx context.Context, message any) error {
	b.barrier.Done()
	select {
	case <-b.proceed:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func TestConcurrentExecutionAcrossDifferentActors(t *testing.T) {
	system := NewSystem()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	defer func() { _ = system.Shutdown(ctx) }()

	const numActors = 5
	var barrier sync.WaitGroup
	barrier.Add(numActors)
	proceed := make(chan struct{})

	refs := make([]*ActorRef, numActors)
	for i := 0; i < numActors; i++ {
		act := &barrierActor{barrier: &barrier, proceed: proceed}
		ref, err := system.CreateActor(act)
		if err != nil {
			t.Fatalf("failed to create actor %d: %v", i, err)
		}
		refs[i] = ref
	}

	// Trigger all actors
	for _, ref := range refs {
		if err := ref.Tell(ctx, "ping"); err != nil {
			t.Fatalf("tell failed: %v", err)
		}
	}

	// If actors run on separate dedicated goroutines, all 5 will enter Receive concurrently
	// and decrements the barrier wait group.
	barrierDone := make(chan struct{})
	go func() {
		barrier.Wait()
		close(barrierDone)
	}()

	select {
	case <-barrierDone:
		// All 5 actors are executing concurrently!
		close(proceed)
	case <-ctx.Done():
		t.Fatal("timed out waiting for concurrent actors: dedicated goroutines not running concurrently")
	}
}

func TestActorUnregistration(t *testing.T) {
	dispatcher := NewGoroutineDispatcher()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	defer func() { _ = dispatcher.Shutdown(ctx) }()

	got := make(chan Message, 5)
	mailbox := NewMailBox(10)
	cell := &ActorCell{
		Actor:   recordingActor{got: got},
		Ref:     &ActorRef{id: "actor-to-unreg", mailbox: mailbox},
		Mailbox: mailbox,
	}

	if err := dispatcher.Register(cell); err != nil {
		t.Fatalf("register failed: %v", err)
	}

	if err := mailbox.Send(ctx, "before"); err != nil {
		t.Fatalf("send before failed: %v", err)
	}
	select {
	case msg := <-got:
		if msg != "before" {
			t.Fatalf("expected 'before', got %v", msg)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for 'before' message")
	}

	// Unregister
	if err := dispatcher.Unregister(cell.Ref.ID()); err != nil {
		t.Fatalf("unregister failed: %v", err)
	}

	// Second unregister fails with ErrActorNotFound
	if err := dispatcher.Unregister(cell.Ref.ID()); !errors.Is(err, ErrActorNotFound) {
		t.Fatalf("expected ErrActorNotFound, got: %v", err)
	}

	// Messages sent after unregistration are not processed
	if err := mailbox.Send(ctx, "after"); err != nil {
		t.Fatalf("send after failed: %v", err)
	}
	select {
	case msg := <-got:
		t.Fatalf("unexpected message processed after unregister: %v", msg)
	case <-time.After(50 * time.Millisecond):
		// Expected: no processing
	}
}

func TestDispatcherShutdown(t *testing.T) {
	dispatcher := NewGoroutineDispatcher()
	ctx := context.Background()

	got := make(chan Message, 10)
	cell := &ActorCell{
		Actor:   recordingActor{got: got},
		Ref:     &ActorRef{id: "actor-shut", mailbox: NewMailBox(10)},
		Mailbox: NewMailBox(10),
	}

	if err := dispatcher.Register(cell); err != nil {
		t.Fatalf("register: %v", err)
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer shutdownCancel()

	if err := dispatcher.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("shutdown failed: %v", err)
	}

	// Subsequent Register fails
	newCell := &ActorCell{
		Actor:   recordingActor{got: got},
		Ref:     &ActorRef{id: "actor-new", mailbox: NewMailBox(10)},
		Mailbox: NewMailBox(10),
	}
	if err := dispatcher.Register(newCell); !errors.Is(err, ErrDispatcherClosed) {
		t.Fatalf("expected ErrDispatcherClosed, got %v", err)
	}

	// Subsequent Unregister returns ErrDispatcherClosed
	if err := dispatcher.Unregister("actor-shut"); !errors.Is(err, ErrDispatcherClosed) {
		t.Fatalf("expected ErrDispatcherClosed on unregister, got %v", err)
	}

	// Idempotent Shutdown
	if err := dispatcher.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("idempotent shutdown failed: %v", err)
	}
	_ = ctx
}

func TestSendingMessagesDuringShutdownAndUnregistration(t *testing.T) {
	dispatcher := NewGoroutineDispatcher()
	mailbox := NewMailBox(100)
	got := make(chan Message, 100)
	cell := &ActorCell{
		Actor:   recordingActor{got: got},
		Ref:     &ActorRef{id: "actor-concurrent", mailbox: mailbox},
		Mailbox: mailbox,
	}

	if err := dispatcher.Register(cell); err != nil {
		t.Fatalf("register: %v", err)
	}

	var stopSending int32
	var wg sync.WaitGroup

	// Multiple sender goroutines
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(senderID int) {
			defer wg.Done()
			for atomic.LoadInt32(&stopSending) == 0 {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
				_ = cell.Ref.Tell(ctx, senderID)
				cancel()
				time.Sleep(50 * time.Microsecond)
			}
		}(i)
	}

	time.Sleep(20 * time.Millisecond)

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), time.Second)
	defer shutdownCancel()
	if err := dispatcher.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}

	atomic.StoreInt32(&stopSending, 1)
	wg.Wait()
}

func TestRaceConditionsLifecycle(t *testing.T) {
	system := NewSystem()
	var wg sync.WaitGroup
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	const actorCount = 20
	refs := make([]*ActorRef, actorCount)

	// Concurrently create actors
	for i := 0; i < actorCount; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			got := make(chan Message, 20)
			ref, err := system.CreateActor(recordingActor{got: got})
			if err == nil {
				refs[idx] = ref
			}
		}(i)
	}
	wg.Wait()

	// Concurrently Tell and GetActor
	for i := 0; i < actorCount; i++ {
		ref := refs[i]
		if ref == nil {
			continue
		}
		wg.Add(2)
		go func(r *ActorRef) {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				_ = r.Tell(ctx, j)
			}
		}(ref)

		go func(id string) {
			defer wg.Done()
			_, _ = system.GetActor(id)
		}(ref.ID())
	}

	// Concurrently Unregister a subset of actors
	for i := 0; i < actorCount/2; i++ {
		ref := refs[i]
		if ref == nil {
			continue
		}
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			_ = system.Unregister(id)
		}(ref.ID())
	}

	wg.Wait()

	// Shutdown system
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), time.Second)
	defer shutdownCancel()
	if err := system.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("system shutdown failed: %v", err)
	}
}

// Ensure Dispatcher interface is implementable externally with zero unexported types
type mockExternalDispatcher struct{}

func (m *mockExternalDispatcher) Register(cell *ActorCell) error     { return nil }
func (m *mockExternalDispatcher) Unregister(id string) error         { return nil }
func (m *mockExternalDispatcher) Shutdown(ctx context.Context) error { return nil }

var _ Dispatcher = (*mockExternalDispatcher)(nil)

func TestSystemWithCustomDispatcher(t *testing.T) {
	custom := &mockExternalDispatcher{}
	system := NewSystem(WithDispatcher(custom))
	if system.dispatcher != custom {
		t.Fatal("expected custom dispatcher to be configured")
	}

	ref, err := system.CreateActor(recordingActor{got: make(chan Message)})
	if err != nil {
		t.Fatalf("create actor with custom dispatcher: %v", err)
	}
	if ref == nil {
		t.Fatal("expected valid ref")
	}
}
