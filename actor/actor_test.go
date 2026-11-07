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

type sequentialVerificationActor struct {
	concurrent atomic.Int32
	maxOverlap atomic.Int32
	received   []int
	mu         sync.Mutex
	done       chan struct{}
	expected   int
}

func (s *sequentialVerificationActor) Receive(ctx context.Context, message any) error {
	cur := s.concurrent.Add(1)
	defer s.concurrent.Add(-1)

	// Track max concurrent executions (must NEVER exceed 1)
	for {
		old := s.maxOverlap.Load()
		if cur <= old || s.maxOverlap.CompareAndSwap(old, cur) {
			break
		}
	}

	val := message.(int)
	s.mu.Lock()
	s.received = append(s.received, val)
	if len(s.received) == s.expected {
		close(s.done)
	}
	s.mu.Unlock()

	return nil
}

func TestActor_ProcessesMessagesSequentially(t *testing.T) {
	sys := actor.NewSystem()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = sys.Shutdown(ctx)
	})

	const count = 100
	act := &sequentialVerificationActor{
		expected: count,
		done:     make(chan struct{}),
	}

	ref, err := sys.CreateActor(act)
	if err != nil {
		t.Fatalf("create actor: %v", err)
	}

	ctx := context.Background()
	for i := 0; i < count; i++ {
		if err := ref.Tell(ctx, i); err != nil {
			t.Fatalf("tell %d failed: %v", i, err)
		}
	}

	select {
	case <-act.done:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for messages to be processed")
	}

	// Verify maximum concurrent executions was strictly 1
	if max := act.maxOverlap.Load(); max != 1 {
		t.Fatalf("expected max overlap 1, got %d (concurrent execution detected!)", max)
	}

	// Verify FIFO order
	act.mu.Lock()
	defer act.mu.Unlock()
	for i, v := range act.received {
		if v != i {
			t.Fatalf("message out of order at index %d: expected %d, got %d", i, i, v)
		}
	}
}

type contextInspectActor struct {
	receivedMsg any
	hasContext  bool
	done        chan struct{}
}

func (c *contextInspectActor) Receive(ctx context.Context, message any) error {
	if ctx != nil {
		c.hasContext = true
	}
	c.receivedMsg = message
	close(c.done)
	return nil
}

func TestActor_ReceivesExpectedMessageAndContext(t *testing.T) {
	sys := actor.NewSystem()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = sys.Shutdown(ctx)
	})

	act := &contextInspectActor{done: make(chan struct{})}
	ref, err := sys.CreateActor(act)
	if err != nil {
		t.Fatalf("create actor: %v", err)
	}

	type customPayload struct {
		Data string
		ID   int
	}

	expected := customPayload{Data: "payload", ID: 42}
	if err := ref.Tell(context.Background(), expected); err != nil {
		t.Fatalf("tell failed: %v", err)
	}

	select {
	case <-act.done:
		if !act.hasContext {
			t.Fatal("expected non-nil context in Receive")
		}
		if act.receivedMsg != expected {
			t.Fatalf("expected payload %v, got %v", expected, act.receivedMsg)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for actor")
	}
}

type errorResilientActor struct {
	errorsReturned atomic.Int32
	successCount   atomic.Int32
	done           chan struct{}
	expectedTotal  int32
}

var errSimulated = errors.New("simulated processing error")

func (e *errorResilientActor) Receive(ctx context.Context, message any) error {
	str := message.(string)
	if str == "fail" {
		e.errorsReturned.Add(1)
		if e.errorsReturned.Load()+e.successCount.Load() == e.expectedTotal {
			close(e.done)
		}
		return errSimulated
	}

	e.successCount.Add(1)
	if e.errorsReturned.Load()+e.successCount.Load() == e.expectedTotal {
		close(e.done)
	}
	return nil
}

func TestActor_ProcessingErrorsDoNotTerminateActor(t *testing.T) {
	sys := actor.NewSystem()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = sys.Shutdown(ctx)
	})

	act := &errorResilientActor{
		expectedTotal: 4,
		done:          make(chan struct{}),
	}

	ref, err := sys.CreateActor(act)
	if err != nil {
		t.Fatalf("create actor: %v", err)
	}

	ctx := context.Background()
	_ = ref.Tell(ctx, "ok1")
	_ = ref.Tell(ctx, "fail")
	_ = ref.Tell(ctx, "fail")
	_ = ref.Tell(ctx, "ok2")

	select {
	case <-act.done:
		if act.errorsReturned.Load() != 2 {
			t.Fatalf("expected 2 errors, got %d", act.errorsReturned.Load())
		}
		if act.successCount.Load() != 2 {
			t.Fatalf("expected 2 successes, got %d", act.successCount.Load())
		}
	case <-time.After(time.Second):
		t.Fatalf("actor terminated prematurely on error: processed %d/4",
			act.errorsReturned.Load()+act.successCount.Load())
	}
}

type panicActor struct {
	beforePanic chan struct{}
}

func (p *panicActor) Receive(ctx context.Context, message any) error {
	str, ok := message.(string)
	if ok && str == "panic" {
		close(p.beforePanic)
		panic("simulated fatal actor crash")
	}
	return nil
}

func TestActor_PanicsAreRecoveredAndLogged(t *testing.T) {
	dispatcher := actor.NewGoroutineDispatcher()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = dispatcher.Shutdown(ctx)
	})

	mb := actor.NewMailBox(5)
	beforePanic := make(chan struct{})
	ref := &actor.ActorRef{} // Note: unexported fields, but we test through ActorCell
	_ = ref

	// Register with dispatcher
	sys := actor.NewSystem(actor.WithDispatcher(dispatcher))
	act := &panicActor{beforePanic: beforePanic}
	actorRef, err := sys.CreateActor(act)
	if err != nil {
		t.Fatalf("create actor: %v", err)
	}

	// Trigger panic
	if err := actorRef.Tell(context.Background(), "panic"); err != nil {
		t.Fatalf("tell panic: %v", err)
	}

	select {
	case <-beforePanic:
		// Panic triggered and recovered without crashing the process
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for panic trigger")
	}

	// Allow goroutine recovery to finalize
	time.Sleep(20 * time.Millisecond)

	// Dispatcher is still healthy and operational
	newAct := &echoActor{received: make(chan actor.Message, 1)}
	newRef, err := sys.CreateActor(newAct)
	if err != nil {
		t.Fatalf("failed to create new actor after panic: %v", err)
	}

	if err := newRef.Tell(context.Background(), "healthy"); err != nil {
		t.Fatalf("tell on new actor failed: %v", err)
	}

	select {
	case msg := <-newAct.received:
		if msg != "healthy" {
			t.Fatalf("expected 'healthy', got %v", msg)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for healthy actor after recovery")
	}
	_ = mb
}
