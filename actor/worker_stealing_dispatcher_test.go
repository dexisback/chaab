package actor

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// testActorFunc adapts a function to the Actor interface.
type testActorFunc func(context.Context, any) error

func (f testActorFunc) Receive(ctx context.Context, message any) error {
	return f(ctx, message)
}

// newTestActorCell creates an ActorCell with a mailbox and ActorRef.
func newTestActorCell(id string, actor Actor, capacity int) *ActorCell {
	mailbox := NewMailBox(capacity)

	ref := &ActorRef{
		id:      id,
		mailbox: mailbox,
	}

	return &ActorCell{
		Actor:   actor,
		Ref:     ref,
		Mailbox: mailbox,
	}
}

// waitFor waits until a condition becomes true or the timeout expires.
func waitFor(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()

	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		if condition() {
			return
		}

		time.Sleep(time.Millisecond)
	}

	t.Fatal("timed out waiting for condition")
}

func TestWorkStealingDispatcherRegisterInvalidActor(t *testing.T) {
	dispatcher := NewWorkStealingDispatcher(&WorkStealingConfig{
		MaxWorkers: 2,
	})

	defer dispatcher.Shutdown(context.Background())

	err := dispatcher.Register(nil)

	if !errors.Is(err, ErrInvalidActorCell) {
		t.Fatalf("expected ErrInvalidActorCell, got %v", err)
	}
}

func TestWorkStealingDispatcherRegisterDuplicateActor(t *testing.T) {
	dispatcher := NewWorkStealingDispatcher(&WorkStealingConfig{
		MaxWorkers:       2,
		MaxMessageBudget: 1,
	})

	defer dispatcher.Shutdown(context.Background())

	actor := testActorFunc(func(context.Context, any) error {
		return nil
	})

	cell := newTestActorCell("actor-1", actor, 10)

	if err := dispatcher.Register(cell); err != nil {
		t.Fatalf("first registration failed: %v", err)
	}

	err := dispatcher.Register(cell)

	if !errors.Is(err, ErrActorAlreadyExists) {
		t.Fatalf("expected ErrActorAlreadyExists, got %v", err)
	}

	if err := dispatcher.Unregister("actor-1"); err != nil {
		t.Fatalf("unregister failed: %v", err)
	}
}

func TestWorkStealingDispatcherActuallyStealsTasks(t *testing.T) {
	dispatcher := NewWorkStealingDispatcher(&WorkStealingConfig{
		MaxWorkers: 3,
	})

	defer dispatcher.Shutdown(context.Background())

	actor := testActorFunc(func(context.Context, any) error {
		return nil
	})

	cellA := newTestActorCell("actor-a", actor, 10)
	cellB := newTestActorCell("actor-b", actor, 10)
	cellC := newTestActorCell("actor-c", actor, 10)

	taskA := &workStealingTask{
		cell:    cellA,
		mailbox: cellA.Mailbox,
	}

	taskB := &workStealingTask{
		cell:    cellB,
		mailbox: cellB.Mailbox,
	}

	taskC := &workStealingTask{
		cell:    cellC,
		mailbox: cellC.Mailbox,
	}

	dispatcher.mu.Lock()

	// Worker 0 owns all three tasks. The owner pops from the front.
	// An idle worker steals from the back.
	dispatcher.queues[0] = []*workStealingTask{
		taskA,
		taskB,
		taskC,
	}

	stolen := dispatcher.getTaskLocked(1)
	remaining := len(dispatcher.queues[0])

	dispatcher.mu.Unlock()

	if stolen != taskC {
		t.Fatalf("expected worker 1 to steal task C, got %v", stolen)
	}

	if remaining != 2 {
		t.Fatalf("expected victim queue length 2, got %d", remaining)
	}
}

func TestWorkStealingDispatcherMessageDelivery(t *testing.T) {
	dispatcher := NewWorkStealingDispatcher(&WorkStealingConfig{
		MaxWorkers:       3,
		MaxMessageBudget: 5,
	})

	defer dispatcher.Shutdown(context.Background())

	var processed atomic.Int64

	actor := testActorFunc(func(ctx context.Context, message any) error {
		processed.Add(1)
		return nil
	})

	cell := newTestActorCell("actor-1", actor, 100)

	if err := dispatcher.Register(cell); err != nil {
		t.Fatalf("registration failed: %v", err)
	}

	const messageCount = 50

	for i := 0; i < messageCount; i++ {
		if err := cell.Ref.Tell(context.Background(), i); err != nil {
			t.Fatalf("failed to send message %d: %v", i, err)
		}
	}

	if err := dispatcher.Unregister("actor-1"); err != nil {
		t.Fatalf("unregister failed: %v", err)
	}

	if got := processed.Load(); got != messageCount {
		t.Fatalf("expected %d processed messages, got %d",
			messageCount, got)
	}
}

func TestWorkStealingDispatcherActorProcessesSequentially(t *testing.T) {
	dispatcher := NewWorkStealingDispatcher(&WorkStealingConfig{
		MaxWorkers:       4,
		MaxMessageBudget: 10,
	})

	defer dispatcher.Shutdown(context.Background())

	var active atomic.Int32
	var maxActive atomic.Int32
	var processed atomic.Int64

	actor := testActorFunc(func(ctx context.Context, message any) error {
		current := active.Add(1)

		for {
			previous := maxActive.Load()

			if current <= previous {
				break
			}

			if maxActive.CompareAndSwap(previous, current) {
				break
			}
		}

		time.Sleep(time.Millisecond)

		active.Add(-1)
		processed.Add(1)

		return nil
	})

	cell := newTestActorCell("actor-1", actor, 200)

	if err := dispatcher.Register(cell); err != nil {
		t.Fatalf("registration failed: %v", err)
	}

	const messageCount = 100

	var sendWG sync.WaitGroup

	for i := 0; i < messageCount; i++ {
		sendWG.Add(1)

		go func(i int) {
			defer sendWG.Done()

			if err := cell.Ref.Tell(context.Background(), i); err != nil {
				t.Errorf("send failed: %v", err)
			}
		}(i)
	}

	sendWG.Wait()

	if err := dispatcher.Unregister("actor-1"); err != nil {
		t.Fatalf("unregister failed: %v", err)
	}

	if got := processed.Load(); got != messageCount {
		t.Fatalf("expected %d processed messages, got %d",
			messageCount, got)
	}

	if got := maxActive.Load(); got != 1 {
		t.Fatalf("actor executed concurrently: maximum active receives = %d", got)
	}
}

func TestWorkStealingDispatcherWorkersExecuteConcurrently(t *testing.T) {
	dispatcher := NewWorkStealingDispatcher(&WorkStealingConfig{
		MaxWorkers:       2,
		MaxMessageBudget: 10,
	})

	defer dispatcher.Shutdown(context.Background())

	entered := make(chan struct{}, 2)
	release := make(chan struct{})

	createBlockingActor := func() Actor {
		return testActorFunc(func(ctx context.Context, message any) error {
			entered <- struct{}{}

			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
	}

	cellA := newTestActorCell("actor-a", createBlockingActor(), 10)
	cellB := newTestActorCell("actor-b", createBlockingActor(), 10)

	if err := dispatcher.Register(cellA); err != nil {
		t.Fatalf("register actor A: %v", err)
	}

	if err := dispatcher.Register(cellB); err != nil {
		t.Fatalf("register actor B: %v", err)
	}

	if err := cellA.Ref.Tell(context.Background(), "A"); err != nil {
		t.Fatalf("send to actor A: %v", err)
	}

	if err := cellB.Ref.Tell(context.Background(), "B"); err != nil {
		t.Fatalf("send to actor B: %v", err)
	}

	// Both actors must enter Receive before either is released.
	for i := 0; i < 2; i++ {
		select {
		case <-entered:
		case <-time.After(3 * time.Second):
			close(release)
			t.Fatal("workers did not execute actors concurrently")
		}
	}

	close(release)

	if err := dispatcher.Unregister("actor-a"); err != nil {
		t.Fatalf("unregister actor A: %v", err)
	}

	if err := dispatcher.Unregister("actor-b"); err != nil {
		t.Fatalf("unregister actor B: %v", err)
	}
}

func TestWorkStealingDispatcherMessageBudget(t *testing.T) {
	dispatcher := NewWorkStealingDispatcher(&WorkStealingConfig{
		MaxWorkers:       1,
		MaxMessageBudget: 2,
	})

	defer dispatcher.Shutdown(context.Background())

	var processed atomic.Int64

	actor := testActorFunc(func(ctx context.Context, message any) error {
		processed.Add(1)
		return nil
	})

	cell := newTestActorCell("actor-1", actor, 100)

	if err := dispatcher.Register(cell); err != nil {
		t.Fatalf("registration failed: %v", err)
	}

	for i := 0; i < 10; i++ {
		if err := cell.Ref.Tell(context.Background(), i); err != nil {
			t.Fatalf("send failed: %v", err)
		}
	}

	if err := dispatcher.Unregister("actor-1"); err != nil {
		t.Fatalf("unregister failed: %v", err)
	}

	if got := processed.Load(); got != 10 {
		t.Fatalf("expected 10 processed messages, got %d", got)
	}
}

func TestWorkStealingDispatcherGracefulUnregister(t *testing.T) {
	dispatcher := NewWorkStealingDispatcher(&WorkStealingConfig{
		MaxWorkers:       2,
		MaxMessageBudget: 3,
	})

	defer dispatcher.Shutdown(context.Background())

	var processed atomic.Int64

	actor := testActorFunc(func(ctx context.Context, message any) error {
		time.Sleep(time.Millisecond)
		processed.Add(1)
		return nil
	})

	cell := newTestActorCell("actor-1", actor, 100)

	if err := dispatcher.Register(cell); err != nil {
		t.Fatalf("registration failed: %v", err)
	}

	const messageCount = 40

	for i := 0; i < messageCount; i++ {
		if err := cell.Ref.Tell(context.Background(), i); err != nil {
			t.Fatalf("send failed: %v", err)
		}
	}

	if err := dispatcher.Unregister("actor-1"); err != nil {
		t.Fatalf("unregister failed: %v", err)
	}

	if got := processed.Load(); got != messageCount {
		t.Fatalf("expected %d processed messages, got %d",
			messageCount, got)
	}

	if err := cell.Ref.Tell(context.Background(), "after unregister"); !errors.Is(err, ErrActorStopped) {
		t.Fatalf("expected ErrActorStopped after unregister, got %v", err)
	}
}

func TestWorkStealingDispatcherGracefulShutdown(t *testing.T) {
	dispatcher := NewWorkStealingDispatcher(&WorkStealingConfig{
		MaxWorkers:       4,
		MaxMessageBudget: 5,
	})

	var processed atomic.Int64

	const actorCount = 10
	const messagesPerActor = 20

	for i := 0; i < actorCount; i++ {
		actor := testActorFunc(func(ctx context.Context, message any) error {
			processed.Add(1)
			return nil
		})

		cell := newTestActorCell(fmt.Sprintf("actor-%d", i), actor, 100)

		if err := dispatcher.Register(cell); err != nil {
			t.Fatalf("register actor: %v", err)
		}

		for j := 0; j < messagesPerActor; j++ {
			if err := cell.Ref.Tell(context.Background(), j); err != nil {
				t.Fatalf("send message: %v", err)
			}
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := dispatcher.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown failed: %v", err)
	}

	expected := int64(actorCount * messagesPerActor)

	if got := processed.Load(); got != expected {
		t.Fatalf("expected %d processed messages, got %d", expected, got)
	}
}

func TestWorkStealingDispatcherShutdownRejectsRegistration(t *testing.T) {
	dispatcher := NewWorkStealingDispatcher(&WorkStealingConfig{
		MaxWorkers: 2,
	})

	if err := dispatcher.Shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown failed: %v", err)
	}

	actor := testActorFunc(func(context.Context, any) error {
		return nil
	})

	cell := newTestActorCell("actor-1", actor, 10)

	err := dispatcher.Register(cell)

	if !errors.Is(err, ErrDispatcherClosed) {
		t.Fatalf("expected ErrDispatcherClosed, got %v", err)
	}
}

func TestWorkStealingDispatcherPanicIsolation(t *testing.T) {
	dispatcher := NewWorkStealingDispatcher(&WorkStealingConfig{
		MaxWorkers:       1,
		MaxMessageBudget: 1,
	})

	defer dispatcher.Shutdown(context.Background())

	panicActor := testActorFunc(func(ctx context.Context, message any) error {
		panic("intentional test panic")
	})

	var healthyProcessed atomic.Int64

	healthyActor := testActorFunc(func(ctx context.Context, message any) error {
		healthyProcessed.Add(1)
		return nil
	})

	panicCell := newTestActorCell("panic-actor", panicActor, 10)
	healthyCell := newTestActorCell("healthy-actor", healthyActor, 10)

	if err := dispatcher.Register(panicCell); err != nil {
		t.Fatalf("register panic actor: %v", err)
	}

	if err := dispatcher.Register(healthyCell); err != nil {
		t.Fatalf("register healthy actor: %v", err)
	}

	if err := panicCell.Ref.Tell(context.Background(), "panic"); err != nil {
		t.Fatalf("send to panic actor: %v", err)
	}

	if err := healthyCell.Ref.Tell(context.Background(), "healthy"); err != nil {
		t.Fatalf("send to healthy actor: %v", err)
	}

	waitFor(t, 3*time.Second, func() bool {
		return healthyProcessed.Load() == 1
	})

	if got := healthyProcessed.Load(); got != 1 {
		t.Fatalf("expected healthy actor to process one message, got %d", got)
	}

	if err := healthyCell.Ref.Tell(context.Background(), "another"); err != nil {
		t.Fatalf("worker did not survive actor panic: %v", err)
	}

	if err := dispatcher.Unregister("healthy-actor"); err != nil {
		t.Fatalf("unregister healthy actor: %v", err)
	}

	if got := healthyProcessed.Load(); got != 2 {
		t.Fatalf("expected 2 healthy messages, got %d", got)
	}
}

func TestWorkStealingDispatcherConcurrentSendAndShutdown(t *testing.T) {
	dispatcher := NewWorkStealingDispatcher(&WorkStealingConfig{
		MaxWorkers:       4,
		MaxMessageBudget: 10,
	})

	var accepted atomic.Int64
	var processed atomic.Int64

	actor := testActorFunc(func(ctx context.Context, message any) error {
		processed.Add(1)
		return nil
	})

	cell := newTestActorCell("actor-1", actor, 2000)

	if err := dispatcher.Register(cell); err != nil {
		t.Fatalf("registration failed: %v", err)
	}

	const senderCount = 10
	const messagesPerSender = 100

	var sendWG sync.WaitGroup

	for i := 0; i < senderCount; i++ {
		sendWG.Add(1)

		go func(senderID int) {
			defer sendWG.Done()

			for j := 0; j < messagesPerSender; j++ {
				err := cell.Ref.Tell(
					context.Background(),
					fmt.Sprintf("%d-%d", senderID, j),
				)

				if err == nil {
					accepted.Add(1)
					continue
				}

				if errors.Is(err, ErrActorStopped) {
					return
				}

				t.Errorf("unexpected send error: %v", err)
				return
			}
		}(i)
	}

	// Wait for all senders before initiating shutdown.
	sendWG.Wait()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := dispatcher.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown failed: %v", err)
	}

	if got, want := processed.Load(), accepted.Load(); got != want {
		t.Fatalf("processed %d messages, but %d were accepted", got, want)
	}
}

func TestWorkStealingDispatcherConcurrentUnregister(t *testing.T) {
	dispatcher := NewWorkStealingDispatcher(&WorkStealingConfig{
		MaxWorkers:       2,
		MaxMessageBudget: 5,
	})

	defer dispatcher.Shutdown(context.Background())

	actor := testActorFunc(func(ctx context.Context, message any) error {
		time.Sleep(time.Millisecond)
		return nil
	})

	cell := newTestActorCell("actor-1", actor, 100)

	if err := dispatcher.Register(cell); err != nil {
		t.Fatalf("registration failed: %v", err)
	}

	for i := 0; i < 50; i++ {
		if err := cell.Ref.Tell(context.Background(), i); err != nil {
			t.Fatalf("send failed: %v", err)
		}
	}

	var wg sync.WaitGroup
	errs := make(chan error, 2)

	for i := 0; i < 2; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()
			errs <- dispatcher.Unregister("actor-1")
		}()
	}

	wg.Wait()
	close(errs)

	successes := 0

	for err := range errs {
		if err == nil {
			successes++
			continue
		}

		// The task stays registered until finish closes task.done, so a
		// second Unregister can observe the actor and also succeed.
		if !errors.Is(err, ErrActorNotFound) {
			t.Errorf("unexpected unregister error: %v", err)
		}
	}

	if successes < 1 {
		t.Fatalf("expected at least one successful unregister, got %d", successes)
	}
}

func TestWorkStealingDispatcherDefaultConfiguration(t *testing.T) {
	dispatcher := NewWorkStealingDispatcher(nil)

	defer dispatcher.Shutdown(context.Background())

	if dispatcher.config.MaxWorkers != runtime.NumCPU() {
		t.Fatalf(
			"expected %d workers, got %d",
			runtime.NumCPU(),
			dispatcher.config.MaxWorkers,
		)
	}

	if dispatcher.config.MaxMessageBudget != 0 {
		t.Fatalf(
			"expected unset message budget to stay 0, got %d",
			dispatcher.config.MaxMessageBudget,
		)
	}
}
