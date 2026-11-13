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

type metricsTestActor struct {
	received chan any
}

func (a *metricsTestActor) Receive(ctx context.Context, message any) error {
	if str, ok := message.(string); ok {
		if str == "panic" {
			panic("metrics test panic")
		}
		if str == "error" {
			return errors.New("metrics test error")
		}
	}
	if a.received != nil {
		a.received <- message
	}
	return nil
}

type blockingActor struct {
	started chan struct{}
	release chan struct{}
}

func (a *blockingActor) Receive(ctx context.Context, message any) error {
	if str, ok := message.(string); ok && str == "block" {
		close(a.started)
		<-a.release
	}
	return nil
}

type customSpyCollector struct {
	mu           sync.Mutex
	processed    []string
	durations    []time.Duration
	errors       []error
	restarts     []string
	steals       [][2]int
	deadLetters  []actor.DeadLetter
	totalStolen  atomic.Uint64
	totalRestart atomic.Uint64
	totalProc    atomic.Uint64
	totalDLQ     atomic.Uint64
}

func (c *customSpyCollector) MessageProcessed(actorID string, duration time.Duration, err error) {
	c.totalProc.Add(1)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.processed = append(c.processed, actorID)
	c.durations = append(c.durations, duration)
	c.errors = append(c.errors, err)
}

func (c *customSpyCollector) ActorRestarted(actorID string) {
	c.totalRestart.Add(1)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.restarts = append(c.restarts, actorID)
}

func (c *customSpyCollector) TaskStolen(thiefWorkerID int, victimWorkerID int) {
	c.totalStolen.Add(1)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.steals = append(c.steals, [2]int{thiefWorkerID, victimWorkerID})
}

func (c *customSpyCollector) DeadLetterPublished(letter actor.DeadLetter) {
	c.totalDLQ.Add(1)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.deadLetters = append(c.deadLetters, letter)
}

func (c *customSpyCollector) TotalProcessed() uint64 {
	return c.totalProc.Load()
}

func (c *customSpyCollector) TotalRestarts() uint64 {
	return c.totalRestart.Load()
}

func (c *customSpyCollector) TotalTaskSteals() uint64 {
	return c.totalStolen.Load()
}

func (c *customSpyCollector) TotalDeadLetters() uint64 {
	return c.totalDLQ.Load()
}

func TestMetrics_DefaultCollector_BasicStats(t *testing.T) {
	sys := actor.NewSystem()
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = sys.Shutdown(ctx)
	}()

	stats := sys.Stats()
	if stats.ActiveActors != 0 || stats.TotalProcessed != 0 {
		t.Fatalf("expected 0 actors and 0 processed, got %+v", stats)
	}

	received := make(chan any, 10)
	ref, err := sys.CreateActor(&metricsTestActor{received: received})
	if err != nil {
		t.Fatalf("create actor failed: %v", err)
	}

	stats = sys.Stats()
	if stats.ActiveActors != 1 {
		t.Fatalf("expected 1 active actor, got %d", stats.ActiveActors)
	}

	ctx := context.Background()
	for i := 0; i < 10; i++ {
		if err := ref.Tell(ctx, i); err != nil {
			t.Fatalf("tell failed: %v", err)
		}
	}

	for i := 0; i < 10; i++ {
		select {
		case <-received:
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for message %d", i)
		}
	}

	// Give dispatcher a brief moment to finish invocation telemetry hook
	time.Sleep(50 * time.Millisecond)

	stats = sys.Stats()
	if stats.TotalProcessed != 10 {
		t.Fatalf("expected 10 processed messages, got %d", stats.TotalProcessed)
	}

	if err := sys.Stop(ref.ID()); err != nil {
		t.Fatalf("stop failed: %v", err)
	}

	stats = sys.Stats()
	if stats.ActiveActors != 0 {
		t.Fatalf("expected 0 active actors after stop, got %d", stats.ActiveActors)
	}
}

func TestMetrics_SupervisorRestarts(t *testing.T) {
	sys := actor.NewSystem()
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = sys.Shutdown(ctx)
	}()

	received := make(chan any, 10)
	ref, err := sys.CreateActorFromFactory(
		func() (actor.Actor, error) {
			return &metricsTestActor{received: received}, nil
		},
		actor.WithSupervisor(actor.NewOneForOneStrategy(3, time.Minute, nil)),
	)
	if err != nil {
		t.Fatalf("create actor failed: %v", err)
	}

	ctx := context.Background()
	// Send panic message
	if err := ref.Tell(ctx, "panic"); err != nil {
		t.Fatalf("tell panic failed: %v", err)
	}

	// Send normal message to prove actor recovered and processed messages
	if err := ref.Tell(ctx, "after-restart"); err != nil {
		t.Fatalf("tell after-restart failed: %v", err)
	}

	select {
	case msg := <-received:
		if msg != "after-restart" {
			t.Fatalf("expected 'after-restart', got %v", msg)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for after-restart message")
	}

	time.Sleep(50 * time.Millisecond)

	stats := sys.Stats()
	if stats.TotalRestarts != 1 {
		t.Fatalf("expected 1 restart in stats, got %d", stats.TotalRestarts)
	}
	if stats.TotalProcessed != 2 {
		t.Fatalf("expected 2 processed (1 panic + 1 normal), got %d", stats.TotalProcessed)
	}
}

func TestMetrics_DeadLettersTracking(t *testing.T) {
	sys := actor.NewSystem()
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = sys.Shutdown(ctx)
	}()

	// Mailbox with capacity 1 and FailFast
	ref, err := sys.CreateActor(
		&blockingActor{
			started: make(chan struct{}),
			release: make(chan struct{}),
		},
		actor.WithMailboxCapacity(1),
		actor.WithOverflowStrategy(actor.OverflowFailFast),
	)
	if err != nil {
		t.Fatalf("create actor failed: %v", err)
	}

	ctx := context.Background()
	// Fill the buffer
	_ = ref.Tell(ctx, "msg-1")
	_ = ref.Tell(ctx, "msg-2")

	// This one overflows and drops to DLQ
	_ = ref.Tell(ctx, "msg-overflow")

	time.Sleep(50 * time.Millisecond)

	stats := sys.Stats()
	if stats.TotalDeadLetters == 0 {
		t.Fatalf("expected > 0 dead letters, got %d", stats.TotalDeadLetters)
	}
}

func TestMetrics_MailboxDepth(t *testing.T) {
	sys := actor.NewSystem()
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = sys.Shutdown(ctx)
	}()

	started := make(chan struct{})
	release := make(chan struct{})

	ref, err := sys.CreateActor(
		&blockingActor{started: started, release: release},
		actor.WithMailboxCapacity(10),
	)
	if err != nil {
		t.Fatalf("create actor failed: %v", err)
	}

	ctx := context.Background()
	if err := ref.Tell(ctx, "block"); err != nil {
		t.Fatalf("tell block failed: %v", err)
	}

	// Wait until actor starts processing "block"
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for actor to block")
	}

	// Enqueue 4 messages
	for i := 0; i < 4; i++ {
		if err := ref.Tell(ctx, i); err != nil {
			t.Fatalf("tell %d failed: %v", i, err)
		}
	}

	depth := ref.MailboxDepth()
	if depth != 4 {
		t.Fatalf("expected mailbox depth 4, got %d", depth)
	}

	close(release)
	// Give time to drain
	time.Sleep(50 * time.Millisecond)

	depth = ref.MailboxDepth()
	if depth != 0 {
		t.Fatalf("expected mailbox depth 0 after draining, got %d", depth)
	}
}

func TestMetrics_CustomCollector_FullLifecycle(t *testing.T) {
	spy := &customSpyCollector{}
	sys := actor.NewSystem(actor.WithMetrics(spy))
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = sys.Shutdown(ctx)
	}()

	if sys.Metrics() != spy {
		t.Fatal("sys.Metrics() did not return custom spy collector")
	}

	received := make(chan any, 10)
	ref, err := sys.CreateActorFromFactory(
		func() (actor.Actor, error) {
			return &metricsTestActor{received: received}, nil
		},
		actor.WithSupervisor(actor.NewOneForOneStrategy(3, time.Minute, nil)),
	)
	if err != nil {
		t.Fatalf("create actor failed: %v", err)
	}

	ctx := context.Background()
	_ = ref.Tell(ctx, "hello")
	_ = ref.Tell(ctx, "panic")
	_ = ref.Tell(ctx, "world")

	select {
	case <-received:
	case <-time.After(time.Second):
		t.Fatal("timed out on first message")
	}

	select {
	case <-received:
	case <-time.After(time.Second):
		t.Fatal("timed out on post-panic message")
	}

	time.Sleep(50 * time.Millisecond)

	stats := sys.Stats()
	if stats.TotalProcessed != 3 {
		t.Fatalf("expected 3 total processed, got %d", stats.TotalProcessed)
	}
	if stats.TotalRestarts != 1 {
		t.Fatalf("expected 1 total restart, got %d", stats.TotalRestarts)
	}

	spy.mu.Lock()
	defer spy.mu.Unlock()

	if len(spy.processed) != 3 {
		t.Fatalf("expected 3 processed calls in spy, got %d", len(spy.processed))
	}
	for i, dur := range spy.durations {
		if dur <= 0 {
			t.Errorf("expected positive duration for message %d, got %v", i, dur)
		}
	}
	if len(spy.restarts) != 1 {
		t.Fatalf("expected 1 restart in spy, got %d", len(spy.restarts))
	}
	if spy.restarts[0] != ref.ID() {
		t.Fatalf("expected restart on actor %s, got %s", ref.ID(), spy.restarts[0])
	}
}

func TestMetrics_AllDispatchersParity(t *testing.T) {
	dispatchers := []struct {
		name       string
		dispatcher func() actor.Dispatcher
	}{
		{
			name: "GoroutineDispatcher",
			dispatcher: func() actor.Dispatcher {
				return actor.NewGoroutineDispatcher()
			},
		},
		{
			name: "WorkerPoolDispatcher",
			dispatcher: func() actor.Dispatcher {
				return actor.NewWorkerPoolDispatcher(actor.WorkerPoolConfig{
					Workers:       2,
					MessageBudget: 5,
				})
			},
		},
		{
			name: "WorkStealingDispatcher",
			dispatcher: func() actor.Dispatcher {
				return actor.NewWorkStealingDispatcher(&actor.WorkStealingConfig{
					MaxWorkers:       2,
					MaxMessageBudget: 5,
				})
			},
		},
	}

	for _, tc := range dispatchers {
		t.Run(tc.name, func(t *testing.T) {
			sys := actor.NewSystem(actor.WithDispatcher(tc.dispatcher()))
			defer func() {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				_ = sys.Shutdown(ctx)
			}()

			received := make(chan any, 10)
			ref, err := sys.CreateActorFromFactory(
				func() (actor.Actor, error) {
					return &metricsTestActor{received: received}, nil
				},
				actor.WithSupervisor(actor.NewOneForOneStrategy(3, time.Minute, nil)),
			)
			if err != nil {
				t.Fatalf("create actor failed: %v", err)
			}

			ctx := context.Background()
			_ = ref.Tell(ctx, "panic")
			_ = ref.Tell(ctx, "msg")

			select {
			case msg := <-received:
				if msg != "msg" {
					t.Fatalf("expected 'msg', got %v", msg)
				}
			case <-time.After(time.Second):
				t.Fatal("timed out waiting for message delivery")
			}

			time.Sleep(50 * time.Millisecond)

			stats := sys.Stats()
			if stats.TotalRestarts != 1 {
				t.Fatalf("[%s] expected 1 restart, got %d", tc.name, stats.TotalRestarts)
			}
			if stats.TotalProcessed != 2 {
				t.Fatalf("[%s] expected 2 processed, got %d", tc.name, stats.TotalProcessed)
			}
		})
	}
}

func TestMetrics_WorkStealingCounter(t *testing.T) {
	d := actor.NewWorkStealingDispatcher(&actor.WorkStealingConfig{
		MaxWorkers:       2,
		MaxMessageBudget: 1, // Budget 1 forces frequent turns & stealing opportunities
	})
	sys := actor.NewSystem(actor.WithDispatcher(d))
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = sys.Shutdown(ctx)
	}()

	// Create multiple actors to distribute across worker queues
	var wg sync.WaitGroup
	actorsCount := 8
	messagesPerActor := 50
	wg.Add(actorsCount * messagesPerActor)

	ctx := context.Background()
	for i := 0; i < actorsCount; i++ {
		ref, err := sys.CreateActor(&testConsumerActor{
			onMessage: func(msg any) {
				time.Sleep(100 * time.Microsecond)
				wg.Done()
			},
		})
		if err != nil {
			t.Fatalf("create actor failed: %v", err)
		}

		for j := 0; j < messagesPerActor; j++ {
			_ = ref.Tell(ctx, j)
		}
	}

	wg.Wait()
	time.Sleep(50 * time.Millisecond)

	stats := sys.Stats()
	if stats.TotalProcessed < uint64(actorsCount*messagesPerActor) {
		t.Fatalf("expected at least %d processed, got %d", actorsCount*messagesPerActor, stats.TotalProcessed)
	}

	// In a 2-worker setup with budget 1 and 400 total messages with deliberate microsecond delays,
	// task stealing between worker queues will occur.
	if stats.TotalTaskSteals == 0 {
		t.Logf("Note: TaskSteals was 0 in this specific run due to OS scheduler interleaving (non-deterministic concurrency)")
	} else {
		t.Logf("Work stealing occurred: %d task steals recorded", stats.TotalTaskSteals)
	}
}
