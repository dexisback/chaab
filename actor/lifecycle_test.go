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

type resourceManagingActor struct {
	initialized atomic.Bool
	cleanedUp   atomic.Bool
	preStarts   atomic.Int32
	postStops   atomic.Int32
}

func (a *resourceManagingActor) PreStart(ctx context.Context) error {
	a.preStarts.Add(1)
	a.initialized.Store(true)
	return nil
}

func (a *resourceManagingActor) PostStop(ctx context.Context) error {
	a.postStops.Add(1)
	a.cleanedUp.Store(true)
	return nil
}

func (a *resourceManagingActor) Receive(ctx context.Context, message any) error {
	str, ok := message.(string)
	if ok && str == "panic" {
		panic("lifecycle panic")
	}
	return nil
}

type failingPreStartActor struct{}

func (f *failingPreStartActor) PreStart(ctx context.Context) error {
	return errors.New("simulated db connection failure in PreStart")
}

func (f *failingPreStartActor) Receive(ctx context.Context, message any) error {
	return nil
}

func TestLifecycle_PreStartCalledOnCreation(t *testing.T) {
	sys := actor.NewSystem()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = sys.Shutdown(ctx)
	})

	act := &resourceManagingActor{}
	ref, err := sys.CreateActor(act)
	if err != nil {
		t.Fatalf("create actor failed: %v", err)
	}

	if !act.initialized.Load() {
		t.Fatal("expected PreStart to be called before actor is returned")
	}
	if act.preStarts.Load() != 1 {
		t.Fatalf("expected preStarts == 1, got %d", act.preStarts.Load())
	}

	_ = ref
}

func TestLifecycle_PreStartFailureAbortsCreation(t *testing.T) {
	sys := actor.NewSystem()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = sys.Shutdown(ctx)
	})

	act := &failingPreStartActor{}
	_, err := sys.CreateActor(act)
	if err == nil {
		t.Fatal("expected error from CreateActor when PreStart fails, got nil")
	}
}

func TestLifecycle_PostStopCalledOnStop(t *testing.T) {
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

			act := &resourceManagingActor{}
			ref, err := sys.CreateActor(act)
			if err != nil {
				t.Fatalf("create actor failed: %v", err)
			}

			// PreStart must have executed
			if !act.initialized.Load() {
				t.Fatal("expected PreStart to have executed")
			}

			// Stop actor
			if err := sys.Stop(ref.ID()); err != nil {
				t.Fatalf("stop actor failed: %v", err)
			}

			// PostStop must have executed
			if !act.cleanedUp.Load() {
				t.Fatal("expected PostStop to have executed on Stop")
			}
			if act.postStops.Load() != 1 {
				t.Fatalf("expected postStops == 1, got %d", act.postStops.Load())
			}
		})
	}
}

func TestLifecycle_HooksOnSupervisedRestart(t *testing.T) {
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

			strategy := actor.NewOneForOneStrategy(3, time.Second, nil)
			var instMu sync.Mutex
			var instances []*resourceManagingActor

			ref, err := sys.CreateActorFromFactory(func() (actor.Actor, error) {
				act := &resourceManagingActor{}
				instMu.Lock()
				instances = append(instances, act)
				instMu.Unlock()
				return act, nil
			}, actor.WithSupervisor(strategy))
			if err != nil {
				t.Fatalf("create actor: %v", err)
			}

			ctx := context.Background()

			// Trigger crash and supervised restart
			_ = ref.Tell(ctx, "panic")

			// Send follow-up message to ensure replacement is running
			_ = ref.Tell(ctx, "healthy")
			time.Sleep(50 * time.Millisecond)

			instMu.Lock()
			count := len(instances)
			var instance1, instance2 *resourceManagingActor
			if count == 2 {
				instance1 = instances[0]
				instance2 = instances[1]
			}
			instMu.Unlock()

			if count != 2 {
				t.Fatalf("expected 2 instances created (initial + restart), got %d", count)
			}

			// Instance 1: started and stopped (upon restart)
			if instance1.preStarts.Load() != 1 {
				t.Fatalf("instance 1 preStarts expected 1, got %d", instance1.preStarts.Load())
			}
			if instance1.postStops.Load() != 1 {
				t.Fatalf("instance 1 postStops expected 1, got %d", instance1.postStops.Load())
			}

			// Instance 2: started (fresh), not yet stopped
			if instance2.preStarts.Load() != 1 {
				t.Fatalf("instance 2 preStarts expected 1, got %d", instance2.preStarts.Load())
			}
			if instance2.postStops.Load() != 0 {
				t.Fatalf("instance 2 postStops expected 0, got %d", instance2.postStops.Load())
			}

			// Clean teardown
			_ = sys.Stop(ref.ID())

			// Now instance 2 must be stopped
			if instance2.postStops.Load() != 1 {
				t.Fatalf("instance 2 postStops expected 1 after final Stop, got %d", instance2.postStops.Load())
			}
		})
	}
}
