package actor

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

type actorWorker struct {
	cancel  context.CancelFunc
	done    chan struct{}
	mailbox *Mailbox
}

// GoroutineDispatcher executes each registered actor on its own dedicated goroutine.
type GoroutineDispatcher struct {
	mu      sync.Mutex
	workers map[string]*actorWorker
	metrics atomic.Pointer[metricsHolder]
	wg      sync.WaitGroup
	closed  bool
	done    chan struct{}
}

// NewGoroutineDispatcher creates a new GoroutineDispatcher.
func NewGoroutineDispatcher() *GoroutineDispatcher {
	return &GoroutineDispatcher{
		workers: make(map[string]*actorWorker),
		done:    make(chan struct{}),
	}
}

// SetMetrics configures the telemetry metrics collector for the dispatcher.
func (d *GoroutineDispatcher) SetMetrics(m MetricsCollector) {
	setMetricsHolder(&d.metrics, m)
}

// Register registers an actor with the dispatcher and starts its dedicated execution goroutine.
func (d *GoroutineDispatcher) Register(cell *ActorCell) error {
	if cell == nil || cell.Actor == nil || cell.Ref == nil {
		return ErrInvalidActorCell
	}

	mailbox := cell.Mailbox
	if mailbox == nil && cell.Ref != nil {
		mailbox = cell.Ref.mailbox
	}
	if mailbox == nil {
		return ErrInvalidActorCell
	}

	id := cell.Ref.ID()
	if id == "" {
		return ErrInvalidActorCell
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	if d.closed {
		return ErrDispatcherClosed
	}

	if _, exists := d.workers[id]; exists {
		return fmt.Errorf("%w: id=%s", ErrActorAlreadyExists, id)
	}

	ctx, cancel := context.WithCancel(context.Background())
	worker := &actorWorker{
		cancel:  cancel,
		done:    make(chan struct{}),
		mailbox: mailbox,
	}

	d.workers[id] = worker
	d.wg.Add(1)

	go d.run(ctx, cell, mailbox, worker)

	return nil
}

// run is the dedicated execution loop for a single actor.
// It sequentially reads messages from the mailbox and invokes the actor's Receive method.
func (d *GoroutineDispatcher) run(
	ctx context.Context,
	cell *ActorCell,
	mailbox *Mailbox,
	worker *actorWorker,
) {
	id := cell.Ref.ID()
	defer worker.cancel()
	defer mailbox.Close()
	defer d.wg.Done()
	defer close(worker.done)
	defer cell.InvokePostStop(context.Background())
	defer func() {
		if r := recover(); r != nil {
			slog.Error(
				"actor panicked while processing a message",
				"actor_id", id,
				"panic", r,
			)
		}
		d.mu.Lock()
		delete(d.workers, id)
		d.mu.Unlock()
	}()

	for {
		message, err := mailbox.Receive(ctx)
		if err != nil {
			if errors.Is(err, ErrActorStopped) || ctx.Err() != nil {
				return
			}
			slog.Error("mailbox receive failed", "actor_id", id, "error", err)
			continue
		}

		if ctx.Err() != nil {
			return
		}

		if shouldStop := d.invoke(ctx, cell, message); shouldStop {
			return
		}
	}
}

func (d *GoroutineDispatcher) invoke(
	ctx context.Context,
	cell *ActorCell,
	message Message,
) (shouldStop bool) {
	var panicked bool
	var panicVal any
	var receiveErr error

	actorID := ""
	if cell != nil && cell.Ref != nil {
		actorID = cell.Ref.ID()
	}

	start := time.Now()

	func() {
		defer func() {
			if r := recover(); r != nil {
				panicked = true
				panicVal = r
				slog.Error(
					"actor panicked while processing a message",
					"actor_id", actorID,
					"panic", r,
				)
			}
		}()

		if err := cell.CurrentActor().Receive(ctx, message); err != nil {
			receiveErr = err
			slog.Error(
				"actor message processing failed",
				"actor_id", actorID,
				"error", err,
			)
			failure := Failure{ActorID: actorID, Err: err}
			directive := cell.HandleFailure(ctx, failure)
			if directive == DirectiveRestart {
				if restartErr := cell.Restart(); restartErr != nil {
					slog.Error("restart failed", "actor_id", actorID, "error", restartErr)
					shouldStop = true
				} else {
					resolveMetrics(&d.metrics, cell).ActorRestarted(actorID)
				}
			} else if directive == DirectiveStop {
				shouldStop = true
			}
		}
	}()

	duration := time.Since(start)

	if panicked {
		panicErr := fmt.Errorf("panic: %v", panicVal)
		failure := Failure{ActorID: actorID, Panic: panicVal}
		directive := cell.HandleFailure(ctx, failure)
		if directive == DirectiveRestart {
			if restartErr := cell.Restart(); restartErr != nil {
				slog.Error("restart failed", "actor_id", actorID, "error", restartErr)
				resolveMetrics(&d.metrics, cell).MessageProcessed(actorID, duration, panicErr)
				return true
			}
			resolveMetrics(&d.metrics, cell).ActorRestarted(actorID)
			resolveMetrics(&d.metrics, cell).MessageProcessed(actorID, duration, panicErr)
			return false
		}
		resolveMetrics(&d.metrics, cell).MessageProcessed(actorID, duration, panicErr)
		return true
	}

	resolveMetrics(&d.metrics, cell).MessageProcessed(actorID, duration, receiveErr)
	return shouldStop
}

// Unregister stops the actor's goroutine and removes it from the dispatcher.
func (d *GoroutineDispatcher) Unregister(id string) error {
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return ErrDispatcherClosed
	}

	worker, exists := d.workers[id]
	if !exists {
		d.mu.Unlock()
		return fmt.Errorf("%w: id=%s", ErrActorNotFound, id)
	}

	delete(d.workers, id)
	d.mu.Unlock()

	worker.mailbox.Close()
	<-worker.done
	return nil
}

// Shutdown stops all actors and waits for all actor goroutines to exit or ctx to expire.
func (d *GoroutineDispatcher) Shutdown(ctx context.Context) error {
	if ctx == nil {
		return ErrNilContext
	}

	d.mu.Lock()
	if !d.closed {
		d.closed = true

		workers := make([]*actorWorker, 0, len(d.workers))
		for _, worker := range d.workers {
			workers = append(workers, worker)
		}
		d.workers = make(map[string]*actorWorker)

		go func() {
			for _, worker := range workers {
				worker.mailbox.Close()
			}
			d.wg.Wait()
			close(d.done)
		}()
	}
	done := d.done
	d.mu.Unlock()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
