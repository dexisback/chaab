package actor

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
)

type actorWorker struct {
	cancel context.CancelFunc
	done   chan struct{}
}

// GoroutineDispatcher manages actor execution using a Scheduler.
type GoroutineDispatcher struct {
	mu        sync.Mutex
	workers   map[string]*actorWorker
	scheduler Scheduler
	closed    bool
}

// NewGoroutineDispatcher creates a dispatcher using the supplied scheduler.
func NewGoroutineDispatcher(scheduler Scheduler) *GoroutineDispatcher {
	return &GoroutineDispatcher{
		workers:   make(map[string]*actorWorker),
		scheduler: scheduler,
	}
}

func (d *GoroutineDispatcher) Register(cell *actorCell) error {
	if cell == nil || cell.actor == nil || cell.ref == nil {
		return ErrInvalidActorCell
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	if d.closed {
		return ErrDispatcherClosed
	}

	id := cell.ref.id

	if _, exists := d.workers[id]; exists {
		return fmt.Errorf("%w: id=%s", ErrActorAlreadyExists, id)
	}

	ctx, cancel := context.WithCancel(context.Background())
	worker := &actorWorker{
		cancel: cancel,
		done:   make(chan struct{}),
	}

	d.workers[id] = worker

	task := func() {
		defer close(worker.done)

		for {
			message, err := cell.ref.mailbox.Receive(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}

				slog.Error("mailbox receive failed", "error", err)
				continue
			}

			if ctx.Err() != nil {
				return
			}

			if err := cell.actor.Receive(ctx, message); err != nil {
				slog.Error(
					"actor message processing failed",
					"actor_id", id,
					"error", err,
				)
			}
		}
	}

	if err := d.scheduler.Schedule(ctx, task); err != nil {
		delete(d.workers, id)
		cancel()
		return fmt.Errorf("%w: id=%s: %w", ErrScheduleTask, id, err)
	}

	return nil
}

func (d *GoroutineDispatcher) Unregister(id string) error {
	d.mu.Lock()

	worker, exists := d.workers[id]
	if !exists {
		d.mu.Unlock()
		return fmt.Errorf("%w: id=%s", ErrActorNotFound, id)
	}

	delete(d.workers, id)
	worker.cancel()

	d.mu.Unlock()

	<-worker.done
	return nil
}

func (d *GoroutineDispatcher) Shutdown(ctx context.Context) error {
	d.mu.Lock()

	d.closed = true

	for _, worker := range d.workers {
		worker.cancel()
	}

	d.workers = make(map[string]*actorWorker)

	d.mu.Unlock()

	return d.scheduler.Shutdown(ctx)
}