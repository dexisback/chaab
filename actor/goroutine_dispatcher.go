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

// GoroutineDispatcher executes each registered actor on its own dedicated goroutine.
type GoroutineDispatcher struct {
	mu      sync.Mutex
	workers map[string]*actorWorker
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
		cancel: cancel,
		done:   make(chan struct{}),
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
	defer d.wg.Done()
	defer close(worker.done)
	defer func() {
		if r := recover(); r != nil {
			slog.Error(
				"actor panicked while processing a message",
				"actor_id", cell.Ref.ID(),
				"panic", r,
			)
		}
	}()

	for {
		message, err := mailbox.Receive(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			slog.Error("mailbox receive failed", "actor_id", cell.Ref.ID(), "error", err)
			continue
		}

		if ctx.Err() != nil {
			return
		}

		if err := cell.Actor.Receive(ctx, message); err != nil {
			slog.Error(
				"actor message processing failed",
				"actor_id", cell.Ref.ID(),
				"error", err,
			)
		}
	}
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

	worker.cancel()
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
		for _, worker := range d.workers {
			worker.cancel()
		}
		d.workers = make(map[string]*actorWorker)
		go func() {
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
