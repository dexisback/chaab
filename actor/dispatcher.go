package actor

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
)

// ActorCell represents an actor instance managed by the runtime.
// It bundles the actor implementation, its addressable reference,
// its mailbox, and supervision configuration for consumption by a Dispatcher.
type ActorCell struct {
	mu         sync.RWMutex
	Actor      Actor
	Ref        *ActorRef
	Mailbox    *Mailbox
	Factory    ActorFactory
	Supervisor Supervisor
}

// CurrentActor returns the active actor instance safely.
func (c *ActorCell) CurrentActor() Actor {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.Actor
}

// HandleFailure delegates failure classification to the configured Supervisor,
// or applies default fallback directives if no supervisor is attached.
func (c *ActorCell) HandleFailure(ctx context.Context, failure Failure) Directive {
	c.mu.RLock()
	supervisor := c.Supervisor
	c.mu.RUnlock()

	if supervisor != nil {
		return supervisor.HandleFailure(ctx, failure)
	}

	// Default behavior when unsupervised:
	if failure.Panic != nil {
		return DirectiveStop
	}
	return DirectiveResume
}

// InvokePostStop safely executes PostStop on the current actor instance if implemented.
func (c *ActorCell) InvokePostStop(ctx context.Context) error {
	c.mu.RLock()
	act := c.Actor
	c.mu.RUnlock()

	if stopper, ok := act.(PostStopper); ok {
		defer func() {
			if r := recover(); r != nil {
				slog.Error("actor panicked in PostStop", "actor_id", c.Ref.ID(), "panic", r)
			}
		}()
		return stopper.PostStop(ctx)
	}
	return nil
}

// Restart constructs a replacement actor using the retained factory,
// safely swapping the instance while preserving the mailbox and ActorRef.
func (c *ActorCell) Restart() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.Factory == nil {
		return fmt.Errorf("cannot restart actor %s: no factory configured", c.Ref.ID())
	}

	// Clean up old actor resources if implemented
	if stopper, ok := c.Actor.(PostStopper); ok {
		func() {
			defer func() {
				if r := recover(); r != nil {
					slog.Error("actor panicked in PostStop during restart", "actor_id", c.Ref.ID(), "panic", r)
				}
			}()
			_ = stopper.PostStop(context.Background())
		}()
	}

	newActor, err := c.Factory()
	if err != nil {
		return fmt.Errorf("actor factory failed during restart: %w", err)
	}
	if newActor == nil {
		return ErrNilActor
	}

	// Initialize new actor resources if implemented
	if starter, ok := newActor.(PreStarter); ok {
		if initErr := starter.PreStart(context.Background()); initErr != nil {
			return fmt.Errorf("new actor PreStart failed during restart: %w", initErr)
		}
	}

	c.Actor = newActor
	return nil
}

// Dispatcher manages actor execution and registration.
// All execution strategies implement this common interface.
type Dispatcher interface {
	Register(cell *ActorCell) error
	Unregister(id string) error
	Shutdown(ctx context.Context) error
}
