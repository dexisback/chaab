package actor

import (
	"context"
	"fmt"
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

// Restart constructs a replacement actor using the retained factory,
// safely swapping the instance while preserving the mailbox and ActorRef.
func (c *ActorCell) Restart() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.Factory == nil {
		return fmt.Errorf("cannot restart actor %s: no factory configured", c.Ref.ID())
	}

	newActor, err := c.Factory()
	if err != nil {
		return fmt.Errorf("actor factory failed during restart: %w", err)
	}
	if newActor == nil {
		return ErrNilActor
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
