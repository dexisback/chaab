package actor

import "context"

// ActorCell represents an actor instance managed by the runtime.
// It bundles the actor implementation, its addressable reference,
// and its mailbox for consumption by a Dispatcher.
type ActorCell struct {
	Actor   Actor
	Ref     *ActorRef
	Mailbox *Mailbox
}

// Dispatcher manages actor execution and registration.
// All execution strategies implement this common interface.
type Dispatcher interface {
	Register(cell *ActorCell) error
	Unregister(id string) error
	Shutdown(ctx context.Context) error
}
