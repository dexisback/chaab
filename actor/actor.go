//defines what an actor is and its behaviour contract

package actor

import "context"

//actor represents an independent computational entitiy that processes messages sequentially

type Actor interface {
	Receive(ctx context.Context, message any) error //the runtime invokes this wheneve the actor processes a msg, context.Context allows cancellations/deadlins and graceful shutdown

}

// ActorFactory constructs a fresh instance of an Actor.
// It is invoked at actor creation and upon each supervised restart
// to ensure internal state mutations from failures are cleanly discarded.
type ActorFactory func() (Actor, error)

//we intentionally not write mailbox/goroutines/channels/scheduling logic here. these belong to the runtime, not individual actors
