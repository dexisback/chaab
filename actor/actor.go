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

// PreStarter is an optional interface implemented by actors requiring resource
// allocation or initialization (e.g. database pools, tickers) before receiving messages.
type PreStarter interface {
	PreStart(ctx context.Context) error
}

// PostStopper is an optional interface implemented by actors requiring resource
// cleanup (e.g. closing sockets, stopping timers) when stopped or replaced.
type PostStopper interface {
	PostStop(ctx context.Context) error
}

//we intentionally not write mailbox/goroutines/channels/scheduling logic here. these belong to the runtime, not individual actors
