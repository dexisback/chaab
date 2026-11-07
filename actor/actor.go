//defines what an actor is and its behaviour contract

package actor

import "context"

//actor represents an independent computational entitiy that processes messages sequentially

type Actor interface {
	Receive(ctx context.Context, message any) error //the runtime invokes this wheneve the actor processes a msg, context.Context allows cancellations/deadlins and graceful shutdown

}

//we intentionally not write mailbox/goroutines/channels/scheduling logic here. these belong to the runtime, not individual actors
