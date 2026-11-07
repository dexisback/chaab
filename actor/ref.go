package actor

import "context"

type ActorRef struct {
	id      string
	mailbox *Mailbox
}

func (r *ActorRef) ID() string {
	return r.id
}

func (r *ActorRef) Mailbox() *Mailbox {
	return r.mailbox
}

// Tell asynchronously enqueues a message for the actor.
func (reference *ActorRef) Tell(ctx context.Context, message Message) error {
	return reference.mailbox.Send(ctx, message)
}
