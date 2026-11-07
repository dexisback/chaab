package actor

import "context"

type ActorRef struct {
	id      string
	mailbox *Mailbox
}

func (r *ActorRef) ID() string {
	if r == nil {
		return ""
	}
	return r.id
}

func (r *ActorRef) Mailbox() *Mailbox {
	if r == nil {
		return nil
	}
	return r.mailbox
}

// Tell asynchronously enqueues a message for the actor.
func (reference *ActorRef) Tell(ctx context.Context, message Message) error {
	if reference == nil || reference.mailbox == nil {
		return ErrActorStopped
	}
	if ctx == nil {
		return ErrNilContext
	}
	if message == nil {
		return ErrInvalidMessage
	}
	return reference.mailbox.Send(ctx, message)
}
