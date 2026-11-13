package actor

import (
	"context"
	"time"
)

type ActorRef struct {
	id      string
	mailbox *Mailbox
	system  *System
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

// MailboxDepth returns the current number of queued messages waiting in the actor's mailbox.
func (r *ActorRef) MailboxDepth() int {
	if r == nil || r.mailbox == nil {
		return 0
	}
	return r.mailbox.Len()
}

// Tell asynchronously enqueues a message for the actor.
func (reference *ActorRef) Tell(ctx context.Context, message Message) error {
	if reference == nil {
		return ErrActorStopped
	}
	if reference.mailbox == nil {
		if reference.system != nil && message != nil {
			reference.system.PublishDeadLetter(DeadLetter{
				RecipientID: reference.id,
				Message:     message,
				Reason:      ErrActorStopped,
				Timestamp:   time.Now(),
			})
		}
		return ErrActorStopped
	}
	if ctx == nil {
		return ErrNilContext
	}
	if message == nil {
		return ErrInvalidMessage
	}

	err := reference.mailbox.Send(ctx, message)
	if err != nil && reference.system != nil {
		reference.system.PublishDeadLetter(DeadLetter{
			RecipientID: reference.id,
			Message:     message,
			Reason:      err,
			Timestamp:   time.Now(),
		})
	}
	return err
}
