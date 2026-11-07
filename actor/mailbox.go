package actor

import "context"

type Mailbox struct {
	messages chan Message
}

// NewMailBox creates a new mailbox with the given capacity.
func NewMailBox(capacity int) *Mailbox {
	return &Mailbox{
		messages: make(chan Message, capacity),
	}
}

// Send adds a message to the mailbox. It blocks if the mailbox is full until ctx is cancelled.
func (mailbox *Mailbox) Send(ctx context.Context, message Message) error {
	select {
	case mailbox.messages <- message:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Receive retrieves the next message from the mailbox. It blocks until a message is available or ctx is cancelled.
func (m *Mailbox) Receive(ctx context.Context) (Message, error) {
	select {
	case message := <-m.messages:
		return message, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
