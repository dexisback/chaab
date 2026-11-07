package actor

import (
	"context"
	"sync"
)

type Mailbox struct {
	messages  chan Message
	mu        sync.RWMutex
	closed    bool
	done      chan struct{}
	closeDone chan struct{}
	senders   sync.WaitGroup
}

// NewMailBox creates a new mailbox with the given capacity.
func NewMailBox(capacity int) *Mailbox {
	return &Mailbox{
		messages:  make(chan Message, capacity),
		done:      make(chan struct{}),
		closeDone: make(chan struct{}),
	}
}

// Send adds a message to the mailbox. It blocks if the mailbox is full until ctx is cancelled or mailbox is closed.
func (m *Mailbox) Send(ctx context.Context, message Message) error {
	if ctx == nil {
		return ErrNilContext
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if message == nil {
		return ErrInvalidMessage
	}

	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return ErrActorStopped
	}

	m.senders.Add(1)
	m.mu.Unlock()

	defer m.senders.Done()

	select {
	case m.messages <- message:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-m.done:
		return ErrActorStopped
	}
}

// Receive retrieves the next message from the mailbox. It blocks until a message is available or ctx is cancelled.
// Once closed and drained of all buffered messages, it returns ErrActorStopped.
func (m *Mailbox) Receive(ctx context.Context) (Message, error) {
	if ctx == nil {
		return nil, ErrNilContext
	}

	select {
	case message, ok := <-m.messages:
		if !ok {
			return nil, ErrActorStopped
		}
		return message, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Close closes the mailbox safely. Future sends return ErrActorStopped.
// In-progress sends finish or abort. Receive continues draining until empty.
// Close is safe for concurrent use and idempotent.
func (m *Mailbox) Close() {
	m.mu.Lock()
	if m.closed {
		closeDone := m.closeDone
		m.mu.Unlock()
		<-closeDone
		return
	}

	m.closed = true
	close(m.done)
	m.mu.Unlock()

	m.senders.Wait()
	close(m.messages)
	close(m.closeDone)
}
