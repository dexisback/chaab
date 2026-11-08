package actor

import (
	"context"
	"sync"
)

type Mailbox struct {
	messages  chan Message
	mu        sync.RWMutex
	notifier  func()
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
		m.notify() //the dispatcher now gets notifier when a msg is succesfully enqueued
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
	m.notify() //the dispatcher now gets notifier when the mailbox is closed
	close(m.closeDone)
}

func (m *Mailbox) tryReceive() (Message, bool, error) {
	select {
	case message, ok := <-m.messages:
		if !ok {
			return nil, false, ErrActorStopped
		}
		//else if ok is true, return the message
		return message, true, nil
	default:
		return nil, false, nil
	}
}

func (message *Mailbox) hasMessages() bool {
	return len(message.messages) > 0
}

// notify invokes the registered callback, if one is set.
func (m *Mailbox) notify() {
	m.mu.RLock()
	notifier := m.notifier
	m.mu.RUnlock()

	if notifier != nil {
		notifier()
	}
}

// setNotifier registers a callback for mailbox activity.
func (m *Mailbox) setNotifier(notifier func()) {
	m.mu.Lock()
	m.notifier = notifier

	//capture whether there's already work to process
	ready := len(m.messages) > 0 || m.closed
	m.mu.Unlock()

	if ready && notifier != nil {
		notifier()
	}

}
