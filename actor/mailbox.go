package actor

import (
	"context"
	"sync"
)

// OverflowStrategy defines how a Mailbox handles incoming messages when full.
type OverflowStrategy int

const (
	// OverflowBlock blocks the sender until buffer space is available or ctx expires (default).
	OverflowBlock OverflowStrategy = iota

	// OverflowFailFast immediately returns ErrMailboxFull without blocking if capacity is reached.
	OverflowFailFast

	// OverflowDropOldest evicts the oldest message in the mailbox to make room for the new message.
	OverflowDropOldest

	// OverflowDropNewest discards the incoming message without blocking or returning an error.
	OverflowDropNewest
)

type Mailbox struct {
	messages  chan Message
	capacity  int
	strategy  OverflowStrategy
	mu        sync.RWMutex
	notifier  func()
	closed    bool
	done      chan struct{}
	closeDone chan struct{}
	senders   sync.WaitGroup
}

// NewMailBox creates a new mailbox with the given capacity and default OverflowBlock strategy.
func NewMailBox(capacity int) *Mailbox {
	return NewMailboxWithStrategy(capacity, OverflowBlock)
}

// NewMailboxWithStrategy creates a mailbox with a specific capacity and overflow strategy.
func NewMailboxWithStrategy(capacity int, strategy OverflowStrategy) *Mailbox {
	if capacity <= 0 {
		capacity = defaultMailBoxCapacity
	}
	return &Mailbox{
		messages:  make(chan Message, capacity),
		capacity:  capacity,
		strategy:  strategy,
		done:      make(chan struct{}),
		closeDone: make(chan struct{}),
	}
}

// Capacity returns the maximum buffer capacity of the mailbox.
func (m *Mailbox) Capacity() int {
	return m.capacity
}

// Strategy returns the overflow policy configured for this mailbox.
func (m *Mailbox) Strategy() OverflowStrategy {
	return m.strategy
}

// Send adds a message to the mailbox according to its configured OverflowStrategy.
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

	switch m.strategy {
	case OverflowFailFast:
		select {
		case m.messages <- message:
			m.notify()
			return nil
		case <-ctx.Done():
			return ctx.Err()
		case <-m.done:
			return ErrActorStopped
		default:
			return ErrMailboxFull
		}

	case OverflowDropNewest:
		select {
		case m.messages <- message:
			m.notify()
			return nil
		case <-ctx.Done():
			return ctx.Err()
		case <-m.done:
			return ErrActorStopped
		default:
			// Discard incoming message without failing the caller
			return nil
		}

	case OverflowDropOldest:
		for {
			select {
			case m.messages <- message:
				m.notify()
				return nil
			case <-ctx.Done():
				return ctx.Err()
			case <-m.done:
				return ErrActorStopped
			default:
				// Mailbox is full: evict one oldest message to make room
				select {
				case <-m.messages:
					// Oldest evicted! Loop back to enqueue.
				default:
					// Space opened concurrently by consumer
				}
			}
		}

	case OverflowBlock:
		fallthrough
	default:
		select {
		case m.messages <- message:
			m.notify()
			return nil
		case <-ctx.Done():
			return ctx.Err()
		case <-m.done:
			return ErrActorStopped
		}
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

// TryReceive attempts to non-blockingly dequeue a message if available.
func (m *Mailbox) TryReceive() (Message, bool, error) {
	return m.tryReceive()
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
