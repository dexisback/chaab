package actor

import "context"
import "sync"


type Mailbox struct {
	messages chan Message
	mu    sync.RWMutex
	closed bool  //currently, a mailbox has no closed state. After an actor stops, callers can still send messages into its channels. we want -- Close() prevents new sends. Sends already in progress are allowed to finish(graceful). The receiver continues consuming queued msgs, once the queue is empty, Receive() reports that the mailbox is closed
	done  chan struct{}
	closeDone  chan struct{}
	senders sync.WaitGroup
}

// NewMailBox creates a new mailbox with the given capacity.
func NewMailBox(capacity int) *Mailbox {
	return &Mailbox{
		messages: make(chan Message, capacity),
		done: make(chan struct{}),
		closeDone: make(chan struct{}),
		senders: sync.WaitGroup{},
	}
}
// CHANGED:
func (m *Mailbox) Send(ctx context.Context, message Message) error {
    if ctx == nil {
        return ErrNilContext
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
func (m *Mailbox) Receive(ctx context.Context) (Message, error) {
	if ctx == nil {
		return nil, ErrNilContext
	}
	select {
	case message, ok := <-m.messages:  //when a channel closes : buffered messages can still be received. Once drained, ok becomes false
		if !ok{
			return nil, ErrActorStopped
		}
		return message, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

//close the mailbox safely
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