package actor

import (
	"sync"
	"time"
)

// DeadLetter represents an undeliverable, rejected, or dropped message.
type DeadLetter struct {
	RecipientID string
	Message     Message
	Reason      error
	Timestamp   time.Time
}

// DeadLetterListener is a callback invoked when a DeadLetter is published.
type DeadLetterListener func(letter DeadLetter)

// deadLetterBus manages thread-safe subscriptions and broadcasting.
type deadLetterBus struct {
	mu          sync.RWMutex
	subscribers map[uint64]DeadLetterListener
	nextSubID   uint64
}

func newDeadLetterBus() *deadLetterBus {
	return &deadLetterBus{
		subscribers: make(map[uint64]DeadLetterListener),
	}
}

// subscribe registers a new listener and returns an unsubscribe function.
func (b *deadLetterBus) subscribe(listener DeadLetterListener) func() {
	if listener == nil {
		return func() {}
	}

	b.mu.Lock()
	b.nextSubID++
	id := b.nextSubID
	b.subscribers[id] = listener
	b.mu.Unlock()

	return func() {
		b.mu.Lock()
		delete(b.subscribers, id)
		b.mu.Unlock()
	}
}

// publish broadcasts a DeadLetter to all active subscribers safely.
func (b *deadLetterBus) publish(letter DeadLetter) {
	b.mu.RLock()
	if len(b.subscribers) == 0 {
		b.mu.RUnlock()
		return
	}

	listeners := make([]DeadLetterListener, 0, len(b.subscribers))
	for _, l := range b.subscribers {
		listeners = append(listeners, l)
	}
	b.mu.RUnlock()

	for _, listener := range listeners {
		func(l DeadLetterListener) {
			defer func() {
				_ = recover()
			}()
			l(letter)
		}(listener)
	}
}
