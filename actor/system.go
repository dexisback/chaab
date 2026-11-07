package actor

import (
	"context"
	"fmt"
	"sync"
)

const defaultMailBoxCapacity = 100

// System manages actor lifecycles, assigns unique IDs, maintains an
// active actor registry for lookup, and coordinates with a Dispatcher for execution.
type System struct {
	mu         sync.RWMutex
	actors     map[string]*ActorCell
	nextID     uint64
	dispatcher Dispatcher
}

// SystemOption configures a System.
type SystemOption func(*System)

// WithDispatcher sets the execution Dispatcher for the actor system.
func WithDispatcher(dispatcher Dispatcher) SystemOption {
	return func(s *System) {
		s.dispatcher = dispatcher
	}
}

// NewSystem creates a new actor system. By default, it uses a GoroutineDispatcher.
func NewSystem(opts ...SystemOption) *System {
	s := &System{
		actors:     make(map[string]*ActorCell),
		dispatcher: NewGoroutineDispatcher(),
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// NewSystemWithDispatcher creates a new actor system configured with the provided dispatcher.
func NewSystemWithDispatcher(dispatcher Dispatcher) *System {
	return NewSystem(WithDispatcher(dispatcher))
}

// CreateActor registers a new actor with the system and its dispatcher, returning an ActorRef.
func (s *System) CreateActor(actor Actor) (*ActorRef, error) {
	if actor == nil {
		return nil, ErrNilActor
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.nextID++
	id := fmt.Sprintf("actor-%d", s.nextID)

	mailbox := NewMailBox(defaultMailBoxCapacity)

	reference := &ActorRef{
		id:      id,
		mailbox: mailbox,
	}

	cell := &ActorCell{
		Actor:   actor,
		Ref:     reference,
		Mailbox: mailbox,
	}

	if err := s.dispatcher.Register(cell); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrRegisterActor, err)
	}

	s.actors[id] = cell
	return reference, nil
}

// GetActor looks up an actor reference by its ID.
func (s *System) GetActor(id string) (*ActorRef, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	cell, exists := s.actors[id]
	if !exists {
		return nil, false
	}
	return cell.Ref, true
}

// Unregister stops an actor and removes it from the system registry.
func (s *System) Unregister(id string) error {
	s.mu.Lock()
	_, exists := s.actors[id]
	if !exists {
		s.mu.Unlock()
		return fmt.Errorf("%w: id=%s", ErrActorNotFound, id)
	}
	delete(s.actors, id)
	s.mu.Unlock()

	return s.dispatcher.Unregister(id)
}

// Shutdown initiates graceful shutdown of the dispatcher and its actors.
func (s *System) Shutdown(ctx context.Context) error {
	return s.dispatcher.Shutdown(ctx)
}
