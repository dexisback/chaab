package actor

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

const defaultMailBoxCapacity = 100

// System manages actor lifecycles, assigns unique IDs, maintains an
// active actor registry for lookup, and coordinates with a Dispatcher for execution.
type System struct {
	mu          sync.RWMutex
	actors      map[string]*ActorCell
	nextID      uint64
	dispatcher  Dispatcher
	deadLetters *deadLetterBus
	closed      bool
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
		actors:      make(map[string]*ActorCell),
		dispatcher:  NewGoroutineDispatcher(),
		deadLetters: newDeadLetterBus(),
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// SubscribeDeadLetters registers a listener callback to receive all dead letters.
// It returns an unsubscribe function to safely detach the listener.
func (s *System) SubscribeDeadLetters(listener DeadLetterListener) func() {
	if s == nil || s.deadLetters == nil {
		return func() {}
	}
	return s.deadLetters.subscribe(listener)
}

// PublishDeadLetter broadcasts a dead letter to all active listeners.
func (s *System) PublishDeadLetter(letter DeadLetter) {
	if s == nil || s.deadLetters == nil {
		return
	}
	s.deadLetters.publish(letter)
}

// NewSystemWithDispatcher creates a new actor system configured with the provided dispatcher.
func NewSystemWithDispatcher(dispatcher Dispatcher) *System {
	return NewSystem(WithDispatcher(dispatcher))
}

type actorOptions struct {
	mailboxCapacity  int
	overflowStrategy OverflowStrategy
	supervisor       Supervisor
}

// ActorOption configures individual actor cells upon creation.
type ActorOption func(*actorOptions)

// WithSupervisor attaches a custom Supervisor to an actor.
func WithSupervisor(supervisor Supervisor) ActorOption {
	return func(o *actorOptions) {
		o.supervisor = supervisor
	}
}

// WithMailboxCapacity configures the buffer size of the actor's mailbox.
func WithMailboxCapacity(capacity int) ActorOption {
	return func(o *actorOptions) {
		o.mailboxCapacity = capacity
	}
}

// WithOverflowStrategy configures the backpressure/overflow policy of the actor's mailbox.
func WithOverflowStrategy(strategy OverflowStrategy) ActorOption {
	return func(o *actorOptions) {
		o.overflowStrategy = strategy
	}
}

// CreateActor registers a new actor instance. For stateful actors requiring clean
// state recreation across restarts, prefer CreateActorFromFactory.
func (s *System) CreateActor(actor Actor, opts ...ActorOption) (*ActorRef, error) {
	if actor == nil {
		return nil, ErrNilActor
	}
	return s.CreateActorFromFactory(func() (Actor, error) {
		return actor, nil
	}, opts...)
}

// CreateActorFromFactory registers a new actor using a factory function.
// The factory is executed to construct the initial actor instance and called
// again on any supervised restart to discard mutated internal state.
func (s *System) CreateActorFromFactory(factory ActorFactory, opts ...ActorOption) (*ActorRef, error) {
	if factory == nil {
		return nil, ErrNilActor
	}

	initialActor, err := factory()
	if err != nil {
		return nil, fmt.Errorf("initial actor creation failed: %w", err)
	}
	if initialActor == nil {
		return nil, ErrNilActor
	}

	if starter, ok := initialActor.(PreStarter); ok {
		if err := starter.PreStart(context.Background()); err != nil {
			return nil, fmt.Errorf("actor PreStart failed: %w", err)
		}
	}

	cfg := &actorOptions{
		mailboxCapacity:  defaultMailBoxCapacity,
		overflowStrategy: OverflowBlock,
	}
	for _, opt := range opts {
		opt(cfg)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil, ErrDispatcherClosed
	}

	s.nextID++
	id := fmt.Sprintf("actor-%d", s.nextID)

	mailbox := NewMailboxWithStrategy(cfg.mailboxCapacity, cfg.overflowStrategy)

	reference := &ActorRef{
		id:      id,
		mailbox: mailbox,
		system:  s,
	}

	mailbox.SetOnDrop(func(dropped Message, reason error) {
		s.PublishDeadLetter(DeadLetter{
			RecipientID: id,
			Message:     dropped,
			Reason:      reason,
			Timestamp:   time.Now(),
		})
	})

	cell := &ActorCell{
		Actor:      initialActor,
		Ref:        reference,
		Mailbox:    mailbox,
		Factory:    factory,
		Supervisor: cfg.supervisor,
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

// Stop gracefully stops an actor: closes its mailbox, waits for all buffered messages
// to drain, terminates its execution in the dispatcher, and evicts it from the system registry.
func (s *System) Stop(id string) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return ErrDispatcherClosed
	}
	_, exists := s.actors[id]
	if !exists {
		s.mu.Unlock()
		return fmt.Errorf("%w: id=%s", ErrActorNotFound, id)
	}
	s.mu.Unlock()

	err := s.dispatcher.Unregister(id)
	if err != nil && !errors.Is(err, ErrActorNotFound) {
		return err
	}

	s.mu.Lock()
	delete(s.actors, id)
	s.mu.Unlock()

	return nil
}

// StopActor is an alias for Stop.
func (s *System) StopActor(id string) error {
	return s.Stop(id)
}

// Unregister stops an actor and removes it from the system registry.
// Deprecated: Prefer Stop or StopActor for idiomatic lifecycle management.
func (s *System) Unregister(id string) error {
	return s.Stop(id)
}

// Shutdown initiates graceful shutdown of the dispatcher and its actors.
func (s *System) Shutdown(ctx context.Context) error {
	if ctx == nil {
		return ErrNilContext
	}

	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()

	if err := s.dispatcher.Shutdown(ctx); err != nil {
		return err
	}

	s.mu.Lock()
	s.actors = make(map[string]*ActorCell)
	s.mu.Unlock()

	return nil
}
