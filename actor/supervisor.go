package actor

//this file defines how our framework responds when an actor fails
//either can restart the actor, stop the actor, or escalate the failure to another supervisor

import (
	"context"
	"sync"
	"time"
)

type Directive int //describes what should happen after an actor fails , final directive

const (
	DirectiveStop Directive = iota
	DirectiveRestart
	DirectiveResume
	DirectiveEscalate
)

type Failure struct {
	ActorID string
	Err     error
	Panic   any
}

type Supervisor interface {
	HandleFailure(ctx context.Context, failure Failure) Directive
}

// DeciderFunc inspects a Failure and determines the appropriate Directive.
type DeciderFunc func(failure Failure) Directive

// OneForOneStrategy applies restart limits to each actor independently.
// If an actor fails more than MaxRestarts times within WithinDuration,
// the supervisor stops the actor instead of restarting it.
type OneForOneStrategy struct {
	MaxRestarts    int
	WithinDuration time.Duration
	Decider        DeciderFunc

	mu       sync.Mutex
	restarts map[string][]time.Time
}

// NewOneForOneStrategy creates a bounded restart strategy.
func NewOneForOneStrategy(maxRestarts int, withinDuration time.Duration, decider DeciderFunc) *OneForOneStrategy {
	if decider == nil {
		decider = func(failure Failure) Directive {
			if failure.Panic != nil {
				return DirectiveRestart
			}
			return DirectiveResume
		}
	}
	return &OneForOneStrategy{
		MaxRestarts:    maxRestarts,
		WithinDuration: withinDuration,
		Decider:        decider,
		restarts:       make(map[string][]time.Time),
	}
}

// HandleFailure enforces bounded restart limits for the failing actor.
func (s *OneForOneStrategy) HandleFailure(ctx context.Context, failure Failure) Directive {
	directive := s.Decider(failure)
	if directive != DirectiveRestart {
		return directive
	}

	if s.MaxRestarts <= 0 {
		return DirectiveStop
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	history := s.restarts[failure.ActorID]

	// Prune timestamps older than WithinDuration window
	if s.WithinDuration > 0 {
		cutoff := now.Add(-s.WithinDuration)
		valid := history[:0]
		for _, t := range history {
			if t.After(cutoff) {
				valid = append(valid, t)
			}
		}
		history = valid
	}

	if len(history) >= s.MaxRestarts {
		delete(s.restarts, failure.ActorID)
		return DirectiveStop
	}

	s.restarts[failure.ActorID] = append(history, now)
	return DirectiveRestart
}
