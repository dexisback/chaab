//this create actors, assigns unique ids , maintains an active actor registry for lookup, returns actor refereces so other actors can communicate

package actor

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

const defaultMailBoxCapacity = 100

// actorCell represents an actor managed by the actorSystem
type actorCell struct {
	actor Actor
	ref   *ActorRef
}

type System struct {
	mu         sync.RWMutex
	actors     map[string]*actorCell
	nextID     uint64
	dispatcher Dispatcher //new for goroutine_dispatcher
}

func NewSystemWithDispatcher(dispatcher Dispatcher) *System {
	return &System{
		actors:     make(map[string]*actorCell),
		dispatcher: dispatcher,
	}
}

func NewSystem() *System {
	// return &System{
	// 	actors: make(map[string]*actorCell),
	// }
	scheduler := NewGoRoutineScheduler()
	dispatcher := NewGoroutineDispatcher(scheduler)
	return NewSystemWithDispatcher(dispatcher)
}

//others didnt need intialisation because they were just types(you could technically though)

func (s *System) CreateActor(actor Actor) (*ActorRef, error) {
	if actor == nil {
		return nil, ErrNilActor
	}
	//else:
	s.mu.Lock()
	defer s.mu.Unlock()

	s.nextID++
	id := fmt.Sprintf("actor-%d", s.nextID)

	mailbox := NewMailBox(defaultMailBoxCapacity)

	reference := &ActorRef{
		id:      id,
		mailbox: mailbox,
	}

	// s.actors[id] = &actorCell{
	// 	actor: actor,
	// 	ref: reference,
	// }
	// return reference, nil

	cell := &actorCell{
		actor: actor,
		ref:   reference,
	}
	if err := s.dispatcher.Register(cell); err != nil {
		return nil, errors.Join(ErrRegisterActor, err)
	}

	s.actors[id] = cell
	return reference, nil
}

// lookup an actor by its id:
func (s *System) GetActor(id string) (*ActorRef, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	cell, exists := s.actors[id]
	if !exists {
		return nil, false
	}
	//else:
	return cell.ref, true
}

//NOTE: for now, we're generating sequential IDs. later, we'll consider using UUIDs or more robust ID generation strategies.

func (s *System) Shutdown(ctx context.Context) error {
	return s.dispatcher.Shutdown(ctx)
}  //so the applicatoin doesnt need to know which dispatcher / strategy is running 
