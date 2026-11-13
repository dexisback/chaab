package banking

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/dexisback/chaab/actor"
)

type Bank struct {
	system      *actor.System
	accounts    map[string]*actor.ActorRef
	deadLetters []actor.DeadLetter
	mu          sync.RWMutex
}

func NewBank(dispatcher actor.Dispatcher) *Bank {
	var opts []actor.SystemOption
	if dispatcher != nil {
		opts = append(opts, actor.WithDispatcher(dispatcher)) // configure execution strategy
	}
	sys := actor.NewSystem(opts...)
	b := &Bank{
		system:   sys,
		accounts: make(map[string]*actor.ActorRef),
	}
	sys.SubscribeDeadLetters(func(letter actor.DeadLetter) {
		b.mu.Lock()
		b.deadLetters = append(b.deadLetters, letter) // capture dropped messages
		b.mu.Unlock()
	})
	return b
}

func (b *Bank) System() *actor.System {
	return b.system // expose actor system
}

func (b *Bank) OpenAccount(id string, initialBalance int64, cap int, strat actor.OverflowStrategy, sup actor.Supervisor) (*actor.ActorRef, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	opts := []actor.ActorOption{
		actor.WithMailboxCapacity(cap),    // buffer capacity
		actor.WithOverflowStrategy(strat), // backpressure strategy
	}
	if sup != nil {
		opts = append(opts, actor.WithSupervisor(sup)) // attach supervisor
	}

	ref, err := b.system.CreateActorFromFactory(func() (actor.Actor, error) {
		return NewAccountActor(id, initialBalance, b.system), nil // clean instance on restart
	}, opts...)
	if err != nil {
		return nil, err
	}

	b.accounts[id] = ref
	b.accounts[ref.ID()] = ref // index by generated ref id
	return ref, nil
}

func (b *Bank) GetAccount(id string) (*actor.ActorRef, bool) {
	b.mu.RLock()
	ref, ok := b.accounts[id]
	b.mu.RUnlock()
	if ok {
		return ref, true
	}
	return b.system.GetActor(id) // fallback to actor system registry
}

func (b *Bank) QueryBalance(ctx context.Context, id string) (int64, error) {
	ref, ok := b.GetAccount(id)
	if !ok {
		return 0, fmt.Errorf("account %s not found", id)
	}
	reply := make(chan int64, 1)
	if err := ref.Tell(ctx, GetBalance{ReplyTo: reply}); err != nil {
		return 0, err
	}
	select {
	case bal := <-reply:
		return bal, nil
	case <-time.After(2 * time.Second):
		return 0, errors.New("timeout querying balance")
	}
}

func (b *Bank) DeadLettersCount() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.deadLetters)
}

func (b *Bank) Shutdown(ctx context.Context) error {
	return b.system.Shutdown(ctx) // graceful system drain
}
