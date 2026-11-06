//first kinda implementation. one actor = one goroutine

package actor

//start a goroutine when an actor is registered. continously read msgs from its mailbox. pass each message to actor.receive(). stop the goroutine when the actor is unregister or the dispatcher shuts down

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
)

type actorWorker struct {
	cancel context.CancelFunc
	done   chan struct{}
}

type GoroutineDispatcher struct {
	mu      sync.Mutex
	workers map[string]*actorWorker
	wg      sync.WaitGroup
	closed  bool
}

func NewGoroutineDispatcher() *GoroutineDispatcher {
	return &GoroutineDispatcher{
		workers: make(map[string]*actorWorker),
	}
}

func (d *GoroutineDispatcher) Register(cell *actorCell) error {

	if cell == nil || cell.actor == nil || cell.ref == nil {
		return fmt.Errorf("invalid actor cell")

	}

	d.mu.Lock()
	defer d.mu.Unlock()

	if d.closed {
		return fmt.Errorf("dispatcher is closed")
	}
	id := cell.ref.id

	if _, exists := d.workers[id]; exists {
		return fmt.Errorf("actor %s already registered", id)
	}
	ctx, cancel := context.WithCancel(context.Background())
	worker := &actorWorker{
		cancel: cancel,
		done:   make(chan struct{}),
	}

	d.workers[id] = worker
	d.wg.Add(1)

	go d.run(ctx, cell, worker)

	return nil

}

// run function (continously processes messages for one actor)
func (d *GoroutineDispatcher) run(
	ctx context.Context,
	cell *actorCell,
	worker *actorWorker,
) {
	defer d.wg.Done()
	defer close(worker.done)

	for {
		message, err := cell.ref.mailbox.Receive(ctx)
		if err != nil {

			if ctx.Err() != nil {
				return
			}
			slog.Error("mailbox receive error: %v", err)
			continue
		}
		if ctx.Err() != nil {
			return 
		}
		if err := cell.actor.Receive(ctx, message); err != nil{
			slog.Error(
				"actor receive error: %v",
				err,
				"actor", cell.ref.id,
			)
		}
	}
}



func (d *GoroutineDispatcher) Unregister(id string) error {
	d.mu.Lock()
	worker, exists := d.workers[id]
	if !exists {
		d.mu.Unlock()
		return fmt.Errorf("actor %s is not registered", id)
	}

	delete(d.workers, id)
	worker.cancel()
	d.mu.Unlock()

	<-worker.done
	return nil

}


//shutdown stops ALL actors managed by the dispatcher:
func (d *GoroutineDispatcher) Shutdown(ctx context.Context) error {
	d.mu.Lock()

	d.closed = true



	for _, worker := range d.workers{
		worker.cancel()
	}

	d.workers = make(map[string]*actorWorker)
	d.mu.Unlock()

	done :=make(chan struct{})

	go func(){
		d.wg.Wait()
		close(done)
	}()
	

	select{
	case <- done:
		return nil

	case <- ctx.Done():
		return ctx.Err()
	
	}
	

}