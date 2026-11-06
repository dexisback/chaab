package actor

import (
	"context"
	"sync"
)

type GoroutineScheduler struct {
	mu     sync.Mutex
	wg     sync.WaitGroup
	closed bool
}

func NewGoRoutineScheduler() *GoroutineScheduler {
	return &GoroutineScheduler{}
}

// schedule executes a task in its own goroutine
func (s *GoroutineScheduler) Schedule(ctx context.Context, task Task) error {
	if ctx == nil {
		return ErrNilContext
	}
	if task == nil {
		return ErrNilTask
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return ErrSchedulerClosed
	}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		if ctx.Err() != nil {
			return 
		}
		task()
	}()
	return nil
}

//shutdown:
func (s *GoroutineScheduler) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	s.closed = true 
	s.mu.Unlock()

	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()

	select{
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
	


// NOTE: goroutine_dispatcher.go now uses goroutine_scheduler.go
// 	:goroutineDispatcher.Register() schedules tasks using goroutine_scheduler.Schedule()
// 	:goroutineDispatcher.Shutdown() waits for all goroutines to finish using goroutine_scheduler.Shutdown()
// this separation allows us to implement different scheduling strategies without coupling them to actor, mailbox, or system


//One important design detail: our current dispatcher submits a long-running mailbox loop as one task per actor. That's fine for this scheduler, but a worker pool cannot use that model efficiently—each actor would occupy a worker indefinitely.
//We'll address that by changing actor execution to process one message (or a bounded batch) per scheduled task before implementing the worker pool.

