// how this differs from worker pool dispatcher?
// 1: only one shared "ready" slice (queue) -> 1 queue per worker
// 2: workers pop from shared queue -> workers pop from their own queue
// 3: no stealing -> idle workers can steal from other workers
// 4: sync.Cond wakes workers -> here asw sync.Cond wakes workers

package actor

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

type workStealingTask struct {
	cell    *ActorCell //the task needs message and id from the cell
	mailbox *Mailbox   // mailbox because the message is extracted from the mailbox first (task.Mailbox.tryReceive()), the worker also needs task.mailbox.Close() for handling panics , task.mailbox.hasMessages() to se eif more work arrived
	ctx     context.Context
	cancel  context.CancelFunc
	done    chan struct{} //when the actor fully shuts down, finish() closes it (close(task.done))

	queued      bool //to prevent duplicate copies of the same actor from flooding the queues
	running     bool //to guarantee that no 2 workers ever execute the same actor at the same time
	wakePending bool
	finished    bool //to mark an actor as permanently dead and prevent zombie
}

// it sets runtime limits and sizing for the dispatcher
type WorkStealingConfig struct {
	MaxWorkers       int //number of worker goroutines running in parallel. if unset , it defaults to runtime.NumCPU()   (//runtime.NumCPU returns the number of logical CPUs avaiable to the OS, so utne workers ban jaate)
	MaxMessageBudget int
}

// it manages thread pool, routing, synchronisation, and lifecycle of all actors
type WorkStealingDispatcher struct {
	mu sync.Mutex //because only one goroutine can hold the lock (d.mu.Lock()) at a given time. Any other goroutine trying to acquire it has to pause and wait until d.mu.Unlock() is called. (this is what sync.Mutex does (mutual exclusion))
	//because multiple goroutines are running parallely, all of them touch shared variables. Adding tasks to d.queues, stealing tasks from another worker's queue, reading or updating d.tasks map, etc
	//in go , reading and writing to slices and maps across multiple goroutines without synchronisation causes race conditions. (it leads to race

	cond *sync.Cond //condition variable -- cond.Wait() -> automatically releases the lock (d.mu) and puts the goroutine to sleep. When it wakes back up, it automatically re-acquires the lock before continuing
	//cond.Broadcast() -> wakes up all sleeping goroutines
	//basically cond used for coordinating b/w goroutines (when one goroutine wants to wait for another goroutine to do something)

	tasks map[string]*workStealingTask //this is our central registry -> a hashmap where keys are strings (unique actor_ids) and values are pointers to workStealingTask
	//why is it here? because queues only hold tasks that are ready to run right now . but what about actors that are idle? waiting for tasks ? dispatcher still needs to keep track of every actor and this is how we do it

	queues  [][]*workStealingTask
	config  WorkStealingConfig
	metrics atomic.Pointer[metricsHolder]
	next    int //the next pointer for our round robin scheduler

	closed   bool //for shutdown signlaing ,calling this rejects any new registrations and new unregistrations
	stopping bool //for shutdown signaling, calling this tells the worker loops to exit permanently (final exit call, close ke baad still remaining things process hoti hai)

	done chan struct{} //acts as an asynchronous completion signal for anyone calling Shutdown(ctx)

	wg sync.WaitGroup
}

// SetMetrics configures the telemetry metrics collector for the dispatcher.
func (dispatcher *WorkStealingDispatcher) SetMetrics(m MetricsCollector) {
	setMetricsHolder(&dispatcher.metrics, m)
}

func NewWorkStealingDispatcher(config *WorkStealingConfig) *WorkStealingDispatcher {
	if config == nil {
		config = &WorkStealingConfig{}

	}

	if config.MaxWorkers <= 0 {
		config.MaxWorkers = runtime.NumCPU()
	}

	dispatch := &WorkStealingDispatcher{

		tasks:  make(map[string]*workStealingTask),
		queues: make([][]*workStealingTask, config.MaxWorkers),
		config: *config,
		done:   make(chan struct{}),
	}

	dispatch.cond = sync.NewCond(&dispatch.mu)

	for i := 0; i < config.MaxWorkers; i++ {
		dispatch.wg.Add(1)
		go dispatch.worker(i) //defined further down the line   //worker method is a long running infinite loop, it keeps on waiting for tasks to appear in its queue or stealing them from other workers
	}
	return dispatch
}

// register adds an actor to the dispatcher:
func (dispatcher *WorkStealingDispatcher) Register(cell *ActorCell) error {
	if cell == nil || cell.Actor == nil || cell.Ref == nil {
		return ErrInvalidActorCell
	}

	mailbox := cell.Mailbox //get the mailbox

	if mailbox == nil {
		mailbox = cell.Ref.mailbox
	}

	if mailbox == nil {
		return ErrInvalidActorCell
	}

	id := cell.Ref.ID() //ref contains ID and mailbox
	if id == "" {
		return ErrInvalidActorCell
	}

	ctx, cancel := context.WithCancel(context.Background())

	task := &workStealingTask{
		cell:    cell,
		mailbox: mailbox,
		ctx:     ctx,
		cancel:  cancel,
		done:    make(chan struct{}),
	}

	dispatcher.mu.Lock()

	if dispatcher.closed {
		dispatcher.mu.Unlock()
		cancel()
		return ErrDispatcherClosed
	}

	if _, exists := dispatcher.tasks[id]; exists {
		dispatcher.mu.Unlock()
		cancel()
		return fmt.Errorf("%w: id=%s", ErrActorAlreadyExists, id)
	}

	dispatcher.tasks[id] = task
	dispatcher.mu.Unlock()

	mailbox.setNotifier(func() {
		dispatcher.schedule(task)
	})

	return nil

}

// schedule makes an actor eligible to run on the next available worker
func (dispatcher *WorkStealingDispatcher) schedule(task *workStealingTask) {
	dispatcher.mu.Lock()
	defer dispatcher.mu.Unlock()

	if task.finished {
		return
	}

	if task.running {
		task.wakePending = true
		return
	}

	dispatcher.scheduleLocked(task)
}

func (dispatcher *WorkStealingDispatcher) scheduleLocked(task *workStealingTask) {
	if task.finished || task.queued || task.running {
		return
	}

	workerID := dispatcher.next
	dispatcher.next = (dispatcher.next + 1) % dispatcher.config.MaxWorkers //increase counter to point to next worker
	task.queued = true

	dispatcher.queues[workerID] = append(dispatcher.queues[workerID], task)

	dispatcher.cond.Broadcast()
}

func (dispatcher *WorkStealingDispatcher) worker(workerID int) {
	defer dispatcher.wg.Done()

	for {
		dispatcher.mu.Lock()
		var task *workStealingTask

		for task == nil && !dispatcher.stopping {
			task = dispatcher.getTaskLocked(workerID) //bhai wtf

			if task == nil {
				dispatcher.cond.Wait()

			}
		}

		if dispatcher.stopping && task == nil {
			dispatcher.mu.Unlock()
			return
		}

		if task.finished || task.running {
			// The task was removed from its queue, so it is no longer queued.
			task.queued = false
			dispatcher.mu.Unlock()
			continue
		}
		task.queued = false
		task.running = true
		task.wakePending = false
		dispatcher.mu.Unlock()

		dispatcher.process(task)
	}

}

//getsTaskLocked first checks the worker's own queue
//If empty, it attempts to steal from other workers

//The owner takes from the front (FIFO), and the Thieves take from the back (LIFO) ,The caller must already hold dispatcher.mu

func (dispatcher *WorkStealingDispatcher) getTaskLocked(workerID int) *workStealingTask {
	ownQueue := dispatcher.queues[workerID]

	if len(ownQueue) > 0 {
		task := ownQueue[0]
		dispatcher.queues[workerID][0] = nil
		dispatcher.queues[workerID] = ownQueue[1:]
		return task
	}

	for offset := 1; offset < len(dispatcher.queues); offset++ {
		victimID := (workerID + offset) % len(dispatcher.queues)
		victimQueue := dispatcher.queues[victimID]

		if len(victimQueue) == 0 {
			continue
		}

		last := len(victimQueue) - 1
		task := victimQueue[last]

		dispatcher.queues[victimID][last] = nil
		dispatcher.queues[victimID] = victimQueue[:last]
		resolveMetrics(&dispatcher.metrics, task.cell).TaskStolen(workerID, victimID)
		return task
	}
	return nil
}

// process handles a limited number of messages for one actor:
func (dispatcher *WorkStealingDispatcher) process(task *workStealingTask) {
	for i := 0; i < dispatcher.config.MaxMessageBudget; i++ {
		idle, finished := dispatcher.receiveOne(task)
		if finished {
			return
		}
		if idle {
			break
		}
	}

	// A turn can spend its whole budget on the last buffered messages of a
	// mailbox that is already closed. The loop then ends on the budget,
	// not on ErrActorStopped, and Unregister waits forever.
	if !task.mailbox.hasMessages() {
		if _, finished := dispatcher.receiveOne(task); finished {
			return
		}
	}

	dispatcher.mu.Lock()
	task.running = false
	if task.wakePending || task.mailbox.hasMessages() {
		dispatcher.scheduleLocked(task)
	}

	dispatcher.mu.Unlock()
}

// receiveOne takes one message.
// idle is true when the mailbox has nothing ready.
// finished is true when the actor has been stopped or isolated after a panic.
func (dispatcher *WorkStealingDispatcher) receiveOne(task *workStealingTask) (idle, finished bool) {
	message, ok, err := task.mailbox.tryReceive()

	if errors.Is(err, ErrActorStopped) {
		dispatcher.finish(task)
		return false, true
	}
	if err != nil {
		slog.Error(
			"error receiving message",
			"actor", task.cell.Ref.ID(),
			"error", err,
		)
		return false, false
	}
	if !ok {
		return true, false
	}

	if _, shouldStop := dispatcher.invoke(task, message); shouldStop {
		task.mailbox.Close()
		dispatcher.finish(task)
		return false, true
	}
	return false, false
}

// invoke calls the actor's Receive method and isolates panics.
func (dispatcher *WorkStealingDispatcher) invoke(
	task *workStealingTask,
	message Message,
) (panicked bool, shouldStop bool) {
	var panicVal any
	var receiveErr error

	actorID := ""
	if task != nil && task.cell != nil && task.cell.Ref != nil {
		actorID = task.cell.Ref.ID()
	}

	start := time.Now()

	func() {
		defer func() {
			if r := recover(); r != nil {
				panicked = true
				panicVal = r
				slog.Error(
					"panic in actor receive",
					"actor", actorID,
					"message", message,
					"error", r,
				)
			}
		}()

		if err := task.cell.CurrentActor().Receive(task.ctx, message); err != nil {
			receiveErr = err
			slog.Error(
				"error in actor receive",
				"actor", actorID,
				"message", message,
				"error", err,
			)
			failure := Failure{ActorID: actorID, Err: err}
			directive := task.cell.HandleFailure(task.ctx, failure)
			if directive == DirectiveRestart {
				if restartErr := task.cell.Restart(); restartErr != nil {
					slog.Error("restart failed", "actor_id", actorID, "error", restartErr)
					shouldStop = true
				} else {
					resolveMetrics(&dispatcher.metrics, task.cell).ActorRestarted(actorID)
				}
			} else if directive == DirectiveStop {
				shouldStop = true
			}
		}
	}()

	duration := time.Since(start)

	if panicked {
		panicErr := fmt.Errorf("panic: %v", panicVal)
		failure := Failure{ActorID: actorID, Panic: panicVal}
		directive := task.cell.HandleFailure(task.ctx, failure)
		if directive == DirectiveRestart {
			if restartErr := task.cell.Restart(); restartErr != nil {
				slog.Error("restart failed", "actor_id", actorID, "error", restartErr)
				resolveMetrics(&dispatcher.metrics, task.cell).MessageProcessed(actorID, duration, panicErr)
				return false, true
			}
			resolveMetrics(&dispatcher.metrics, task.cell).ActorRestarted(actorID)
			resolveMetrics(&dispatcher.metrics, task.cell).MessageProcessed(actorID, duration, panicErr)
			return false, false
		}
		resolveMetrics(&dispatcher.metrics, task.cell).MessageProcessed(actorID, duration, panicErr)
		return true, true
	}

	resolveMetrics(&dispatcher.metrics, task.cell).MessageProcessed(actorID, duration, receiveErr)
	return false, shouldStop
}

func (dispatcher *WorkStealingDispatcher) finish(task *workStealingTask) {
	dispatcher.mu.Lock()
	if task.finished {
		dispatcher.mu.Unlock()
		return
	}

	task.finished = true
	delete(dispatcher.tasks, task.cell.Ref.ID())
	dispatcher.mu.Unlock()

	task.cancel()
	_ = task.cell.InvokePostStop(context.Background())
	close(task.done)
}

func (dispatcher *WorkStealingDispatcher) Unregister(id string) error {
	dispatcher.mu.Lock()

	if dispatcher.closed {
		dispatcher.mu.Unlock()
		return ErrDispatcherClosed
	}

	task, exists := dispatcher.tasks[id]
	if !exists {
		dispatcher.mu.Unlock()
		return fmt.Errorf("%w: id=%s", ErrActorNotFound, id)
	}

	dispatcher.mu.Unlock()

	task.mailbox.Close()
	<-task.done
	return nil
}

func (dispatcher *WorkStealingDispatcher) Shutdown(ctx context.Context) error {
	if ctx == nil {
		return ErrNilContext

	}

	dispatcher.mu.Lock()
	if !dispatcher.closed {
		dispatcher.closed = true
		tasks := make([]*workStealingTask, 0, len(dispatcher.tasks))
		for _, task := range dispatcher.tasks {
			tasks = append(tasks, task)

		}

		go dispatcher.shutdown(tasks)
	}
	done := dispatcher.done
	dispatcher.mu.Unlock()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

//shutdown performs the actual graceful shutdown

func (dispatcher *WorkStealingDispatcher) shutdown(tasks []*workStealingTask) {
	for _, task := range tasks {
		task.mailbox.Close()
	}

	for _, task := range tasks {
		<-task.done
	}

	dispatcher.mu.Lock()
	dispatcher.stopping = true
	dispatcher.cond.Broadcast()
	dispatcher.mu.Unlock()
	dispatcher.wg.Wait()

	close(dispatcher.done)
}

//per worker dequeus: each worker owns a separate dequeue
//Lock free stealing : use a proven Case-lev work-stealing queue
//Owner and thief architecutre: Owner pushes and pops from one end, thieves steal from the opposite end
//Scheduling: ROund robin distribution of newly runnable actors
//Actor safety: preserve the queued, running, wakePending, and finished invariants
// Lifecycle: preserve graceful unregister, shutdown, message draining, and panic isolation
