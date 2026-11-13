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

type WorkerPoolConfig struct {
	Workers       int
	MessageBudget int
}

type workerPoolTask struct {
	cell    *ActorCell
	mailbox *Mailbox
	ctx     context.Context
	cancel  context.CancelFunc
	done    chan struct{}

	queued      bool
	running     bool
	wakePending bool
	finished    bool
}

type WorkerPoolDispatcher struct {
	mu       sync.Mutex
	cond     *sync.Cond
	tasks    map[string]*workerPoolTask
	ready    []*workerPoolTask
	config   WorkerPoolConfig
	metrics  atomic.Pointer[metricsHolder]
	closed   bool
	stopping bool
	done     chan struct{}
	wg       sync.WaitGroup
}

// SetMetrics configures the telemetry metrics collector for the dispatcher.
func (d *WorkerPoolDispatcher) SetMetrics(m MetricsCollector) {
	setMetricsHolder(&d.metrics, m)
}

func NewWorkerPoolDispatcher(config WorkerPoolConfig) *WorkerPoolDispatcher {
	if config.Workers <= 0 {
		config.Workers = runtime.NumCPU()
	}

	if config.MessageBudget <= 0 {
		config.MessageBudget = 100
	}

	d := &WorkerPoolDispatcher{
		tasks:  make(map[string]*workerPoolTask),
		config: config,
		done:   make(chan struct{}),
	}

	d.cond = sync.NewCond(&d.mu)

	for i := 0; i < config.Workers; i++ {
		d.wg.Add(1)
		go d.worker()
	}

	return d
}

// Register adds an actor to the worker pool.
func (d *WorkerPoolDispatcher) Register(cell *ActorCell) error {
	if cell == nil || cell.Actor == nil || cell.Ref == nil {
		return ErrInvalidActorCell
	}

	mailbox := cell.Mailbox
	if mailbox == nil {
		mailbox = cell.Ref.mailbox
	}
	if mailbox == nil {
		return ErrInvalidActorCell
	}

	id := cell.Ref.ID()
	if id == "" {
		return ErrInvalidActorCell
	}

	ctx, cancel := context.WithCancel(context.Background())

	task := &workerPoolTask{
		cell:    cell,
		mailbox: mailbox,
		ctx:     ctx,
		cancel:  cancel,
		done:    make(chan struct{}),
	}

	d.mu.Lock()

	if d.closed {
		d.mu.Unlock()
		cancel()
		return ErrDispatcherClosed
	}

	if _, exists := d.tasks[id]; exists {
		d.mu.Unlock()
		cancel()
		return fmt.Errorf("%w: id=%s", ErrActorAlreadyExists, id)
	}

	d.tasks[id] = task

	d.mu.Unlock()

	// The mailbox notifies us whenever it becomes ready.
	mailbox.setNotifier(func() {
		d.schedule(task)
	})

	return nil
}

// schedule makes an actor eligible for execution.
func (d *WorkerPoolDispatcher) schedule(task *workerPoolTask) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if task.finished {
		return
	}

	if task.running {
		task.wakePending = true
		return
	}

	d.scheduleLocked(task)
}

// scheduleLocked adds an actor to the ready queue.
// The caller must already hold d.mu.
func (d *WorkerPoolDispatcher) scheduleLocked(task *workerPoolTask) {
	if task.finished || task.queued || task.running {
		return
	}

	task.queued = true
	d.ready = append(d.ready, task)
	d.cond.Signal()
}

// worker continuously takes ready actors and processes their messages.
func (d *WorkerPoolDispatcher) worker() {
	defer d.wg.Done()

	for {
		d.mu.Lock()

		for len(d.ready) == 0 && !d.stopping {
			d.cond.Wait()
		}

		if d.stopping && len(d.ready) == 0 {
			d.mu.Unlock()
			return
		}

		task := d.ready[0]
		d.ready[0] = nil
		d.ready = d.ready[1:]

		task.queued = false

		if task.finished || task.running {
			d.mu.Unlock()
			continue
		}

		task.running = true
		task.wakePending = false

		d.mu.Unlock()

		d.process(task)
	}
}

// process handles a limited number of messages for one actor.
func (d *WorkerPoolDispatcher) process(task *workerPoolTask) {
	for i := 0; i < d.config.MessageBudget; i++ {
		message, ok, err := task.mailbox.tryReceive()

		if errors.Is(err, ErrActorStopped) {
			d.finish(task)
			return
		}

		if err != nil {
			slog.Error("mailbox receive failed",
				"actor_id", task.cell.Ref.ID(),
				"error", err,
			)
			continue
		}

		if !ok {
			break
		}

		if _, shouldStop := d.invoke(task, message); shouldStop {
			go task.mailbox.Close()
			d.finish(task)
			return
		}
	}

	d.mu.Lock()

	task.running = false

	if task.wakePending || task.mailbox.hasMessages() {
		d.scheduleLocked(task)
	}

	d.mu.Unlock()
}

// invoke calls the actor's Receive method and isolates panics.
func (d *WorkerPoolDispatcher) invoke(
	task *workerPoolTask,
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
					"actor panicked while processing a message",
					"actor_id", actorID,
					"panic", r,
				)
			}
		}()

		if err := task.cell.CurrentActor().Receive(task.ctx, message); err != nil {
			receiveErr = err
			slog.Error(
				"actor message processing failed",
				"actor_id", actorID,
				"error", err,
			)
			failure := Failure{ActorID: actorID, Err: err}
			directive := task.cell.HandleFailure(task.ctx, failure)
			if directive == DirectiveRestart {
				if restartErr := task.cell.Restart(); restartErr != nil {
					slog.Error("restart failed", "actor_id", actorID, "error", restartErr)
					shouldStop = true
				} else {
					resolveMetrics(&d.metrics, task.cell).ActorRestarted(actorID)
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
				resolveMetrics(&d.metrics, task.cell).MessageProcessed(actorID, duration, panicErr)
				return false, true
			}
			resolveMetrics(&d.metrics, task.cell).ActorRestarted(actorID)
			resolveMetrics(&d.metrics, task.cell).MessageProcessed(actorID, duration, panicErr)
			return false, false
		}
		resolveMetrics(&d.metrics, task.cell).MessageProcessed(actorID, duration, panicErr)
		return true, true
	}

	resolveMetrics(&d.metrics, task.cell).MessageProcessed(actorID, duration, receiveErr)
	return false, shouldStop
}

// finish marks an actor as finished and releases its resources.
func (d *WorkerPoolDispatcher) finish(task *workerPoolTask) {
	d.mu.Lock()
	if task.finished {
		d.mu.Unlock()
		return
	}

	task.finished = true
	delete(d.tasks, task.cell.Ref.ID())
	d.mu.Unlock()

	task.cancel()
	_ = task.cell.InvokePostStop(context.Background())
	close(task.done)
}

// Unregister closes an actor's mailbox and waits for it to finish draining.
func (d *WorkerPoolDispatcher) Unregister(id string) error {
	d.mu.Lock()

	if d.closed {
		d.mu.Unlock()
		return ErrDispatcherClosed
	}

	task, exists := d.tasks[id]
	if !exists {
		d.mu.Unlock()
		return fmt.Errorf("%w: id=%s", ErrActorNotFound, id)
	}

	d.mu.Unlock()

	task.mailbox.Close()
	<-task.done

	return nil
}

// Shutdown gracefully closes all actor mailboxes and waits for workers to exit.
func (d *WorkerPoolDispatcher) Shutdown(ctx context.Context) error {
	if ctx == nil {
		return ErrNilContext
	}

	d.mu.Lock()

	if !d.closed {
		d.closed = true

		tasks := make([]*workerPoolTask, 0, len(d.tasks))
		for _, task := range d.tasks {
			tasks = append(tasks, task)
		}

		go d.shutdown(tasks)
	}

	done := d.done

	d.mu.Unlock()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// shutdown performs the actual graceful shutdown.
func (d *WorkerPoolDispatcher) shutdown(tasks []*workerPoolTask) {
	for _, task := range tasks {
		task.mailbox.Close()
	}

	for _, task := range tasks {
		<-task.done
	}

	d.mu.Lock()
	d.stopping = true
	d.cond.Broadcast()
	d.mu.Unlock()

	d.wg.Wait()
	close(d.done)
}
