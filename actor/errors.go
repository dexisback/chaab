package actor

import "errors"

var (
	ErrNilActor           = errors.New("actor cannot be nil")
	ErrActorNotFound      = errors.New("actor not found")
	ErrActorAlreadyExists = errors.New("actor already exists")
	ErrDispatcherClosed   = errors.New("dispatcher is shut down")
	ErrActorStopped       = errors.New("actor is stopped")
	ErrMailboxFull        = errors.New("mailbox is full")
	ErrInvalidMessage     = errors.New("invalid message")
	ErrSchedulerClosed    = errors.New("scheduler is closed") // distinguishes scheduler shutdown from dispatcher shutdown
	ErrInvalidActorCell   = errors.New("invalid actor cell")
	ErrNilContext         = errors.New("context is nil")
	ErrNilTask            = errors.New("task is nil")
	ErrRegisterActor      = errors.New("failed to register actor with dispatcher")
	ErrScheduleTask       = errors.New("failed to schedule actor task")
)



//what this allows: 
// if errors.Is(err, actor.ErrActorNotFound) {
    // Handle missing actor
//}