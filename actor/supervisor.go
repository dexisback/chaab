package actor

//this file defines how our framework responds when an actor fails
//either can restart the actor, stop the actor, or escalate the failure to another supervisor

import "context"

type Directive int //describes what should happen after an actor fails , final directive

const (
	DirectiveStop Directive = iota
	DirectiveRestart
	Directiveescalate
)

type Failure struct {
	ActorID string
	Err     error
}
type Supervisor interface {
	HandleFailure(ctx context.Context, failure Failure) Directive
}
