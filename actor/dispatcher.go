package actor
import "context"
//connects message to actor
//currently our `System` can create actors and assign mailboxes, but nothing actually consumes messages from those mailboxes



//dispatch solves that
//application -> ActorRef (via Tell) -> Mailbox -> Dispatcher (via Receive) -> Actor.Receive(ctx , message)

//dispatcher: manages actor execution and registration
//scheduler: decides which worker executes an actor ka message
type Dispatcher interface {
	Register(cell *actorCell) error //make an actor avaiable for execution
	Unregister(id string) error  //remove it from execution
	Shutdown(ctx context.Context) error  //handles graceful shutdown
}

//later on we will have mutliple dispatch strategy. currently its one goroutine per actor, later well have workerpooldispatcher, and workstealingdispatcher

