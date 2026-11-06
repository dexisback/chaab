//actor needs a stable identity, so others can send it message 
//so PID wali cheez

package actor 

import "context"

type ActorRef struct {
	id   string 
	mailbox   *Mailbox
}
//we can also use just id in the ActorRef (for address)

func (r *ActorRef) ID() string {
	return r.id
}


//tell asynchronously enqueue a message for the actor
func (reference *ActorRef) Tell(ctx context.Context, message Message) error {
	return reference.mailbox.Send(ctx, message)
}

