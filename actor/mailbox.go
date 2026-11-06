//stores incoming msgs until an actor is ready to take it in
//buffered go channel ( so async ). 
//TODO: custom mailbox for bounded queues/backpressure and perf work

package actor
import "context"


type Mailbox struct {
	messages chan Message
}


//factory func to create a new mailbox with a given capacity
func NewMailBox(capacity int) *Mailbox {
	return &Mailbox{
		messages: make(chan Message, capacity),  //buff chann so we can have async model
	}
}

//send adds a msg to the mailbox. blocks if full
func (mailbox *Mailbox) Send(ctx context.Context, message Message) error {  //only error returns because if it succeeds it returns nil
	select {
	case mailbox.messages <- message:
		return nil
	case <- ctx.Done():
		return ctx.Err()
	}
	
}


//receive retries the next msg from the mailbox:
func (m *Mailbox) Receive(ctx context.Context) (Message, error) {
	//either returns the msg to the actor or an error
	select {
	case message := <-m.messages:
		return message , nil
	case <- ctx.Done():
		return nil, ctx.Err()
	}
}


