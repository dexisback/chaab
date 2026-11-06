we're building an actor model implementation in go baby!



we'll eventually implement : 
- one goroutine per actor
- shared worker pool
- work stealing
- cooperative scheduling



- also cover supervision, message ordering, backpressure, graceful shutdown, observability, testing and failure handling

- the banking application should depend on the actor framework, not the other way around
- 



---


//TODO: custom mailbox for bounded queues/backpressure and perf work
//TODO: blocking logic if mailbox is already full (in mailbox.go ka send)

//TODO : One limitation: this initial implementation doesn't support mailbox closure or distinguish a closed mailbox from a message. We'll address lifecycle semantics later.
//For this first version, we'll implement actor registration and lookup. Actors won't process messages yet.

//