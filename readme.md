we're building an actor model implementation in go baby!

we'll eventually implement : 
- one goroutine per actor
- shared worker pool
- work stealing
- cooperative scheduling

- also cover supervision, message ordering, backpressure, graceful shutdown, observability, testing and failure handling
- the banking application should depend on the actor framework, not the other way around

---
Architecture:
- system: creates actors, maintains registry, manages lifecycle
- actorref + mailbox: actorref identifies the actor, mailbox queues messages
- dispatcher: manages actor execution; all execution strategies implement the common Dispatcher interface
  - goroutine dispatcher: one dedicated goroutine per actor (blocking receive loop)
  - worker pool dispatcher: shared worker pool (future)
  - work stealing dispatcher: distributed work queues (future)
  - cooperative execution: cooperative budgets and yielding (future)



---



Two separate flows: message delivery and message execution:

1: Message delivery: the banking app talks to System to create an actor. Once it has an `actorRef`,it uses that reference to send messages
2: Message Execution: System registers actor -> Dispatcher starts actor execution -> GoroutineDispatcher (right now one per actor, starts dedicated goroutine) -> Actor worker receives -> puts it in the mailbox and mailbox returns next message  


(Mailbox is used for both send and receive). 
Send() enqueues messages. Receive() dequeues them



