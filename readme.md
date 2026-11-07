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
