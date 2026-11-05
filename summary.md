# Cineyma: Exhaustive Repository Summary

> This document summarizes the public Cineyma repository at [github.com/pixperk/cineyma](https://github.com/pixperk/cineyma), the deployed frontend at [cineyma.vercel.app](https://cineyma.vercel.app/), and the generated API documentation at [docs.rs/cineyma](https://docs.rs/cineyma).
>
> The repository was audited from the `main` branch at commit `8c2bb6b` (`ci: remove clippy`). The local checkout built successfully and its test suite passed: 48 active tests passed, 3 tests were ignored, and none failed.

## 1. What Cineyma is

Cineyma is a lightweight actor-model framework for Rust. It is designed for building concurrent and distributed systems using:

- actors with private state;
- typed messages;
- bounded mailboxes;
- asynchronous Tokio tasks;
- request-response and fire-and-forget messaging;
- supervision trees and panic isolation;
- timers and external streams;
- named actor registries;
- remote actors over TCP;
- Protocol Buffers serialization;
- gossip-based cluster membership;
- distributed actor discovery.

The repository describes Cineyma as being inspired by Erlang/OTP, Akka, and Actix. Its central idea is that application state should be owned by actors and modified through messages, rather than being shared directly between concurrent tasks.

The project is a Rust library crate named `cineyma`, currently version `0.1.0` in the repository metadata. It targets Rust `1.75` or newer and uses Tokio as its async runtime.

The name “Cineyma” is not formally expanded or explained in the repository. It appears to be a project name rather than an acronym.

## 2. The problem it is trying to solve

Concurrent software becomes difficult when many tasks directly share mutable state. Developers then have to reason about mutexes, race conditions, deadlocks, ownership, ordering, backpressure, failures, and communication between machines.

Cineyma attempts to provide a clearer model:

```text
Actor owns state
      |
      v
Bounded mailbox receives typed messages
      |
      v
Actor processes one message at a time
      |
      v
Other actors communicate through addresses
```

The actor does not expose its state directly. Other code receives an `Addr<A>` handle and sends messages to it. The actor task then processes those messages sequentially with mutable access to its own state.

The intended benefits are:

1. Fewer shared-memory synchronization problems.
2. Compile-time checking of message and response types.
3. Explicit backpressure from bounded mailboxes.
4. Failure isolation at actor boundaries.
5. A path from one-process concurrency to multi-node communication.

## 3. Design philosophy

The README presents four major principles:

### Explicit supervision

Failures should be handled through visible supervision policies instead of being silently swallowed or recovered from without a defined strategy.

### Typed messaging

Messages and their response types are expressed through Rust traits and generics. The compiler checks that an actor supports the message being sent.

### Sequential state ownership

An actor owns its state and handles one message at a time. This makes the actor’s state easier to reason about than shared state accessed by many tasks.

### Minimal magic

Cineyma does not rely heavily on procedural macros or code generation for the local actor API. The main concepts are explicit Rust traits, Tokio channels, and spawned tasks.

## 4. The core actor model

The public API is built around six concepts:

| Concept | Meaning |
|---|---|
| `Actor` | A type that owns state and participates in the runtime |
| `Message` | A typed value sent to an actor |
| `Handler<M>` | The implementation of how an actor handles message type `M` |
| `Addr<A>` | A handle used to communicate with actor type `A` |
| `Context<A>` | Runtime capabilities available to the actor |
| `ActorSystem` | The object used to spawn and manage top-level actors |

### Defining an actor

An actor is an ordinary Rust struct implementing the `Actor` trait:

```rust
struct Counter {
    value: i32,
}

impl Actor for Counter {}
```

The `Actor` trait requires the type to be `Send`, sized, and `'static`. It provides optional lifecycle hooks:

```rust
fn started(&mut self, ctx: &mut Context<Self>) {}
fn stopped(&mut self, ctx: &mut Context<Self>) {}
```

### Defining messages

A message implements `Message` and declares its response type through an associated type:

```rust
struct Increment(i32);

impl Message for Increment {
    type Result = i32;
}
```

The associated result can be `()`, a primitive, a custom struct, or another message-like type.

### Defining handlers

An actor can handle multiple message types by implementing `Handler<M>` multiple times:

```rust
impl Handler<Increment> for Counter {
    fn handle(
        &mut self,
        msg: Increment,
        _ctx: &mut Context<Self>,
    ) -> i32 {
        self.value += msg.0;
        self.value
    }
}
```

The handler receives `&mut self`, so the actor can mutate its state without a mutex for that state. The runtime guarantees that the actor task invokes the handler serially.

## 5. How spawning works internally

The central implementation is in [`src/system.rs`](https://github.com/pixperk/cineyma/blob/main/src/system.rs).

When `ActorSystem::spawn(actor)` is called, Cineyma:

1. Creates a Tokio bounded `mpsc` channel for the actor.
2. Assigns the actor a unique `ActorId`.
3. Creates a stop notification signal.
4. Creates an `Addr<A>` containing the sender side of the channel.
5. Creates a `Context<A>` containing the address and runtime signals.
6. Spawns a Tokio task for the actor.
7. Calls `actor.started(...)`.
8. Enters a `tokio::select!` loop.
9. Receives and dispatches messages, streams, shutdown notifications, and stop signals.
10. Calls `actor.stopped(...)` during termination.

The default mailbox capacity is 256 messages:

```rust
let addr = system.spawn(actor);
let addr = system.spawn_with_capacity(actor, 10_000);
```

Every actor gets its own mailbox and Tokio task. That makes the implementation conceptually simple, although it also means that spawning an actor creates a runtime task and channel allocation.

## 6. Addresses and message sending

`Addr<A>` is the main communication handle. It is cheap to clone because it internally clones the Tokio sender and shares watcher state.

### Request-response

```rust
let result = addr.send(message).await?;
```

Internally, Cineyma creates a Tokio one-shot response channel, wraps the message and response sender in an envelope, and puts that envelope into the actor’s mailbox. The actor invokes the handler, and the envelope sends the result back through the one-shot channel.

### Fire-and-forget with backpressure

```rust
addr.do_send(message).await?;
```

This still waits asynchronously if the mailbox is full. It does not wait for a handler result, but it does apply mailbox backpressure.

### Immediate non-blocking send

```rust
addr.try_send(message)?;
```

This returns immediately. It produces `MailboxFull` when the bounded channel is full and `MailboxClosed` when the actor has stopped.

### Time-limited request-response

```rust
addr.send_timeout(message, duration).await?;
```

The response wait is wrapped in `tokio::time::timeout`. A timeout returns `MailboxError::Timeout`.

### Async handlers

Cineyma also defines `AsyncHandler<M>`. Async handlers return a boxed future and can perform non-blocking I/O inside the actor’s message-processing path.

The corresponding address methods are:

```rust
addr.send_async(message).await?;
addr.do_send_async(message).await?;
addr.try_send_async(message)?;
```

## 7. Envelopes and type erasure

One mailbox must be able to contain different message types, even though Rust is statically typed. Cineyma solves this with type-erased envelopes.

The public API remains generic and type-safe:

```rust
addr.send(Add(2, 3)).await?;
```

Internally, the message is placed inside a boxed trait object implementing `Envelope<A>` or `AsyncEnvelope<A>`.

The envelope knows how to:

1. recover the concrete message;
2. invoke the matching `Handler<M>` implementation;
3. send the handler result to the waiting caller if one exists.

Relevant source:

- [`src/envelope.rs`](https://github.com/pixperk/cineyma/blob/main/src/envelope.rs)
- [`src/message.rs`](https://github.com/pixperk/cineyma/blob/main/src/message.rs)
- [`src/address.rs`](https://github.com/pixperk/cineyma/blob/main/src/address.rs)

## 8. Bounded mailboxes and backpressure

The default mailbox size is 256. This is one of the project’s explicit design choices.

An unbounded mailbox can allow a fast producer to keep enqueueing work while a slow consumer falls further behind. Eventually, memory usage can grow without a fixed limit.

Cineyma instead uses Tokio bounded channels:

```text
producer -> bounded mailbox -> actor consumer
```

When the mailbox is full:

- `do_send().await` waits for capacity;
- `try_send()` returns `MailboxFull` immediately.

This does not magically solve overload. It gives the application a defined point at which it must slow down, reject work, retry, or apply another policy.

## 9. Actor lifecycle and panic isolation

The actor runtime catches panics around synchronous and asynchronous message handling.

For a top-level actor:

```text
handler panics
      |
      v
panic is caught
      |
      v
actor stops
      |
      v
watchers are notified
      |
      v
children are asked to stop
```

For a supervised child, the parent’s selected `SupervisorStrategy` determines what happens.

Important distinction: panic isolation does not mean the actor automatically survives. A top-level actor currently stops after a panic. Restart behavior is implemented for children created through the supervision API.

## 10. Supervision trees

An actor can spawn child actors from its `Context`:

```rust
ctx.spawn_child(child);
```

or with a strategy:

```rust
ctx.spawn_child_with_strategy(
    || Worker::new(),
    SupervisorStrategy::restart(
        3,
        Duration::from_secs(10),
    ),
);
```

The supported strategies are:

### `Stop`

The child is allowed to terminate after a panic.

### `Restart`

The factory is called again to construct a new child instance. A `RestartTracker` limits the number of restarts within a time window.

For example, three restarts within ten seconds are allowed. Once the limit is exceeded, the child stops.

Because a new instance is constructed, ordinary in-memory state is reset on restart.

### `Escalate`

The child notifies its parent’s escalation signal. The parent treats the escalation as a failure and can itself be supervised by its own parent.

The implementation resembles an OTP-style supervision tree, but it is a smaller implementation than the full Erlang/OTP supervision system.

Source: [`src/supervisor.rs`](https://github.com/pixperk/cineyma/blob/main/src/supervisor.rs) and [`src/context.rs`](https://github.com/pixperk/cineyma/blob/main/src/context.rs).

## 11. Death watching

An actor can watch another actor:

```rust
ctx.watch(&worker_addr);
```

The watched actor stores watcher handles. When it terminates, it sends a `Terminated` message containing the dead actor’s ID to each watcher.

This allows an actor to react to another actor’s death without directly sharing lifecycle state.

The current implementation stores watcher handles in a `Mutex<Vec<...>>` inside the address. The actor’s application state remains sequentially owned, but watcher registration itself uses shared synchronization.

## 12. Context capabilities

`Context<A>` gives the actor access to runtime features:

- its own `Addr<A>`;
- its `ActorId`;
- stopping itself;
- spawning children;
- stopping children;
- watching other actors;
- scheduling delayed messages;
- scheduling repeated messages;
- attaching external streams;
- accessing escalation signals.

The context is passed to lifecycle hooks and handlers:

```rust
fn handle(&mut self, msg: Message, ctx: &mut Context<Self>) -> Result
```

## 13. Timers

The context supports delayed and periodic self-messages.

### Delayed messages

```rust
let handle = ctx.run_later(
    Duration::from_secs(5),
    TimeoutMessage,
);
```

### Repeated messages

```rust
let handle = ctx.run_interval(
    Duration::from_secs(1),
    Tick,
);
```

Both return a `TimerHandle` that can be cancelled. The timer task sends through the actor’s own address, so timer messages still go through the mailbox and are processed in the same serialized manner as external messages.

Source: [`src/timer.rs`](https://github.com/pixperk/cineyma/blob/main/src/timer.rs) and [`src/context.rs`](https://github.com/pixperk/cineyma/blob/main/src/context.rs).

## 14. Stream processing

Actors can consume external asynchronous streams by implementing `StreamHandler<I>`:

```rust
impl StreamHandler<String> for MyActor {
    fn handle(
        &mut self,
        item: String,
        _ctx: &mut Context<Self>,
    ) {
        println!("received: {item}");
    }

    fn finished(&mut self, _ctx: &mut Context<Self>) {
        println!("stream ended");
    }
}
```

The context attaches the stream with `add_stream`. The actor runtime polls streams alongside its mailbox, prioritizing mailbox messages in its `tokio::select!` loop.

This is demonstrated by the chat example, where TCP input is converted into a `LinesStream` and attached to a `ClientSession` actor.

Source: [`src/stream.rs`](https://github.com/pixperk/cineyma/blob/main/src/stream.rs).

## 15. Local actor registry

The local registry maps names to type-erased actor addresses:

```rust
system.register("user-store", address);

let store = system.lookup::<UserStore>("user-store");
```

There are two registration modes:

```rust
system.register("name", address);        // automatic cleanup
system.register_manual("name", address); // manual cleanup
```

Automatic cleanup starts a watcher task that periodically checks whether the address is alive. Once the actor stops, the registry removes the name.

The registry is process-local. It is separate from the distributed actor registry used by the clustering layer.

Source: [`src/registry.rs`](https://github.com/pixperk/cineyma/blob/main/src/registry.rs).

## 16. Remote actors

The remote layer allows messages to travel between processes or machines over TCP.

The wire format is based on Protocol Buffers. The project defines an `Envelope` containing:

```text
message_type
payload
correlation_id
sender_node
target_actor
is_response
```

The TCP framing format is:

```text
[4-byte big-endian payload length][Protocol Buffers envelope]
```

### Remote message requirements

A message intended for remote use implements `RemoteMessage`:

```rust
impl Message for Add {
    type Result = AddResult;
}

impl RemoteMessage for Add {}
```

The trait requires the message to also implement Protocol Buffers’ `Message` trait, `Default`, `Clone`, and Cineyma’s `Message` trait.

The default remote type identifier is Rust’s fully qualified type name:

```rust
std::any::type_name::<Self>()
```

That means both sides must agree on the message type identity. Renaming or moving a Rust type can affect its wire-level type name unless an explicit routing scheme is added.

### Remote client

`RemoteClient` owns a persistent TCP connection and a background Tokio task. It has:

- a command channel for outgoing envelopes;
- a pending-request map keyed by correlation ID;
- a receive loop for incoming responses;
- one-shot channels for request-response callers.

This enables multiple concurrent requests to share one connection and be matched by correlation ID.

### Remote server

`RemoteServer` accepts TCP connections and repeatedly receives envelopes. Each envelope is passed to an `EnvelopeHandler`.

`LocalNode::handler::<Actor, Message>(addr)` creates a request-response handler that:

1. decodes the protobuf request;
2. sends it to the local actor;
3. waits for the actor result;
4. serializes the result;
5. returns a response envelope.

`tell_handler` supports fire-and-forget delivery.

### Message router

`MessageRouter` maps message type identifiers to envelope handlers:

```rust
let router = MessageRouter::new()
    .route::<Add>(add_handler)
    .route::<Subtract>(subtract_handler)
    .build();
```

Unknown message types can optionally be sent to a default handler.

## 17. Cluster architecture

The cluster layer builds on remote TCP messaging.

Each `ClusterNode` maintains:

```text
members:        node ID -> Node
last_heartbeat: node ID -> last-seen time
actor_registry: actor ID -> (node ID, actor type)
```

Each node begins knowing about itself. Nodes learn about other nodes through gossip.

### Gossip messages

Gossip includes:

- node membership information;
- node addresses;
- node statuses;
- distributed actor locations.

This means actor discovery is piggybacked on membership gossip instead of requiring a separate discovery service.

### Periodic gossip

`start_periodic_gossip(interval, suspect_timeout)` starts a Tokio task that:

1. checks heartbeats;
2. changes `Up` nodes to `Suspect` after the timeout;
3. changes `Suspect` nodes to `Down` after twice the timeout;
4. removes actors registered on down nodes;
5. chooses a random non-local peer;
6. sends the current gossip state to that peer.

The implementation is an eventually consistent, timeout-based gossip system. It is not a full production membership system.

### Cluster server

The cluster server multiplexes two kinds of traffic on one TCP port:

```text
ClusterMessage
  ├── GossipMessage
  └── Actor Envelope
```

Gossip messages update membership and registry state. Actor envelopes are passed to the node’s local message router.

### Cluster client

`ClusterClient` provides location-transparent communication:

```rust
let client = ClusterClient::new(cluster.clone());
let remote = client.remote_addr::<KVStore>("kv-store");

let result = remote.call(GetRequest { key }).await?;
```

The call flow is:

```text
actor name
    |
    v
cluster registry lookup
    |
    v
hosting node lookup
    |
    v
connection pool lookup/creation
    |
    v
TCP request with correlation ID
    |
    v
remote router
    |
    v
local actor
```

The client pools connections by node address. Failed connections are removed so a later request can reconnect.

## 18. The distributed KV example

The distributed KV example is the clearest example of the intended cluster architecture.

Each node has:

- a `ClusterNode`;
- a cluster TCP server;
- periodic gossip;
- an interactive command-line interface.

`node-1` hosts a `KVStore` actor. The actor owns a `HashMap<String, String>` and handles:

- `GetRequest`;
- `SetRequest`;
- `DeleteRequest`.

These messages and their responses are generated from Protocol Buffers definitions.

The important design detail is that the data is not replicated. Other nodes discover where `kv-store` lives and forward messages to that actor.

Therefore this example demonstrates location-transparent messaging, not distributed data durability.

Source: [`examples/distributed-kv`](https://github.com/pixperk/cineyma/tree/main/examples/distributed-kv).

## 19. The chat example

The chat example demonstrates local actor coordination and stream handling.

It contains:

### `ChatServer`

Owns a map of usernames to `ClientSession` addresses. It handles:

- clients joining;
- clients leaving;
- message broadcasts.

### `ClientSession`

Represents one TCP client. It:

- reads lines through a Tokio stream;
- sends incoming text to `ChatServer` through messages;
- receives outgoing messages through its actor mailbox;
- writes output through a separate async writer task;
- notifies the server when it stops.

The architecture is:

```text
TCP listener
    |
    v
ClientSession actor per connection
    |
    v
ChatServer actor
    |
    v
other ClientSession actors
```

Source: [`examples/chat/src/main.rs`](https://github.com/pixperk/cineyma/blob/main/examples/chat/src/main.rs).

## 20. Repository layout

```text
.
├── src/
│   ├── actor.rs              Actor, Handler, AsyncHandler, StreamHandler
│   ├── address.rs            Addr and message sending
│   ├── context.rs            Actor context, children, timers, streams
│   ├── envelope.rs           Type-erased message envelopes
│   ├── error.rs              Mailbox errors
│   ├── message.rs            Message definitions
│   ├── registry.rs           Local named actor registry
│   ├── stream.rs             Stream integration
│   ├── supervisor.rs         Stop, Restart, Escalate strategies
│   ├── system.rs             ActorSystem and top-level actors
│   ├── timer.rs              TimerHandle
│   ├── watcher.rs            Death-watch abstraction
│   └── remote/
│       ├── addr.rs           Remote addresses and IDs
│       ├── client.rs         Persistent remote client
│       ├── cluster.rs        Membership and gossip
│       ├── cluster_client.rs Cluster-aware messaging
│       ├── handler.rs        Local handlers and message router
│       ├── registry.rs       Remote message deserialization registry
│       ├── server.rs         Remote TCP server
│       ├── tcp.rs            TCP framing and transport
│       └── transport.rs      Transport traits and errors
├── tests/
│   ├── basic.rs              Local messaging and mailbox behavior
│   ├── system.rs             Lifecycle and shutdown behavior
│   ├── supervision.rs        Child supervision and restart behavior
│   ├── streams.rs            Stream integration
│   ├── remote.rs             Remote messaging
│   └── cluster.rs            Gossip and distributed messaging
├── examples/
│   ├── chat/                 TCP chat application
│   └── distributed-kv/       Clustered key-value example
├── benches/                  Criterion performance benchmarks
├── proto/messages.proto      Core cluster/remote protobuf schema
├── build.rs                  Protobuf code generation
├── .github/workflows/ci.yml  CI checks and cross-platform tests
├── README.md                 Main documentation
├── Cargo.toml                Package and workspace configuration
├── Makefile                  Test and benchmark shortcuts
└── LICENSE                   MIT license
```

The repository also contains a `website/` directory in the GitHub file listing, but the deployed frontend source is not meaningfully present in this main checkout. The Vercel frontend appears to be maintained or built separately.

## 21. Dependencies and build process

The main crate depends on:

- `tokio` for async tasks, channels, timers, and networking;
- `tokio-util` for framed TCP codecs;
- `tokio-stream` for stream adapters;
- `futures` for stream and future utilities;
- `prost` for Protocol Buffers encoding and decoding;
- `bytes` for byte buffers;
- `rand` for selecting random gossip peers.

Build dependencies include `prost-build`, which compiles `proto/messages.proto` into Rust code during the build.

Development dependencies include Criterion for benchmarks.

The CI workflow runs:

- `cargo check --all-features`;
- formatting verification;
- tests on Linux, macOS, and Windows;
- tests with stable Rust and Rust 1.75;
- documentation generation with warnings denied;
- code coverage generation.

The CI workflow explicitly installs `protoc` because Protocol Buffers code generation requires it.

## 22. Tests

The test suite covers the main advertised areas:

- basic actor creation;
- synchronous request-response;
- fire-and-forget messaging;
- bounded mailbox behavior;
- async handlers;
- timeouts;
- actor lifecycle hooks;
- system shutdown;
- child actors;
- restart limits;
- escalation;
- death watching;
- streams;
- remote envelope serialization;
- remote TCP connections;
- remote request-response;
- gossip membership merging;
- periodic gossip convergence;
- failure detection;
- distributed actor registry propagation;
- actor cleanup after a node is marked down;
- cluster-aware remote calls.

The tests are primarily in-process Tokio tests. They exercise real TCP listeners for many remote and cluster scenarios, but they should not be interpreted as proof that the system is production-ready across arbitrary machines, networks, firewalls, or failure modes.

## 23. Benchmarks and performance claims

The README and frontend advertise measurements such as:

- approximately 2.2 microseconds to spawn an actor;
- approximately 1.5 million messages per second in a multi-actor benchmark;
- approximately 17.8 microseconds for synchronous request-response;
- less than 200 nanoseconds for small Protocol Buffers serialization;
- approximately 112 milliseconds for a seven-node gossip convergence benchmark.

The repository includes Criterion benchmarks for:

- actor spawning;
- message throughput;
- request-response latency;
- serialization;
- gossip;
- failure detection.

However, these are benchmark results documented by the project, not independently reproduced during this audit. Benchmark results depend heavily on CPU, compiler version, optimization level, runtime configuration, operating system, and benchmark methodology.

One important example is the message-throughput benchmark analysis in the README: it notes that a sleep used by the benchmark dominates some measurements. Therefore the headline throughput numbers should be treated as project-specific benchmark observations, not universal framework guarantees.

## 24. Frontend and documentation site

The deployed site is a marketing and documentation frontend rather than part of the actor runtime.

The homepage presents Cineyma as:

```text
local actors
      -> remote messaging
      -> cluster
```

Its main claims emphasize:

- typed messages;
- bounded mailboxes;
- self-healing actors;
- gossip-based clustering;
- automatic discovery;
- Rust and Tokio.

The documentation site contains pages for:

- installation;
- quick start;
- actors;
- messages;
- handlers;
- context;
- addresses;
- supervision;
- death watching;
- timers;
- streams;
- registry;
- remote actors;
- clustering;
- distributed registry;
- CRUD API;
- chat server;
- distributed KV store.

The frontend’s roadmap advertises completed versions for the foundation, fault tolerance, runtime features, and distribution, followed by planned work for:

- event sourcing;
- persistent actors;
- snapshots;
- storage backends;
- sharding;
- consistent hashing;
- rebalancing;
- distributed pub/sub;
- OpenTelemetry;
- metrics;
- tracing;
- dead-letter monitoring;
- TestKit;
- debugging tools;
- hot code reloading;
- a dashboard;
- CLI tools.

## 25. Documentation/source discrepancies

The marketing language is ahead of the current implementation in a few places.

### Supervision terminology

The roadmap mentions `OneForOne` and `AllForOne` supervision policies, but the current Rust API exposes `Stop`, `Restart`, and `Escalate`. There is no visible implementation of those two group supervision strategies.

### Failure detection terminology

The roadmap calls the failure detector “phi-accrual.” The source instead compares `Instant` values against fixed suspect and down timeouts.

### “Self-healing” wording

Restarting is available for supervised child actors. A top-level actor that panics is stopped rather than automatically restarted.

### Frontend code example

The deployed homepage’s rendered code example displays:

```rust
format!("Hello, !", msg.0)
```

The repository README and library documentation correctly show:

```rust
format!("Hello, {}!", msg.0)
```

This appears to be a frontend display/content bug, not a runtime implementation bug.

## 26. Important limitations and risks

### No encryption or authentication

Remote actors use raw TCP and Protocol Buffers. The repository does not provide TLS, authentication, authorization, or message signing. A deployment should not expose the actor port directly to an untrusted network.

### No persistence

Actor state is in memory. Restarting an actor resets ordinary state. Restarting a process loses all state.

### No replication

The distributed KV example routes requests to one actor location. It does not replicate values between nodes.

### Eventual consistency

Cluster membership and actor locations spread through gossip. A newly registered actor may not be immediately visible everywhere.

### Limited failure semantics

Connection failure and actor failure are represented through relatively small error types. Some complex cases, such as outstanding remote calls during a connection drop, need stronger production-grade handling.

### No sharding or rebalancing

Actor locations are registered and discovered, but the framework does not currently move actors between nodes or rebalance workloads.

### No observability layer

The current code does not provide built-in metrics for mailbox depth, actor latency, dead letters, distributed traces, or health endpoints.

### Shutdown is signal-based

`ActorSystem::shutdown()` broadcasts a notification. It does not itself wait for every actor to finish and does not return a shutdown report.

## 27. What the project is best understood as

Cineyma is best understood as:

> A compact, educational Rust actor runtime that demonstrates how typed actor messaging can be built on Tokio, then extended with supervision, streams, TCP remoting, and a simple gossip-based cluster.

It is valuable because the implementation is small enough to read end to end. The framework exposes the mechanics that larger systems often hide:

- channels become mailboxes;
- Tokio tasks become actor processes;
- generic traits become typed messages and handlers;
- one-shot channels become request-response replies;
- `catch_unwind` becomes actor-boundary panic isolation;
- child factories become restart behavior;
- protobuf envelopes become remote messages;
- gossip state becomes distributed actor discovery.

## 28. Recommended reading order

For someone learning actor systems and Rust, the most useful reading order is:

1. [`src/actor.rs`](https://github.com/pixperk/cineyma/blob/main/src/actor.rs) — actor and handler traits.
2. [`src/message.rs`](https://github.com/pixperk/cineyma/blob/main/src/message.rs) — typed message contract.
3. [`src/address.rs`](https://github.com/pixperk/cineyma/blob/main/src/address.rs) — sending messages.
4. [`src/envelope.rs`](https://github.com/pixperk/cineyma/blob/main/src/envelope.rs) — type erasure and replies.
5. [`src/system.rs`](https://github.com/pixperk/cineyma/blob/main/src/system.rs) — actor task and mailbox loop.
6. [`src/context.rs`](https://github.com/pixperk/cineyma/blob/main/src/context.rs) — children, timers, streams, and supervision.
7. [`tests/basic.rs`](https://github.com/pixperk/cineyma/blob/main/tests/basic.rs) — everyday local behavior.
8. [`tests/supervision.rs`](https://github.com/pixperk/cineyma/blob/main/tests/supervision.rs) — failure behavior.
9. [`examples/chat/src/main.rs`](https://github.com/pixperk/cineyma/blob/main/examples/chat/src/main.rs) — a practical actor application.
10. [`src/remote`](https://github.com/pixperk/cineyma/tree/main/src/remote) — networking and serialization.
11. [`examples/distributed-kv`](https://github.com/pixperk/cineyma/tree/main/examples/distributed-kv) — cluster discovery and location-transparent messaging.

This order keeps the local actor model understandable before introducing serialization, TCP, correlation IDs, gossip, and eventual consistency.

## 29. Final assessment

Cineyma successfully demonstrates the core actor-model idea:

```text
private state
+ typed messages
+ sequential processing
+ bounded mailboxes
+ supervision
+ async runtime
+ optional network distribution
```

The local actor implementation is the strongest and clearest part of the project. The remote and cluster layers are ambitious extensions that work in the repository’s tests, but they should be treated as early-stage infrastructure rather than a mature distributed runtime.

For learning, experimentation, and understanding actor-model building blocks, the repository is a strong reference. For production use, it would need additional work around security, persistence, observability, cluster correctness, failure semantics, lifecycle management, and API stabilization.
