# WebFlux — `Mono` and `Flux`

> **WebFlux gives you the non-blocking edge; nioflow gives you the blocking middle.** Neither has to pretend to be the other.

A nioflow pipeline already ends in a `CompletableFuture`, so there is no bridge to build. What the reactive facade adds is the ability to write `Mono`-returning calls as **ordinary steps** — and to end the pipeline in a `Mono`:

```java
@PostMapping("/orders/{id}/pay")
public Mono<Receipt> pay(@PathVariable String id) {
    return orders.just(id)
            .handle("load", repo::findById)                    // blocking JDBC — on a virtual worker
            .handleMono("fraud", fraud::score, ofMillis(200))  // a WebClient call — a stage like any other
            .adaptMono(psp::charge)                            // Mono<Receipt>: the chain continues at Receipt
            .executeMono();                                    // Netty's event loop was never touched
}
```

No `subscribeOn`, no `publishOn`, no `boundedElastic`.

## Why it fits

1. **`call()` never blocks the caller.** `executeMono()` from a Netty event-loop thread hands a task to a boss and returns. (Admission control with `OverflowPolicy.BLOCK` lives in `inject()`, the fire-and-forget path — never here.)
2. **The boss never runs user code, and workers are virtual threads.** So a stage may block — JDBC, JPA, a legacy SOAP client — and the only thread that parks is a virtual one. That is the problem WebFlux otherwise solves with `publishOn(Schedulers.boundedElastic())`, a *capped pool of platform threads* that a slow downstream saturates.
3. **A pipeline is a value, not a running thing.** So `executeMono()` is lazy per subscription: nothing runs until someone subscribes, and `.retry()` / `.repeat()` re-run the whole pipeline.

## Setup

Reactor is **`compileOnly`** in core, like OpenTelemetry and Resilience4j: the facade only loads if your app brings `reactor-core` (every WebFlux app does), and core keeps its zero required runtime dependencies.

```java
@Bean(destroyMethod = "close")
public ReactiveFlow<String, Receipt> orders() {
    return Reactive.flow(DefaultNioFlow.from(String.class));
}
```

`ReactiveFlow<I, O>` **is** a `NioFlow<I, O>` — the facade is a subinterface, not a wrapper — so every step you already know keeps working and hands back a reactive builder.

## The steps

| | |
| --- | --- |
| `handleMono(name, call)` | A stage whose work is a `Mono`. Type-preserving. |
| `handleMono(name, call, budget)` | With a budget **on the Mono** — see below, this is not the same as a stage timeout. |
| `handleMono(name, call, retry)` | Each attempt re-subscribes the Mono. |
| `adaptMono(call)` | Re-types **through** a Mono: `T → Mono<R> →` the chain continues at `R`. |
| `adaptFlux(call)` | Collects a `Flux` into the `List` the chain carries. **Buffers it all, with no cap.** |
| `adaptFlux(call, maxItems)` | The same collect, bounded: over the cap, `FlowOverflowException` and the source is cancelled. |
| `fanOutMono(name, branches, join)` | Parallel split-join over reactive branches, each on its own worker. |
| `executeMono()` | The terminal. Lazy, one execution per subscription. |
| `executeFlux(tail)` | The **streaming** terminal: one value out of the engine, then the tail's `Flux`. |

A reactive stage is **not a new kind of link**: `handleMono` appends the same `Stage` every other step appends, whose function parks a virtual worker on the Mono. So it fuses, retries, rate-limits, lands in a lane and reports its metrics *because it is an ordinary stage*.

## The budget belongs on the Mono

```java
.handleMono("fraud", fraud::score, ofMillis(200))   // ✅ cancels the HTTP call
.handle("fraud", fn, ofMillis(200))                 // ❌ abandons the parked worker
```

nioflow's stage timeout cannot cancel what the worker is waiting on: the HTTP request stays alive on the connection pool. `handleMono`'s budget applies `mono.timeout(d)`, which cancels the subscription — reactor-netty then releases the connection. Both surface the same `TimeoutException` to `recover()`.

## `filter()` is `Mono.empty()`, which is your 404

A deliberate cut completes the execution with no value; a `Mono` cannot carry one either. The two notions of "nothing" line up, so the idiomatic ending just works:

```java
return orders.just(id)
        .handle("load", repo::findById)
        .filter(Objects::nonNull)
        .executeMono()
        .switchIfEmpty(Mono.error(new NotFound(id)));   // → 404
```

## Inside a `when` / `match` lane

The branch contracts hard-code `UnaryOperator<Lane<T>>`, and Java will not let a reactive variant both *be* a `Condition` and hand out a reactive lane (same erasure — a name clash, not an override). So a lane gets one unwrap, and that is the only helper the facade needs:

```java
orders.when(Order::highValue)
        .then(lane -> Reactive.lane(lane)                  // ← the one unwrap
                .handleMono("compliance", compliance::file, ofSeconds(5)))
      .handleMono("after", psp::charge);                   // ← still reactive
```

## A `Flux` through the flow

`pipe` is a method on the flow that returns a Reactor `transform`. **Backpressure IS the `concurrency` argument** — it is the number of executions in flight — and Reactor's own operator does the `request(n)` accounting (nioflow does not implement a `Publisher`).

```java
// unordered, 64 in flight (flatMap)
incoming.transform(orders.pipe(64, (order, step) -> step.adapt(Receipt::of)));

// output order = input order (flatMapSequential)
incoming.transform(orders.pipeOrdered(64, (order, step) -> step.adapt(Receipt::of)));

// per-key FIFO with full cross-key parallelism: the ordering lives in the
// ENGINE, so the stream is never serialized
incoming.transform(orders.pipe(256, (order, step) -> step
        .key(order.customerId())
        .adapt(Receipt::of)));
```

Do **not** reach for `inject`/`await` + `OverflowPolicy` here: its `BLOCK` policy parks the calling thread, which on an event loop is the one thing you must never do.

## A `Flux` out of the flow

The opposite direction of `pipe` (which is many inputs *through* one pipeline): **one** input, and a streaming tail. `executeFlux` runs the pipeline for one value and hands that value to the tail — the engine's part is one object, the stream's part is Reactor's, and nothing in between is buffered.

```java
@GetMapping(value = "/orders/{id}/events", produces = TEXT_EVENT_STREAM_VALUE)
Flux<Event> events(@PathVariable String id) {
    return orders.just(id)
            .handle("load", repo::findById)                   // engine: policy, recovery,
            .filter(order -> order.visibleTo(caller()))       // metrics, key, retry — one value
            .executeFlux(order -> events.stream(order.id())); // Reactor: the unbounded part
}
```

It inherits `executeMono()`'s semantics exactly: lazy (nothing runs until the `Flux` is subscribed), one execution per subscription (`.retry()` re-runs the pipeline), a `filter()` cut is an empty `Flux` (so `switchIfEmpty` is still your 404) and a pipeline failure is `onError` — before the tail is ever subscribed.

**The rule: a nioflow value is one object. If you cannot name a bound, do not collect it.** `adaptFlux(call)` has no cap of any kind — a stream of ten million rows becomes a `List` of ten million rows, and the failure mode is an `OutOfMemoryError` that takes the JVM with it. So:

- the size is known small (a fixed lookup, a handful of rows) → `adaptFlux(call)`;
- you can name a bound → `adaptFlux(call, maxItems)`: over the cap the stage fails with `FlowOverflowException` (an ordinary stage failure, so `recover()` catches it) and the source is **cancelled** at `maxItems + 1`, so the overrun costs one element, not the rest of the stream;
- you cannot name one → do not collect it: `executeFlux`.

## Tracing: Reactor context → nioflow context

Declare the keys **once, on the flow**, and every `executeMono()` / `executeFlux()` lifts them out of Reactor's subscriber context into the per-execution `Context`:

```java
static final Context.Key<String> TRACE = Context.Key.of("traceId");

@Bean(destroyMethod = "close")
ReactiveFlow<String, Receipt> orders() {
    return Reactive.<String, Receipt>flow(DefaultNioFlow.from(String.class))
            .propagate(TRACE);                       // ← the whole bridge
}
```

```java
@PostMapping("/orders/{id}/pay")
Mono<Receipt> pay(@PathVariable String id) {         // no traceId parameter anywhere
    return orders.just(id)
            .handleContextual("charge", (order, ctx) -> psp.charge(order, ctx.get(TRACE)))
            .executeMono();                          // TRACE is already in the context
}
```

The keys line up **by name**: `Context.Key.of("traceId")` reads the subscriber-context entry `"traceId"` (the same correspondence that lets a map handed to `engine.call` interoperate). Whatever puts it there — typically a `WebFilter` doing `contextWrite(Context.of("traceId", id))` — is your edge's business, not the flow's.

Four properties worth knowing:

- **It is a whitelist.** A key the config did not name does not cross, ever. Declared-and-automatic, never discovered-and-automatic: no `Hooks`, no Micrometer context propagation, and a reader of the config sees exactly what crosses the boundary. An MDC that is right 99 % of the time is wrong during the incident you bought it for.
- **Absence is defined.** A declared key the subscriber context does not carry is simply not seeded — no throw, no null entry, and `ctx.get` gives back null exactly as for a key nobody ever wrote.
- **Per subscription, never per assembly.** Two subscriptions of the same `Mono` under two different subscriber contexts get their own values, and a `.retry()` re-seeds on every attempt. (This is why the seeding cannot go through `with()`, which writes into the *pipeline*: internally it runs through `NioStep.executeAsync(map)`, whose entries stay in the run.)
- **An explicit `with()` wins** over a propagated key of the same name — that one the caller wrote down here.

`propagate()` unused costs exactly zero: no `deferContextual`, no map, the same one-line terminal `executeMono()` always was.

No write-back: nothing is ever published back into the subscriber context. A stage that wants to publish something publishes it in the value.

## The trap

Operators you chain **onto the Mono** (`.map`, `.doOnNext`, `.filter`) run on the thread that *completes* it — Netty's event loop:

```
controller=webflux-http-nio-4          ← Netty, released immediately
stage=VIRTUAL:                         ← your code: a virtual worker
insideMonoOperator=reactor-http-nio-2  ← a .map() on the Mono: NETTY!
afterReactiveStage=VIRTUAL:            ← the chain resumes on a worker
```

That is Reactor behaving normally, and it is exactly where a blocking call takes the server down. **Do the work in a stage, not in a `map()` on the Mono.** A stage body always runs on a virtual worker.

## The trade-off, measured

A request in flight, parked on a remote call, retains **3 615 B** (an `Execution` plus a parked virtual thread's stack) against **215 B** for a pure Reactor chain — **16.8×**. At 1 000 concurrent that is noise; at 100 000 it is ~360 MB against ~21 MB.

So: if **every** stage is a remote call, **and** concurrency is very high, **and** you use none of the engine (retry, rate limit, batch, `key`, `fork`, `recover`, runtime `splice`, the metrics SPI, chain validation) — use plain Reactor. `Mono.zip` and `flatMap` are the right tools and this library is not a campaign to be in front of everything.

Otherwise the engine is buying you policy, and the price is one parked virtual thread per in-flight call.

**Not supported:** cancellation. If the client disconnects, the pipeline runs to completion and its result is dropped — per-stage budgets are what bound it today.

Full design and rationale: [RFC 0002](https://github.com/fabiangftech/nioflow/blob/main/docs/rfc/0002-webflux.md). Runnable example: `examples/springwebflux-with-nioflow`.
