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
| `adaptFlux(call)` | Collects a `Flux` into the `List` the chain carries. **Buffers it all** — bounded results only. |
| `fanOutMono(name, branches, join)` | Parallel split-join over reactive branches, each on its own worker. |
| `executeMono()` | The terminal. Lazy, one execution per subscription. |

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

## Tracing: Reactor context → nioflow context

```java
static final Context.Key<String> TRACE = Context.Key.of("traceId");

return Mono.deferContextual(view -> orders.just(id)
        .with(TRACE, view.get("traceId"))            // seeds the per-execution Context
        .handleContextual("charge", (order, ctx) -> psp.charge(order, ctx.get(TRACE)))
        .executeMono());
```

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
