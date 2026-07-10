# Examples

All examples assume:

```java
import dev.nioflow.application.facade.DefaultNioFlow;
```

## Branching with `when`

A two-way fork. Each branch builds its own visually nested lane; a value only runs the stages of its lane. Stages chained after the fork are back on the main line and run for **every** value.

```java
flow.when(x -> x > 10)
    .then(lane -> lane
            .handle(x -> x * 2)
            .submit(x -> slowIo(x)))
    .otherwise(lane -> lane
            .handle(x -> x - 1))
    .handle(x -> audit(x));   // main line: runs for both lanes
```

`otherwise` is optional — without it, non-matching values skip the lane unchanged.

## Switch-style branching with `match`

Cases are tried in declaration order; the first matching predicate wins and later predicates are not evaluated. Unmatched values take `otherwise`, or pass through unchanged without it.

```java
flow.match()
    .is(x -> x > 100, lane -> lane
            .submit(x -> big(x)))
    .is(x -> x > 10, lane -> lane
            .handle(x -> medium(x)))
    .otherwise(lane -> lane
            .handle(x -> small(x)))
    .handle(x -> audit(x));   // main line again
```

## Changing the type with `adapt`

`adapt` converts the value and returns a re-typed view over the same running flow. Register `onComplete` on the **final** view, after the last `adapt`.

```java
NioFlow<Order> orders = new DefaultNioFlow<>();
orders.handle("validate", o -> validate(o))
      .adapt(o -> toInvoice(o))            // NioFlow<Invoice> from here on
      .submit(invoice -> send(invoice))
      .onComplete(invoice -> log.info("sent {}", invoice));
```

## Splitting one value into many with `fanOut`

Each element of the returned list continues down the chain as its own independent value; the parent is consumed. An empty list drops the value like a filter.

```java
flow.submit(order -> loadItems(order))     // produce with IO first...
    .fanOut(order -> order.items())        // ...then split (fanOut runs like a handle)
    .submit(item -> reserveStock(item));
```

## Filtering

Dropped values leave deliberately: they fire neither `onComplete` nor `onError`, stop counting toward `join()` and free their backpressure slot.

```java
flow.filter(event -> event.relevant())
    .submit(event -> process(event));
```

## Bulk IO with `batch`

Values are grouped until `size` of them arrived or the oldest waited `maxWait`; one async call processes the whole group. The function must return exactly one result per input, matched by index. A failure fails every value of the group — each one individually recoverable downstream.

```java
flow.batch(50, Duration.ofMillis(200), rows -> jdbc.batchInsert(rows))
    .onErrorResume(error -> Row.dead(error));
```

## Reusable segments with `via`

Define a sub-flow once and splice it anywhere — including inside lanes. The segment may change the type.

```java
static Function<NioFlow<Order>, NioFlow<Invoice>> billing() {
    return f -> f
            .submit(o -> price(o))
            .adapt(o -> invoice(o));
}

flow.via(billing())
    .submit(invoice -> send(invoice));
```

## Request/response with `call`

Each `call` injects one value and returns a future resolved with **that value's own** outcome — many calls fly concurrently, and a failure fails only its own future. The fit for web handlers sharing one long-lived flow:

```java
flow.handle("validate", o -> validate(o))
    .submit("price", o -> price(o))
    .onErrorResume(error -> Order.rejected(error))
    .release();

CompletableFuture<Order> reply = flow.call(order, Duration.ofSeconds(2));
```

A value dropped by a `filter` (or an empty `fanOut`) **cancels** its future; prefer the `timeout` variants in request-driven callers anyway.

## Editing the chain at runtime

Structural edits anchor on **named stages** and are copy-on-write: values in flight finish on the version they entered; values injected afterwards walk the new chain. `replace` remembers what it spliced under that name — a later `replace` swaps the whole segment, `remove` takes it all out:

```java
flow.handle("routes", e -> e)          // the editable anchor
    .submit("store", e -> store(e))
    .release();                        // flat memory, chain stays editable

// later, from any thread — e.g. an admin endpoint:
flow.replace("routes", f -> f.match()
        .is(e -> e.isBilling(), lane -> lane.submit(e -> billing(e)))
        .is(e -> e.isAudit(),   lane -> lane.handle(e -> audit(e))));

flow.remove("routes");                 // removes the whole spliced segment
```

Segments only declare structure — injecting or registering handlers inside one throws — and inherit the anchor's lane, so editing inside a fork stays inside that fork.

## Scoped flows: stages at the call site

`scoped()` opens an ephemeral, caller-private chain over the same engine. Links declared on the scope never touch the shared chain; concurrent scopes never interfere; `join()` waits only for the scope's own values. One empty shared flow can serve every caller:

```java
NioFlow<String> flow = new DefaultNioFlow<>();   // shared, empty — e.g. a Spring bean

String greeting = flow.scoped()
        .just("Hello")
        .handle("greeting", s -> s + ", World!")
        .join();                                 // "Hello, World!", every time

// or the async style, one scope per request:
CompletableFuture<Quote> quote = flow.scoped()
        .submit(r -> priceRemote(r))
        .adapt(r -> toQuote(r))
        .call(request, Duration.ofSeconds(2));
```

Scope values ride the shared engine (threads, executor, backpressure) and are released on finish. `onComplete`/`onError` on a scope observe only that scope; structural edits, `metrics` and `trace` throw on one; closing a scope never stops the shared engine.

## Backpressure

Bound the number of values in flight and choose what `just` does at capacity:

```java
new DefaultNioFlow<>(Backpressure.blocking(1_000));   // producer waits for a free slot
new DefaultNioFlow<>(Backpressure.dropping(1_000));   // new values silently discarded
new DefaultNioFlow<>(Backpressure.failing(1_000));    // just() throws RejectedExecutionException
```

Only injection is bounded — values already flowing are never lost to backpressure.

> Careful with `blocking` and stages that inject new values into the same flow: a worker blocked inside `just` cannot finish its own value, so a full flow where every worker is injecting deadlocks. Prefer `dropping`/`failing` or capacity headroom for feedback loops.

## Timeouts

Bound an async stage; on expiry the worker is interrupted and only that value fails, with a `TimeoutException`:

```java
flow.submit(x -> slowRemoteCall(x), Duration.ofSeconds(2))
    .onErrorResume(error -> cachedFallback());
```

## Resilience: retry, circuit breaker, rate limiter, bulkhead

The `Resilience<T>` port decorates a stage function. Implement it with a lambda, or use the [Resilience4j](https://resilience4j.readme.io/) adapter — in that case add the dependency yourself (the core only compiles against it):

```groovy
implementation 'io.github.resilience4j:resilience4j-all:2.4.0'
```

```java
import dev.nioflow.infrastructure.resilience.Resilience4j;

// Preconfigured retry with exponential backoff
flow.submit(x -> flakyCall(x), Resilience4j.retry(3, Duration.ofMillis(100)));

// Any Resilience4j instance, fully configured by you
CircuitBreaker breaker = CircuitBreaker.ofDefaults("payments");
flow.submit(x -> charge(x), Resilience4j.circuitBreaker(breaker));

// Policies compose: retry, then breaker around it
flow.submit(x -> call(x),
        Resilience4j.<X>retry(3, Duration.ofMillis(50))
                .andThen(Resilience4j.circuitBreaker(breaker)));
```

An exhausted policy fails only that value, like any other stage error. Blocking policies (retry backoff, rate limiter, bulkhead) are fine on `submit` stages and on the default virtual handle workers; on a **fixed** handle-worker pool keep `handle` policies non-blocking (e.g. circuit breaker).

## Observability

### Metrics

Register a `NioFlowMetrics` sink before injecting values. With the [OpenTelemetry](https://opentelemetry.io/) adapter (add `io.opentelemetry:opentelemetry-api` to your build):

```java
import dev.nioflow.infrastructure.metrics.OpenTelemetryMetrics;

flow.metrics(OpenTelemetryMetrics.of(openTelemetry.getMeter("orders")));
```

Instruments: `dev.nioflow.values.injected/completed/failed/dropped` counters, a `dev.nioflow.values.in_flight` up-down counter, and a `dev.nioflow.stage.duration` histogram with stage name, kind and outcome attributes.

### Tracing

Opt-in per-value trace of every transition — stages in/out, lanes, drops, splits, recoveries, failures, completions. Zero cost when no tracer is registered.

```java
import dev.nioflow.infrastructure.trace.LoggingTracer;

flow.trace(LoggingTracer.debug());   // JDK System.Logger, level DEBUG
```

Or implement `NioFlowTracer` yourself and ship transitions wherever you want.

### Diagnostics

A point-in-time snapshot of the running flow — chain shape, queue depths, active/parked/batched counts. Logging the flow itself is enough:

```java
log.info("{}", flow);
// NioFlow[active=3, parked=0, batched=2, submissionQueue=1, ...]
//   1. handle[validate]
//   2. submit[enrich] timeout=PT2S
//   3. when#0
//   ...
```

## Context propagation

Per-value metadata (trace id, tenant, ...) travels with the value, not the thread. Seed it at injection; read or extend it from any stage, no matter which thread runs it:

```java
import dev.nioflow.core.model.FlowContext;

flow.handle(order -> {
        FlowContext.put("step", "validated");
        return validate(order);
    })
    .submit(order -> {
        var traceId = FlowContext.get("traceId");   // works on the executor too
        return enrich(order, traceId);
    });

flow.just(order, Map.of("traceId", "abc-123"));
```

Fan-out children inherit a copy of the parent's context. Batch functions process many values at once and run unbound.

## Graceful shutdown

```java
flow.close();                          // drain up to 10s, then stop
flow.close(Duration.ofSeconds(30));    // custom grace period
```

`close` never shuts down an executor you supplied — you keep its lifecycle. It is idempotent, and producers blocked on backpressure are released with a `RejectedExecutionException`.
