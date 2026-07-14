# Pipeline API

Every builder method appends a **link** to the chain. This page walks through all of them; the [API reference](api-reference.md) has the compact tables.

## Stages — `handle`

The workhorse: a function on the current value, run on a virtual-thread worker.

```java
flow.handle(order -> order.normalized())          // anonymous
    .handle("price", pricing::apply)              // named: splice anchor + metrics tag
    .handle("reserve", inventory::reserve,
            Duration.ofSeconds(2))                // per-attempt time budget
    .handle("notify", mailer::send,
            Retry.exponential(3, Duration.ofMillis(100)))   // retry with backoff
    .handle("charge", payments::charge,
            Duration.ofSeconds(2),
            Retry.of(3, Duration.ofMillis(100)));           // timeout per attempt + retry
```

- A **timeout** cuts the attempt with a `TimeoutException` — recoverable downstream like any failure.
- A **retry** re-runs failed attempts with backoff on the worker; the last failure flows to recovery. Layering order: rate limit → timeout per attempt → retry over attempts → `recover()`.
- Consecutive plain stages are **fused**: they travel to a worker as one composed function, paying two thread hops per run instead of per stage.

### Boss-inlined stages — `handleSync`

For pure-CPU, sub-microsecond functions, skip both thread hops:

```java
flow.handleSync("mask", card -> card.masked());
```

Same contract as `when()` predicates: cheap, never blocking. There are deliberately no timeout/retry variants — nothing can cut an inlined call.

### Context-aware stages — `handleContextual`

Per-execution scratch state shared across stages, without threading it through the value type:

```java
static final Context.Key<String> USER = Context.Key.of("user");

flow.handleContextual("auth", (order, ctx) -> {
        ctx.put(USER, resolveUser(order));
        return order;
    })
    .handle("price", pricing::apply)              // plain stages never see the context
    .handleContextual("audit", (order, ctx) ->
        order.withAuditTrail(ctx.get(USER)));
```

Keys are name-based, so a map handed to `engine.call(input, map)` interoperates entry by entry.

## Re-typing — `adapt`

The step that moves the pipeline from one type to the next. It lives on the **per-request** builder (the shared definition is type-preserving, which is what lets `just()` start at the input type):

```java
NioFlow<OrderRequest, Receipt> flow = DefaultNioFlow.from(OrderRequest.class);

Receipt receipt = flow.just(request)     // OrderRequest
        .adapt(pricing::price)           // -> Order
        .handle("tax", pricing::withTax) // still an Order
        .adapt(Receipt::from)            // -> Receipt
        .execute();                      // Receipt
```

`fanOut`, `batch` and `use(segment)` re-type the value too. Everything else preserves it.

## Cutting the flow — `filter`

A failed predicate ends the execution deliberately: `execute()` returns `null`, `executeResult()` returns `Filtered`.

```java
flow.filter(order -> order.total() > 0);
```

## Side effects — `background`

Fire-and-forget on a worker. It never delays and never fails the flow; a throwing effect only reports to the error handlers.

```java
flow.background("audit", audit::record);
```

## Branching — `when` and `match`

```java
flow.when(order -> order.express())
    .then(lane -> lane.handle("fast", shipping::overnight))
    .otherwise(lane -> lane.handle("standard", shipping::ground))
    .handle("label", shipping::label);      // main line: runs for every value
```

`match()` is first-match-wins — later cases are not even evaluated after a hit:

```java
flow.match()
    .is(o -> o.total() > 5_000, lane -> lane.handle("manual-review", risk::queue))
    .is(o -> o.newCustomer(),   lane -> lane.handle("light-check", risk::score))
    .otherwise(lane -> lane.handle("approve", risk::approve));
```

Inside a lane you get the same API (`handle`, `filter`, `recover`, `batch`, nested `when`/`match`…) but no `execute`/`just` — lanes only build steps. Branches nest and their routing guards compose automatically.

## Detached sub-flows — `fork`

`background` is a fire-and-forget *step*. `fork` is a fire-and-forget **pipeline**: the value is handed to a child execution and the main line keeps going — the request does not wait for it, its latency does not include it, and a failure the fork does not recover never reaches the caller.

```java
flow.handle("price", pricing::apply)
    .fork("audit", sub -> sub                          // detached: nobody waits
            .adapt(AuditRecord::of)
            .when(AuditRecord::highValue)
                .then(lane -> lane.handle("compliance", compliance::file))
                .otherwise(lane -> lane.background("log", audit::debug))
            .handle("persist", auditRepo::save, Duration.ofSeconds(2))
            .recover(AuditRecord::failed))             // the fork's own net
    .handle("charge", payments::charge);               // runs without waiting for "audit"
```

The sub-flow is a real pipeline: every step works inside it (`handle`, `handleSync`, `handleContextual`, `background`, `adapt`, `filter`, `recover`, `fanOut`, `batch`, `use`, `when`/`match`, and nested forks), its stages report their own metrics, and its name is a splice anchor — `splice("audit", REPLACE, …)` swaps the whole sub-flow at runtime. Because the body is a `Segment`, it is reusable and testable on its own.

Three things to keep in mind:

- **Nothing comes back.** A fork gives no value to the main line — if you need the result, you want [`fanOut`](#parallel-split-join--fanout), which waits.
- **Failures go to `onError`**, never to the caller's future. A fork with no `recover()` and no error handler fails silently, exactly like a throwing `background`.
- **The context is copied** at the fork point: the fork reads what the main line had written so far, and its own writes stay inside the fork (parent and child run concurrently — sharing the map would be a data race).

Forks are in-flight work: `shutdown(grace)` waits for them, and `NioFlowMetrics` reports `forkStarted` / `forkCompleted` / `forkFailed` / `forksInFlight`.

## Parallel split-join — `fanOut`

Branches run concurrently on workers, the join combines results **in declaration order**:

```java
List<Function<Order, Object>> branches = List.of(
        o -> stockService.check(o),
        o -> fraudService.score(o),
        o -> loyaltyService.points(o));

flow.fanOut("enrich", branches, results -> Enriched.of(results));
```

A failing branch fails the fan-out — recoverable downstream. Use it for real work: trivial branches pay more in coordination than they win in parallelism.

## Coalescing — `batch`

Executions park at the link until `size` of them accumulated or `window` elapsed, then **one** bulk call maps all their values positionally. Callers never see the batch — each future completes with its own element:

```java
flow.batch("bulk-insert", 16, Duration.ofMillis(10), orders ->
        repository.insertAll(orders));     // List<Order> -> List<SavedOrder>, same size & order
```

One bulk failure (or a wrong-sized result) fails every batched execution — recoverable per execution. See [Scaling](scaling.md) for when this pays.

## Reusable pieces — `Segment` and `use`

A `Segment<T, R>` is a chapter of pipeline you can test in isolation and embed anywhere:

```java
Segment<Order, Order> fraudGate = lane -> lane
        .handle("score", risk::score)
        .filter(order -> order.riskScore() < 80);

flow.use(fraudGate)                      // inline, zero runtime footprint
    .use("shipping", shippingSegment);   // NAMED: becomes a swappable region
```

Naming the embedding turns it into a **region** — the unit of atomic runtime replacement. [Runtime editing →](runtime-editing.md)

## Error handling — `recover`

Positional: it catches failures from links **upstream** of it, and the flow continues after it with the recovered value.

```java
flow.handle("charge", payments::charge, Duration.ofSeconds(2))
    .recover("charge-fallback", error -> Payment.deferred())   // catches the timeout too
    .handle("receipt", Receipt::from);
```

Inside a lane, `recover` inherits the lane's guards: it only catches failures of values routed through that branch.

## Observing outcomes — `onComplete` / `onError`

```java
// On the shared definition: EVERY execution reports here.
flow.onComplete(receipt -> metrics.count("orders"))
    .onError(error -> alerting.page(error));

// On one execution: scoped callbacks, guaranteed to fire before execute() returns.
flow.just(request)
    .onComplete(r -> log.info("done {}", r))
    .execute();
```

On the shared flow, `onError` is the engine's error tap: it also sees rejected/dropped values and failing background effects.

## Ordering — `key`

Executions sharing a key run strictly one at a time, in submission order — Kafka-partition semantics per business entity:

```java
flow.just(payment).key(payment.accountId()).executeAsync();
```

Distinct keys keep full parallelism. [Scaling →](scaling.md)

## Fire-and-forget — `justAll`, `inject`, `await`

Request/response is `just(...).execute()`. For pipeline-style feeding there is the fire-and-forget pair on the engine:

```java
flow.justAll(List.of(a, b, c));   // inject through the shared chain
Object first = engine.await();    // collect results as they finish
```

Bound it with backpressure: `new DefaultNioEngine(1_000, OverflowPolicy.BLOCK)` — see [Scaling](scaling.md).
