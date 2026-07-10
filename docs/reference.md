# API reference

The authoritative reference is the Javadoc (`./gradlew javadoc` from `core/`). This page is the map.

Entry point: `dev.nioflow.application.facade.DefaultNioFlow<T>` — implements the `dev.nioflow.core.facade.NioFlow<T>` contract and `AutoCloseable`.

## Constructors

| Constructor | Use when |
|---|---|
| `new DefaultNioFlow<>()` | Default: everything on virtual threads, unbounded admission. |
| `new DefaultNioFlow<>(Backpressure bp)` | Virtual threads + admission control. |
| `new DefaultNioFlow<>(ExecutorService ex)` | Your executor for `submit` stages; you keep its lifecycle. |
| `new DefaultNioFlow<>(ExecutorService ex, int handleWorkers)` | Plus a fixed handle-worker pool bounding sync parallelism (CPU-heavy chains). |
| `new DefaultNioFlow<>(ExecutorService ex, int handleWorkers, Backpressure bp)` | Fully tuned. |

## Injecting values

| Operator | Description |
|---|---|
| `just(input)` | Injects one value; it starts walking the chain immediately. |
| `just(input, context)` | Same, with seed metadata for `FlowContext`. |
| `justAll(inputs)` | Injects every value in iteration order, honoring backpressure per value. |
| `call(input[, context][, timeout])` | Request/response injection: returns that value's own `CompletableFuture`. See [below](#requestresponse-call). |

## Stages

The one distinction that matters: **`handle` is synchronous, `submit` is asynchronous**. Both accept a `Function<T, T>` and both can do IO (JDBC, HTTP, ...).

| Operator | Runs on | Description |
|---|---|---|
| `handle(fn)` | handle worker | Sync: runs to completion before the value moves on. Blocking (IO included) is fine on the default virtual workers. |
| `handle(name, fn)` | handle worker | Named: failures arrive wrapped in `StageException`. |
| `handle(fn, resilience)` | handle worker | Decorated by a `Resilience` policy. |
| `submit(fn)` | executor | Async: the engine fires and moves on without waiting; the result is reaped later. |
| `submit(name, fn)` | executor | Named async stage. |
| `submit(fn, timeout)` | executor | Bounded: on expiry the worker is interrupted, the value fails with `TimeoutException`. |
| `submit(fn, resilience)` | executor | Decorated async stage. |
| `batch(size, maxWait, fn)` | executor | Groups values; one async call per group; one result per input, matched by index. |
| `adapt(fn)` | handle worker | Changes the value type; returns a re-typed view of the same flow. |
| `fanOut(fn)` | handle worker | Splits one value into many independent ones; empty list drops it. |
| `via(segment)` | — | Splices a reusable sub-flow (`Function<NioFlow<T>, NioFlow<N>>`) at this point. |

## Routing

| Operator | Description |
|---|---|
| `filter(predicate)` | Deliberate drop: no handlers fire, `join()` stops counting it, backpressure slot freed. |
| `when(predicate).then(lane)` | Two-way fork; lane runs only for matching values. |
| `...otherwise(lane)` | Optional false lane. Stages after the fork are main line again. |
| `match().is(predicate, lane)...` | Switch-style fork: first matching case wins, in declaration order. |
| `...otherwise(lane)` | Default lane for unmatched values; without it they pass through unchanged. |

## Errors

| Operator | Description |
|---|---|
| `onErrorResume(fallback)` | Recovery link: turns an upstream failure into a replacement value; flow resumes from here. Only catches failures of links declared before it. |
| `onError(handler)` | Observes terminal failures (after all recoveries). Recent failures are replayed to late registrations — bounded history. |

## Observability

| Operator | Description |
|---|---|
| `metrics(sink)` | Registers the `NioFlowMetrics` sink (lifecycle counters + per-stage latency). Second call replaces the first. |
| `trace(tracer)` | Opt-in `NioFlowTracer`: every transition of every value. Zero cost unregistered. |
| `diagnostics()` | Point-in-time snapshot: chain shape, queue depths, counters. `toString()` renders the dump. |
| `onComplete(handler)` | Runs each time a value finishes the chain. Register on the final view, after any `adapt`. |

## Lifecycle

| Operator | Description |
|---|---|
| `seal()` | Freezes the chain (further links and edits throw) and releases finished values. Seal every fixed, stream-style flow. |
| `release()` | Releases finished values like `seal()`, but the chain stays open to appends and runtime edits. The mode for long-lived, editable service flows. |
| `join()` | Waits for quiescence; returns the newest injected value's result; rethrows a recorded failure once. |
| `join(timeout)` | Same, bounded by a timeout (`CompletionException` wrapping `TimeoutException`). |
| `close()` | Graceful: drains up to 10 s, then stops the engine. Idempotent. Never shuts down your executor. |
| `close(gracePeriod)` | Custom grace period. |

## Request/response (`call`)

`call` is the request/response form of injection — native to `NioFlow`, no bridge class needed. Each `call` injects one value and returns a `CompletableFuture` resolved with **that value's own outcome** — unlike `join()`, which waits for the whole flow. Declare the chain once at startup; serve each request with a `call`. This is the fit for Spring controllers — see [Spring Boot](springboot.md).

| Method | Description |
|---|---|
| `call(input)` | Injects and returns the value's own future. Rejected admission (closed flow, FAIL backpressure) fails the future instead of throwing. |
| `call(input, context)` | Same, with seed metadata for `FlowContext`. |
| `call(input, timeout)` | Bounded: the future fails with `TimeoutException` if the value is slow; the value keeps flowing. Prefer this in web handlers. |
| `call(input, context, timeout)` | Metadata + bound combined. |

How the future resolves:

- **Completed** with the value's end-of-chain result. A `fanOut` resolves it with the first split-off value to finish.
- **Failed** with the terminal error, after every `onErrorResume` had its chance.
- **Cancelled** when the value leaves deliberately: dropped by a `filter`, an empty `fanOut`, or a `Backpressure.dropping` admission.

The result type is stated at the call site (`CompletableFuture<Invoice> f = flow.call(order)`) — the engine is untyped, like with `adapt`.

## Runtime editing

Structural edits start a **new version** of the chain (copy-on-write): values already in flight — recoveries and lanes included — finish on the version they were injected into; values injected after the edit walk the new one. Edits anchor on **named stages**, run under the engine lock (safe from any thread) and throw `IllegalStateException` on a sealed flow.

| Operator | Description |
|---|---|
| `remove(name)` | Takes out the named stage — or the whole region a previous `replace` spliced under that name. |
| `replace(name, segment)` | Swaps the named stage for the segment's links. The links are remembered as the name's **region**: a later `replace` swaps the whole segment again. |
| `insertBefore(anchor, segment)` | Splices the segment right before the anchor. |
| `insertAfter(anchor, segment)` | Splices the segment right after the anchor. |

Segments build with the full fluent API (stages, filters, forks, recoveries) on a detached view — injecting, joining or registering handlers inside one throws. Spliced links inherit the anchor's lane, so editing inside a fork stays inside that fork. Values parked at the end of the old version are released by an edit.

```java
flow.replace("routes", f -> f.match()
        .is(v -> isBilling(v), lane -> lane.submit(v -> billing(v)))
        .is(v -> isAudit(v),   lane -> lane.handle(v -> audit(v))));
```

## Scoped flows

`scoped()` hands out an ephemeral, caller-private view over the same engine: links declared on it never touch the shared chain, and concurrent scopes never see each other. The pattern for one shared (even empty) flow serving many callers that each declare their own stages.

| Behavior | Detail |
|---|---|
| Chain | Starts from a snapshot of the shared chain; every link declared on the scope is private. |
| `just` / `justAll` | Buffered — links and values may be declared in any order. |
| `join()` / `join(timeout)` | Flushes the buffered values through the scope's chain and waits for **those values only**. |
| `call(...)` | Dispatches one value immediately with the links declared so far. |
| `onComplete` / `onError` | Scope-local: observe only the scope's values. |
| Memory | Scope values are released on finish — no `seal()` needed. |
| Rejected on a scope | Structural edits, `metrics`, `trace` (global concerns). `close()` is a no-op — it never stops the shared engine. |

```java
flow.scoped()
    .just("Hello")
    .handle("greeting", s -> s + ", World!")
    .join();                             // "Hello, World!" — the shared chain stays untouched
```

## Backpressure

`dev.nioflow.core.model.Backpressure` — admission control for `just`:

| Factory | At capacity, `just`... |
|---|---|
| `Backpressure.unbounded()` | never limits (default). |
| `Backpressure.blocking(n)` | blocks the producer until a slot frees. |
| `Backpressure.dropping(n)` | silently discards the new value. |
| `Backpressure.failing(n)` | throws `RejectedExecutionException`. |

## Ports & adapters

| Port | Adapter | Extra dependency |
|---|---|---|
| `Resilience<T>` | `Resilience4j.retry / circuitBreaker / rateLimiter / bulkhead` | `io.github.resilience4j:resilience4j-all` |
| `NioFlowMetrics` | `OpenTelemetryMetrics.of(meter)` | `io.opentelemetry:opentelemetry-api` |
| `NioFlowTracer` | `LoggingTracer.debug() / info() / to(logger, level)` | none (JDK `System.Logger`) |

All ports are functional or default-method interfaces — implement them with a lambda when an adapter is overkill.

## Context

`dev.nioflow.core.model.FlowContext` — static access to the current value's metadata, bound around every execution of user code:

| Method | Description |
|---|---|
| `FlowContext.get(key)` | The current value's metadata, or `null` when absent/unbound. |
| `FlowContext.put(key, value)` | Adds metadata visible to every later stage of this value. Throws outside a bound execution. |

Seed it with `just(input, context)`; fan-out children inherit a copy; batch functions run unbound.
