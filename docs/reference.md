# API reference

The authoritative reference is the Javadoc (`./gradlew javadoc` from `core/`). This page is the map.

Entry point: `dev.nioflow.application.facade.DefaultNioFlow<T>` — implements the `dev.nioflow.core.facade.NioFlow<T>` contract and `AutoCloseable`.

## Constructors

| Constructor | Use when |
|---|---|
| `new NioFlow<>()` | Default: everything on virtual threads, unbounded admission. |
| `new NioFlow<>(Backpressure bp)` | Virtual threads + admission control. |
| `new NioFlow<>(ExecutorService ex)` | Your executor for `submit` stages; you keep its lifecycle. |
| `new NioFlow<>(ExecutorService ex, int handleWorkers)` | Plus a fixed handle-worker pool bounding sync parallelism (CPU-heavy chains). |
| `new NioFlow<>(ExecutorService ex, int handleWorkers, Backpressure bp)` | Fully tuned. |

## Injecting values

| Operator | Description |
|---|---|
| `just(input)` | Injects one value; it starts walking the chain immediately. |
| `just(input, context)` | Same, with seed metadata for `FlowContext`. |
| `justAll(inputs)` | Injects every value in iteration order, honoring backpressure per value. |

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
| `seal()` | Freezes the chain (further links throw) and releases finished values. Seal every stream-style flow. |
| `join()` | Waits for quiescence; returns the newest injected value's result; rethrows a recorded failure once. |
| `join(timeout)` | Same, bounded by a timeout (`CompletionException` wrapping `TimeoutException`). |
| `close()` | Graceful: drains up to 10 s, then stops the engine. Idempotent. Never shuts down your executor. |
| `close(gracePeriod)` | Custom grace period. |

## Request/response (web)

`NioFlowGateway<T, R>` bridges request-driven callers (Spring WebMVC, WebFlux, RPC) into a running flow: each `call` injects one value and returns a `CompletableFuture` completed with **that value's own result** — unlike `join()`, which waits for the whole flow. Declare and seal the chain once at startup; serve each request with a `call`.

| Method | Description |
|---|---|
| `NioFlowGateway.of(flow)` | Bridges a chain that keeps its type (`<T, T>`). |
| `NioFlowGateway.of(entry, exit)` | Re-typed chain: inject through `entry`, observe results on the final view `exit`. |
| `call(input)` | Injects and returns the value's own future. Rejected admission fails the future instead of throwing. |
| `call(input, context)` | Same, with seed metadata for `FlowContext` (`"nioflow.gateway.id"` is reserved). |
| `call(input, timeout)` | Bounded: `TimeoutException` if the value is slow — or dropped by a `filter`. Prefer this in web handlers. |
| `pending()` | Calls still waiting for their result. |

Spring adapters (both generic, `compileOnly` dependencies):

| Adapter | Returns | Extra dependency |
|---|---|---|
| `NioFlowMvc.deferred(gateway, input[, timeout])` | `DeferredResult<R>` for async WebMVC controllers | `org.springframework:spring-web` |
| `NioFlowReactive.mono(gateway, input[, timeout])` | cold `Mono<R>` for WebFlux handlers | `io.projectreactor:reactor-core` |

WebMVC controllers may also return the gateway's `CompletableFuture` directly — Spring handles it natively. Register `NioFlow` beans normally: they are `AutoCloseable`, so Spring closes them on context shutdown.

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
| `NioFlowGateway<T, R>` | `NioFlowMvc.deferred(...)` (WebMVC) | `org.springframework:spring-web` |
| `NioFlowGateway<T, R>` | `NioFlowReactive.mono(...)` (WebFlux) | `io.projectreactor:reactor-core` |

All ports are functional or default-method interfaces — implement them with a lambda when an adapter is overkill.

## Context

`dev.nioflow.core.model.FlowContext` — static access to the current value's metadata, bound around every execution of user code:

| Method | Description |
|---|---|
| `FlowContext.get(key)` | The current value's metadata, or `null` when absent/unbound. |
| `FlowContext.put(key, value)` | Adds metadata visible to every later stage of this value. Throws outside a bound execution. |

Seed it with `just(input, context)`; fan-out children inherit a copy; batch functions run unbound.
