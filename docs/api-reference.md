# API reference

Compact reference for the public surface. Types live in `dev.nioflow.core.facade` and `dev.nioflow.core.model`; implementations in `dev.nioflow.application.facade`.

## Entry points

| Method | Returns | Notes |
|---|---|---|
| `DefaultNioFlow.from(Class<I>)` | `DefaultNioFlow<I, O>` | Own engine on the shared JVM pools; the Class token lets `just()` reject a wrong input |
| `DefaultNioFlow.from(Class<I>, NioEngine)` | `DefaultNioFlow<I, O>` | Bring your own engine |
| `DefaultNioFlow.create()` | `DefaultNioFlow<I, O>` | No Class token — for generic beans (`NioFlow<?, ?>`); no input check |
| `flow.just(input)` | `NioStep<I, O>` | Opens an isolated per-request pipeline, **starting at the input type** |
| `flow.pipeline(segment)` | `Pipeline<I, R>` | A **prebuilt** per-request pipeline: recorded, validated and compiled once — see below |
| `flow.justAll(iterable)` | — | Fire-and-forget through the shared chain; collect with `engine.await()` |

## The two types

`NioFlow<I, O>` is the **shared definition** (the bean): `I` goes in, `O` comes out. Its steps are type-preserving — that is what lets `just()` hand you a builder that starts at `I`.

`NioStep<T, O>` is the **per-request pipeline** that `just()` returns: `T` is the value's type right here. `adapt`, `fanOut`, `batch` and `use` move `T`; the terminals return `T`, so the compiler tells you when a pipeline has not reached the `O` your method promised.

## `NioFlow<I, O>` — the shared definition (type-preserving over `I`)

| Method | Effect |
|---|---|
| `handle(fn)` / `handle(name, fn)` | Stage on a virtual worker |
| `handle(name, fn, Duration)` | Stage with a per-attempt time budget |
| `handle(name, fn, Retry)` | Stage with retry + backoff |
| `handle(name, fn, Duration, Retry)` | Budget per attempt, retry over attempts |
| `handle(name, fn, RateLimit)` | Token-bucket gated stage |
| `handleAsync(name, call[, Duration][, Retry])` | **The stage that holds no thread**: the function returns a `CompletionStage`, a worker invokes it and leaves — for `HttpClient.sendAsync`, the AWS SDK, a reactive driver, in core with no Reactor. The timeout **cancels** the call |
| `handleSync(fn)` / `handleSync(name, fn)` | Boss-inlined stage — pure CPU, sub-µs, never blocking |
| `handleContextual((v, ctx) -> …)` | Stage with the typed per-execution `Context` |
| `filter(predicate)` | Deliberate cut; `execute()` maps it to `null` |
| `background(name, effect)` | Fire-and-forget side effect |
| `fanOut(name, branches, join)` | Parallel split-join, declaration-order combine |
| `fanOutAsync(name, branches, join)` | The same split-join with **`CompletionStage` branches**: N concurrent calls, zero parked workers |
| `fork(name, segment)` | **Detached sub-flow**: a full pipeline the main line does not wait for |
| `batch(name, size, window, bulk)` | Cross-execution coalescing; positional bulk mapping |
| `use(segment)` / `use(name, segment)` | Embed a `Segment<I, I>`; naming it creates a swappable region |
| `recover(fn)` / `recover(name, fn)` | Positional error handler |
| `when(pred).then(lane).otherwise(lane)` | Boolean fork |
| `match().is(pred, lane)….otherwise(lane)` | First-match-wins cases |
| `onComplete(cb)` / `onError(cb)` | Outcome taps — engine-wide on the definition, scoped on an execution |

## `NioStep<T, O>` — the per-request pipeline

Everything above (over `T` instead of `I`), plus the steps that re-type the value and the terminals:

| Method | Effect |
|---|---|
| `adapt(fn)` | Re-types the value — the step the compiler follows |
| `adaptAsync(call[, Duration])` | Re-types **through** a `CompletionStage` without holding a thread — `adaptAsync` to `handleAsync` what `adapt` is to `handle` |
| `fanOut(...)` / `fanOutAsync(...)` / `batch(...)` / `use(segment)` | Also re-type (to `C` / `C` / `R` / `R`) |
| `key(k)` | Per-key FIFO ordering |
| `with(Context.Key, value)` | Seeds the per-execution `Context` **before** the pipeline runs (a trace id, a principal) |
| `execute()` | Blocks; returns the value's **current** type; `null` on a filter cut |
| `executeAsync()` / `executeAsync(map)` | `CompletableFuture<T>` — non-blocking; the overload seeds a per-run context |
| `executeResult()` | `FlowResult<T>` — `Completed(value)` vs `Filtered` vs `Cancelled`, pattern-matchable |
| `executeCancellable([map])` | `Cancellable<T>` — the future plus an idempotent `cancel()`; a cooperative stop that the reactive `executeMono()` is built on |

`Lane<T>` (inside branch lanes, segments and fork sub-flows) exposes the same building methods, minus `just`/`execute`/lifecycle.

## `Pipeline<I, R>` — a prebuilt per-request pipeline

The `just(...)…execute()` path rebuilds and re-interprets the chain per request. When the pipeline is the same every time (its lambdas do not close over the input), declare it once: `flow.pipeline(segment)` records the segment, **validates** it and **compiles** it a single time, and each request only allocates its run.

| Method | Effect |
|---|---|
| `flow.pipeline(segment)` | `Pipeline<I, R>` — recorded, validated, compiled once (throws `ChainValidationException` at build) |
| `pipeline.just(input)` | `PipelineRun<R>` — carries only what varies per request, dispatches off the plan |
| `run.key(k)` / `run.with(key, value)` / `run.onComplete/onError(cb)` | Per-request configuration, as on a `NioStep` |
| `run.execute()` / `executeAsync([map])` / `executeResult()` / `executeCancellable([map])` | The same terminals as `NioStep` |

A `Pipeline` snapshots the shared definition at build time, so a later runtime `splice` does not reach it — a prebuilt pipeline is stable by design. Keep `just(...)…execute()` for pipelines that genuinely vary per request.

## `ReactiveFlow` / `ReactiveStep` / `ReactiveLane` — WebFlux

Subinterfaces of the three above, so a reactive step is a step like any other. They live in `dev.nioflow.infrastructure.reactive` — the same package as always, now shipped by its own artifact, `dev.nioflow:nioflow-reactive` (declare it *and* `nioflow-core`, same version). See [WebFlux](webflux.md).

| Method | Effect |
|---|---|
| `handleMono(name, call[, budget \| retry])` | Stage whose work is a `Mono` (parks a worker on it); the budget goes on the Mono (it **cancels** the call) |
| `handleMonoAsync(name, call[, budget])` | The same stage **holding no thread**: `mono.toFuture()` into an async stage. A run of them fuses (RFC 0013), so it costs no throughput; the budget cancels the call |
| `adaptMono(call[, budget])` / `adaptMonoAsync(call[, budget])` | Re-type through a `Mono`: blocking, and the non-parking async form |
| `adaptFlux(call)` | **Deprecated** (`forRemoval = false`): collects a `Flux` with **no cap** — an OOM waiting to happen. Prefer the bounded overload, or `executeFlux` |
| `adaptFlux(call, maxItems)` | The same collect, bounded: over the cap it fails with `FlowOverflowException` and cancels the source |
| `fanOutMono(name, branches, join)` | Split-join over reactive branches — decorates `fanOutAsync`, so N remote calls hold N futures, **not N parked workers** (RFC 0016) |
| `executeMono()` | Terminal. Lazy: one execution per subscription; a filter cut is an empty `Mono`; a dispose cancels the execution |
| `executeFlux(tail)` | Streaming terminal: the engine's one value, then the tail's `Flux` — nothing is buffered |
| `pipe(concurrency, …)` / `pipeOrdered(…)` / `pipeResilient(…, onError)` | A `Flux` through the flow; concurrency **is** the backpressure. Routes `handleMono` async by default (RFC 0015) |
| `pipe(concurrency, Pipeline)` | The same, over a **prebuilt** pipeline: assembled once, dispatched off the plan per element (RFC 0014) |
| `preferAsync()` | Config, like `defaultBudget`/`propagate`: route this flow's `handleMono`/`adaptMono` to the async, future-holding path |
| `Reactive.flow(nioFlow)` / `Reactive.lane(lane)` | Entry point, and the one unwrap needed inside a branch lane |

## `NioEngine`

| Method | Notes |
|---|---|
| `call(input, context)` / `call(input, ctx, chain)` / `call(input, ctx, chain, key)` | Raw request/response — flows call these for you |
| `inject(input)` / `await()` / `await(timeout)` | Fire-and-forget pair |
| `append(link)` | Add to the chain (blocked while sealed) |
| `splice(anchor, BEFORE\|AFTER\|REPLACE, links)` | Single-link runtime edit |
| `rememberRegion(name, first, last)` / `spliceRegion(name, links)` | Atomic multi-link edits |
| `seal()` / `release()` | Validate + compile / re-open appends |
| `metrics(NioFlowMetrics)` | Install the metrics SPI |
| `addCompleteHandler(cb)` / `addErrorHandler(cb)` | Engine-wide taps (what `onComplete`/`onError` use) |
| `shutdown(grace)` | Graceful drain; returns pending count (0 = clean) |

## Engine constructors

| Constructor | Use |
|---|---|
| `new DefaultNioEngine()` | Shared JVM boss pool + shared virtual workers |
| `new DefaultNioEngine(capacity, OverflowPolicy)` | Bounded fire-and-forget: `BLOCK`, `DROP` or `FAIL` |
| `new DefaultNioEngine(bossExecutor, workerExecutor)` | Bring your own executors (engine-owned: `shutdown` terminates them) |
| `DefaultNioEngine.dedicated(bossCount)` | Private event loop for latency-critical flows |
| `DefaultNioEngine.dedicated(bossCount, capacity, policy)` | Dedicated + bounded |

JVM flag: `-Dnioflow.bosses=N` sizes the shared boss pool (default: cores, floor 2).

## Model types

| Type | Shape |
|---|---|
| `Retry` | `Retry.of(attempts, backoff)` · `Retry.exponential(attempts, initial)` — multiplier ≥ 1 |
| `RateLimit` | `RateLimit.of(permits, per)` · `RateLimit.perSecond(permits)` — one instance = one bucket |
| `Context` / `Context.Key<V>` | `Key.of("name")`, `ctx.get(key)`, `ctx.put(key, value)`, `ctx.getOrDefault(key, fb)` |
| `FlowResult<T>` | Sealed: `Completed(T value)` \| `Filtered` \| `Cancelled` — pattern-matchable |
| `Splice` | `BEFORE` · `AFTER` · `REPLACE` |
| `Segment<T, R>` | `Lane<R> define(Lane<T> lane)` — reusable, composable, independently testable |
| `Pipeline<I, R>` / `PipelineRun<R>` | A prebuilt per-request pipeline: recorded, validated and compiled once by `flow.pipeline(segment)` — see below |
| `FlowSignal` | `FILTERED` · `CANCELLED` — sentinels carried by raw engine futures |
| `Link` (sealed) | `Stage`, `AsyncStage`, `Decision`, `Filter`, `Background`, `Recovery`, `FanOut`, `Batch`, `Fork` |

> Adding link types is source-breaking for exhaustive `switch`es over `Link` — pin your minor versions if you pattern-match the chain.

## Threading contracts

| Runs on | What |
|---|---|
| Virtual worker | `handle` functions, `recover`, `fanOut` branches and join, `batch` bulk, `background`; and the **invocation** of `handleAsync`/`handleMono` stages and `fanOutAsync` branches (what waits after is a future, not the worker) |
| Boss (keep it cheap, never block) | `when`/`match`/`filter` predicates, `handleSync` stages |
| Engine threads (keep them fast, never throw) | `onComplete`/`onError`, metrics callbacks |
