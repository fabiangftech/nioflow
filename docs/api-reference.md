# API reference

Compact reference for the public surface. Types live in `dev.nioflow.core.facade` and `dev.nioflow.core.model`; implementations in `dev.nioflow.application.facade`.

## Entry points

| Method | Returns | Notes |
|---|---|---|
| `DefaultNioFlow.from(Class<I>)` | `DefaultNioFlow<I, O>` | Own engine on the shared JVM pools; the Class token lets `just()` reject a wrong input |
| `DefaultNioFlow.from(Class<I>, NioEngine)` | `DefaultNioFlow<I, O>` | Bring your own engine |
| `DefaultNioFlow.create()` | `DefaultNioFlow<I, O>` | No Class token — for generic beans (`NioFlow<?, ?>`); no input check |
| `flow.just(input)` | `NioStep<I, O>` | Opens an isolated per-request pipeline, **starting at the input type** |
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
| `handleSync(fn)` / `handleSync(name, fn)` | Boss-inlined stage — pure CPU, sub-µs, never blocking |
| `handleContextual((v, ctx) -> …)` | Stage with the typed per-execution `Context` |
| `filter(predicate)` | Deliberate cut; `execute()` maps it to `null` |
| `background(name, effect)` | Fire-and-forget side effect |
| `fanOut(name, branches, join)` | Parallel split-join, declaration-order combine |
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
| `fanOut(...)` / `batch(...)` / `use(segment)` | Also re-type (to `C` / `R` / `R`) |
| `key(k)` | Per-key FIFO ordering |
| `execute()` | Blocks; returns the value's **current** type; `null` on a filter cut |
| `executeAsync()` | `CompletableFuture<T>` — non-blocking; fails exceptionally when unrecovered |
| `executeResult()` | `FlowResult<T>` — `Completed(value)` vs `Filtered`, pattern-matchable |

`Lane<T>` (inside fork branches and segments) exposes the same building methods, minus `just`/`execute`/lifecycle.

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
| `FlowResult<T>` | Sealed: `Completed(T value)` \| `Filtered` |
| `Splice` | `BEFORE` · `AFTER` · `REPLACE` |
| `Segment<T, R>` | `Lane<R> define(Lane<T> lane)` — reusable, composable, independently testable |
| `FlowSignal.FILTERED` | Sentinel carried by raw engine futures on a filter cut |
| `Link` (sealed) | `Stage`, `Decision`, `Filter`, `Background`, `Recovery`, `FanOut`, `Batch` |

> Adding link types is source-breaking for exhaustive `switch`es over `Link` — pin your minor versions if you pattern-match the chain.

## Threading contracts

| Runs on | What |
|---|---|
| Virtual worker | `handle` functions, `recover`, `fanOut` branches and join, `batch` bulk, `background` |
| Boss (keep it cheap, never block) | `when`/`match`/`filter` predicates, `handleSync` stages |
| Engine threads (keep them fast, never throw) | `onComplete`/`onError`, metrics callbacks |
