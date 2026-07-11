# nio-flow — Kanban

Feature board for nio-flow: a library for complex business logic that stays easy to extend.
Every item is tagged by focus: `[perf]` performance, `[scale]` scalability, `[maint]` maintainability/DX.

Workflow rule (from CLAUDE.md): a feature is checked off only with unit tests in `core/` AND JMH
benchmarks in `tests/` showing good results — no hot-path regressions.

---

## ✅ Done

- [x] **Typed pipeline `NioFlow<I, T>`** — input compile-checked by `I`, `adapt` is the only re-typing step `[maint]`
- [x] **Event loop engine** — boss pool with per-execution affinity + virtual-thread workers; boss never runs user code `[perf] [scale]`
- [x] **JVM-shared executors** — commonPool-style daemon bosses/workers; ownership-aware `shutdown()` `[scale]`
- [x] **Stage + filter fusion** — one boss→worker→boss round-trip per run of cheap links (~5.7x on 8-stage chains) `[perf]`
- [x] **Runtime chain editing** — `splice` BEFORE/AFTER/REPLACE on named stages; immutable chain + per-call snapshot: in-flight requests never affected `[scale] [maint]`
- [x] **Per-request executions** — `just()...execute()` on a shared singleton bean, fully isolated between concurrent requests `[scale]`
- [x] **Forks `when`/`match`** — first-match-wins cases, nested forks, guards composed automatically; restricted `Lane<T>` (no execute/just/close inside a branch) `[maint]`
- [x] **`filter`** — short-circuit on shared or per-request chains, fused into worker runs `[perf]`
- [x] **`background`** — fire-and-forget side effects; errors reported to handlers, never fail the flow `[scale]`
- [x] **Recovery links (engine)** — positional error handling, catches stage failures and timeouts `[maint]`
- [x] **`recover` in the fluent API** — `recover(fn)` / `recover(name, fn)` on `NioFlow` and `Lane` (lane-scoped via guards); recoveries fuse into worker runs: happy path at parity with plain stages, error path 2x faster `[maint] [perf]`
- [x] **`executeAsync()` returning `CompletableFuture<T>`** — non-blocking endpoints by returning the future from the controller; `execute()` is now `executeAsync().join()`; single-call parity, 2.6x on 16 pipelined executions `[scale]`
- [x] **Stage timeout in the fluent API** — `handle(name, fn, Duration)` on `NioFlow` and `Lane`; timeout failures are recoverable downstream; armed budget costs ~40% on that stage only (fusion break + orTimeout timer, by design) `[maint]`
- [x] **Backpressure for `inject`/`justAll`** — `DefaultNioEngine(capacity, OverflowPolicy)`: BLOCK parks the producer, DROP discards (reported to error handlers), FAIL throws; admission happens BEFORE the execution runs, slots free on `await()`; uncontended cost at parity with unbounded `[scale]`
- [x] **Metrics SPI + OpenTelemetry adapter** — `NioFlowMetrics` (no-op defaults) installed via `engine.metrics()`: execution/stage latency, failed/filtered classification, recoveries, drops, queue depth; `infrastructure/OpenTelemetryMetrics` exports histograms/counters/gauge with cached attributes; instrumentation cost at parity with metrics off `[maint] [scale]`
- [x] **Chain compilation at `seal()`** — precomputed fusion windows and unguarded runs per chain version; splice recompiles once per edit; execution-local chains fall back to interpreting. Measured honestly: throughput at parity (thread hops dominate — the "biggest win" framing was wrong), but **~13% less garbage per request** (1170 → 1017 B/op) and the compiler pass is the foundation for seal-time validation and the decisions bitset `[perf]`
- [x] **`fanOut`/`fanIn`** — `fanOut(name, branches, join)` on `NioFlow` and `Lane`: branches run concurrently on workers (one each), join combines in declaration order on a worker, failures recoverable downstream; 2.2x on 3×50µs branches, ~28% overhead on trivial branches (use it for real work); works in lanes and on compiled chains `[scale]`
- [x] **Reusable sub-flows (`Segment<T, R>` + `use`)** — a segment defines a chain piece over `Lane<T>` ending at `Lane<R>`; embedded inline with the caller's guards (lane-scoped when used in forks), segments compose and are independently testable; build-time only, runtime at parity with inline definitions `[maint]`
- [x] **Graceful drain on shutdown** — `shutdown(grace)` now returns the pending count: rejects new call/inject immediately, waits for in-flight executions up to the grace (works for JVM-shared executors too — the old code was a no-op there), then terminates engine-owned executors; stragglers on shared executors still finish on their own; hot-path counter at parity `[scale]`
- [x] **Distinguish filtered from null results** — public `FlowSignal.FILTERED` carried by raw engine futures (engine exits map it to null); `executeResult()` returns sealed `FlowResult<T>` (`Completed(value)` — even genuinely null — vs `Filtered`), pattern-matchable; `execute`/`executeAsync` keep the null mapping for compatibility; filter paths at parity `[maint]`
- [x] **Retry policy per stage** — native `Retry` (attempts + backoff + multiplier, zero external deps: Resilience4j stays an optional adapter for circuit breaker/bulkhead); `handle(name, fn, Retry)` / `handle(name, fn, Duration, Retry)` on `NioFlow` and `Lane`; no-timeout retries loop inline on the (virtual) worker and never break fusion, timeout+retry applies the budget per attempt with non-blocking backoff scheduling; exhausted retries flow to recovery; observable via `stageRetried` metrics; declared-but-unused at parity, one retry costs ~15% `[scale]`
- [x] **Validation at `seal()`** — `ChainValidationException` (with the full problem list) on dangling guards, contradictory guards, duplicate anchor names and dead recoveries; runs at seal (broken definitions stop the deploy) and on every splice over a sealed chain (a rejected runtime edit leaves the previous chain and plan untouched); build-time only, runtime at parity `[maint]`
- [x] **Decisions as bitset** — per-execution decisions in a `long[]` bitset (2 bits per id: recorded + value) instead of `HashMap<Integer, Boolean>`: O(1) records/guards with zero per-decision allocation, and an unrecorded id still fails any guard (what first-match-wins relies on). Sized by the chain's highest Decision id (precomputed at `seal()`, scanned once per call on interpreted chains); ids past 511 — per-request forks on a long-lived engine keep growing the engine-wide counter — fall back to a lazy overflow map with identical semantics. Throughput at parity; ~14% less garbage per forked request (1192 → 1025 B/op on match routing, 1312 → 1144 B/op on forked compiled chains) and decision-free executions drop the eagerly-allocated HashMap (~48 B/op) `[perf]`
- [x] **Inline cheap stages on the boss (opt-in)** — `handleSync(name, fn)` on `NioFlow` and `Lane` runs a pure-CPU, sub-microsecond stage inline on the boss, skipping both thread hops (same contract as Decision predicates: never block; a throw fails the value through recovery, never the boss task). 2.2-2.4x on trivial chains (1 stage: 56 → 126 ops/ms; 4 stages: 57 → 135 ops/ms) with ~59% less garbage (1001 → 416 B/op); a sync stage after a worker stage still fuses into that run. No timeout/retry variants by design — hand-built sync+timeout/retry stages are rejected at seal(). Worker-stage paths at parity (the extra branch measured nothing) `[perf]`
- [x] **Fusion across recorded decisions** — compiled fusion windows now extend through *guarded* non-fusable links (a `match()` case's Decision the execution's recorded decisions already ruled out, skipped lanes): the per-execution scan steps over guard-failed links, a *passing* Decision still ends the run, and an unguarded one stays a hard boundary — converging with what the interpreted scan already did. A case-1 route over `match(3) + 2 tail stages` (sealed) drops from 2 dispatches to 1: 35.0 → 54.6 ops/ms (+56%), 1612 → 1152 B/op (−29%), reaching parity with the already-fused otherwise route; plain chains and already-fused routes at parity `[perf]`
- [x] **Boss safety invariants** — iterative `advance` (no stack overflow on deep chains), throwing predicates fail the value never the boss task `[scale]`
- [x] **Quality harness** — JMH benchmarks (`tests/`), bug-hunting stress tests, Spring Boot showcase example `[maint]`

## 🚀 Ready (next up, in priority order)

- [ ] **Allocation audit per call** — Execution, snapshot, context map; target near-zero garbage for the common path. Baseline to beat after bitset + decision fusion: ~1000 B/op plain, ~1150 B/op forked (`-prof gc` runs recorded) `[perf]`
- [ ] **Flow-level `onComplete`/`onError` in the fluent API** — engine handlers exist; expose them per flow and per execution `[maint]`
- [ ] **Dead-letter handler** — terminal failures routed to a configurable sink with the original input and failing stage name `[maint]`

## 📋 Backlog

### Performance `[perf]`

- [ ] **JMH regression gate in CI** — fail the build if a benchmark drops beyond a threshold vs the recorded baseline (blocked: no CI pipeline exists yet)
- [ ] **Timer wheel for stage timeouts** — replace per-call `orTimeout` scheduling (measured ~40% on timeout-armed stages) with a shared hashed timer wheel

### Scalability `[scale]`

- [ ] **Batching** — `batch(size, window)` link: accumulate values and process them as one unit (bulk inserts, bulk API calls)
- [ ] **Async stages** — launch a stage without awaiting and join later in the chain (needs its own marker on `Stage`; the old unused `async` flag became the `sync` boss-inline marker)
- [ ] **Rate limiting per stage** — token bucket on named stages; protects downstream dependencies
- [ ] **Boss pool tuning** — configurable pool size, dedicated (non-shared) event loop option per engine for latency-critical flows
- [ ] **Sharded/keyed execution** — pin executions with the same business key to the same boss for ordered processing per key (Kafka-partition style)

### Maintainability / DX `[maint]`

- [ ] **Splice regions** — REPLACE remembers named segments so a whole region can be swapped atomically (today splice targets single links)
- [ ] **Context API** — typed accessors for the per-execution context map (`ctx.get(Key<T>)`), available to stages that opt in
- [ ] **Kotlin DSL** — indentation-native branches for Kotlin consumers

### Resilience (cross-cutting)

- [ ] **Resilience4j adapter** — circuit breaker / bulkhead as stage decorators; the `compileOnly` dependency is already declared `[scale]`

## 🧊 Icebox (revisit later)

- [ ] Persistent/resumable flows (checkpoint the value + cursor, resume after restart)
- [ ] Distributed execution across nodes
- [ ] Visual flow designer / live chain editor UI on top of splice + diagnostics
- [ ] Reactive Streams (`Flow.Publisher`) bridge
