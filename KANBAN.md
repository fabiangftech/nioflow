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
- [x] **Boss safety invariants** — iterative `advance` (no stack overflow on deep chains), throwing predicates fail the value never the boss task `[scale]`
- [x] **Quality harness** — JMH benchmarks (`tests/`), bug-hunting stress tests, Spring Boot showcase example `[maint]`

## 🚀 Ready (next up, in priority order)

- [ ] **Expose recovery in the fluent API** — `recover(Function<Throwable, T>)` / `recover(name, fn)` on `NioFlow` and `Lane`; the engine already supports `Recovery` links, the builder does not `[maint]`
- [ ] **`executeAsync()` returning `CompletableFuture<T>`** — the engine's `call()` already returns one; exposing it enables non-blocking Spring MVC/WebFlux controllers without touching the engine `[scale]`
- [ ] **Stage timeout in the fluent API** — `handle(name, fn, Duration)`; `Stage.timeout` exists and works, only the builder lacks it `[maint]`
- [ ] **Backpressure for `inject`/`justAll`** — bounded in-flight queue with overflow policies (BLOCK / DROP / FAIL); today `inFlight` grows unbounded `[scale]`
- [ ] **Metrics SPI + OpenTelemetry adapter** — per-stage latency, queue depth, executions in flight; `infrastructure/` package and the `compileOnly` otel dependency are already reserved for this `[maint] [scale]`

## 📋 Backlog

### Performance `[perf]`

- [ ] **Chain compilation at `seal()`** — precompute fusion runs, guard tables and dispatch plan once instead of scanning per execution; biggest remaining hot-path win
- [ ] **Decisions as bitset** — decision ids are dense ints; replace the per-execution `HashMap<Integer, Boolean>` with a long[] bitset (zero allocation, O(1) guards)
- [ ] **Inline cheap stages on the boss (opt-in)** — `sync` marker for stages that are pure CPU and sub-microsecond; skips both thread hops (measured: 2 hops ≈ 10-18µs)
- [ ] **Fusion across recorded decisions** — a Decision whose guards already failed cannot change routing; extend runs through it
- [ ] **Allocation audit per call** — Execution, snapshot, context map; target near-zero garbage for the common path
- [ ] **JMH regression gate in CI** — fail the build if a benchmark drops beyond a threshold vs the recorded baseline

### Scalability `[scale]`

- [ ] **`fanOut`/`fanIn`** — split one value into N parallel lane executions and join results (the missing sibling of when/match)
- [ ] **Batching** — `batch(size, window)` link: accumulate values and process them as one unit (bulk inserts, bulk API calls)
- [ ] **Async stages** — give `Stage.async` (already in the record, unused) semantics: launch without awaiting, join later in the chain
- [ ] **Rate limiting per stage** — token bucket on named stages; protects downstream dependencies
- [ ] **Boss pool tuning** — configurable pool size, dedicated (non-shared) event loop option per engine for latency-critical flows
- [ ] **Graceful drain on shutdown** — stop accepting, finish in-flight executions within the grace period, report the rest
- [ ] **Sharded/keyed execution** — pin executions with the same business key to the same boss for ordered processing per key (Kafka-partition style)

### Maintainability / DX `[maint]`

- [ ] **Reusable sub-flows** — `use(subFlow)` / named segments: compose large pipelines from smaller tested pieces; the flow calls external methods, segments group them
- [ ] **Splice regions** — REPLACE remembers named segments so a whole region can be swapped atomically (today splice targets single links)
- [ ] **Chain diagnostics** — human-readable chain dump (names, guards, fusion runs), DOT/Mermaid export for architecture docs
- [ ] **Validation at `seal()`** — detect dangling guards, unreachable lanes, duplicate stage names, recovery with nothing upstream
- [ ] **Distinguish filtered from null results** — `Optional<T> executeOptional()` or a result object; today a cut flow and a null-producing stage both return null
- [ ] **Flow-level `onComplete`/`onError` in the fluent API** — engine handlers exist; expose them per flow and per execution
- [ ] **Context API** — typed accessors for the per-execution context map (`ctx.get(Key<T>)`), available to stages that opt in
- [ ] **Kotlin DSL** — indentation-native branches for Kotlin consumers

### Resilience (cross-cutting)

- [ ] **Retry policy per stage** — attempts + backoff on named stages, composing with timeout and recovery `[scale]`
- [ ] **Resilience4j adapter** — circuit breaker / bulkhead as stage decorators; the `compileOnly` dependency is already declared `[scale]`
- [ ] **Dead-letter handler** — terminal failures routed to a configurable sink with the original input and failing stage name `[maint]`

## 🧊 Icebox (revisit later)

- [ ] Persistent/resumable flows (checkpoint the value + cursor, resume after restart)
- [ ] Distributed execution across nodes
- [ ] Visual flow designer / live chain editor UI on top of splice + diagnostics
- [ ] Reactive Streams (`Flow.Publisher`) bridge
