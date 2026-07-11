# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Working language

Always think in English and implement in English (code, identifiers, comments, commit messages), but talk to the user in Spanish.

## Feature workflow

Every new feature must ship with unit tests (in `core/`) AND JMH benchmarks (in `tests/`) showing good results: run the relevant benchmarks before and after, compare, and report the numbers. A feature that regresses the hot path is not done — either fix it or surface the trade-off explicitly. Bug-hunting stress tests in `tests/` complement, not replace, functional coverage in core.

## Project

nio-flow is a Java concurrency library: a fluent, typed pipeline API (`NioFlow<I, T>`) over an event-loop engine (a pool of boss threads orchestrates — each execution pinned to one boss — while virtual-thread workers run user code). Zero required runtime dependencies (Resilience4j and OpenTelemetry are `compileOnly` integrations). Requires a modern JDK — uses virtual threads and pattern matching over sealed types; developed on JDK 25.

## Build and test

The library's Gradle project lives in `core/` — run Gradle from there, not the repo root:

```bash
cd core
./gradlew build                                                                # compile + test
./gradlew test                                                                 # all tests
./gradlew test --tests 'dev.nioflow.application.facade.DefaultNioEngineTest'   # one class
./gradlew test --tests '*.DefaultNioEngineTest.filterCutsTheFlow'              # one method
```

The Spring Boot example is a separate Gradle build that consumes core via `includeBuild('../../core')` (composite build — no publishing needed):

```bash
cd examples/springboot-with-nioflow
./gradlew compileJava    # quickest check that core API changes don't break consumers
./gradlew bootRun        # serves GET /greeting on :8080
```

Tests are JUnit 6 (Jupiter), under `core/src/test/java/dev/nioflow/application/facade/`, split **one class per feature** (`DefaultNioFlowForkTest`, `DefaultNioEngineFusionTest`, ...) — add new tests to the matching feature class or create a new one, never grow a catch-all class. Engine test classes extend `EngineTestSupport` (fresh engine per test + `stage()` helper).

`tests/` is a third Gradle build (also composite against core) for bug-hunting stress tests and JMH benchmarks — functional coverage stays in core:

```bash
cd tests
./gradlew test                                      # stress tests (deep chains, hot splice, fork routing races)
./gradlew jmh                                       # full benchmark run
./gradlew jmh -PjmhArgs='-f 0 -wi 1 -i 1 NioFlowBenchmark'   # quick smoke of one benchmark
```

Benchmarks live in `tests/src/main/java` (JMH annotation processing), stress tests in `tests/src/test/java`. Stress tests use `orTimeout` on the futures they join — an engine bug that kills the boss task leaves futures hanging forever, and the timeout converts that hang into a visible failure.

## Architecture

Three packages under `core/src/main/java/dev/nioflow/`, dependency direction strictly inward (`application` and `infrastructure` depend on `core`, never the reverse). Interfaces live in `core.facade`; implementations in `application.facade` with a `Default` prefix.

- **`core/facade`** — public contracts. `NioFlow<I, T>` is the fluent typed API: `I` is the input type (`just` is compile-checked against it), `T` is the value's type at the current point of the chain; `adapt(Function<T, R>)` is the only step that changes `T` (returning `NioFlow<I, R>`), everything else preserves it. `NioFlow` is deliberately NOT `AutoCloseable` — lifecycle lives only on `DefaultNioFlow.close()` (the engine owner); branches, lanes and executions never expose it. `NioEngine` is the engine contract behind it — untyped (`Object`) on purpose; all unchecked casts are encapsulated in the flow implementations. `Condition`/`Branch`/`Cases` are the fork contracts behind `when()`/`match()`; `Lane`/`LaneCondition`/`LaneBranch`/`LaneCases` are their restricted lane-side counterparts.
- **`core/model`** — the chain model: `Link` is a sealed interface permitting `Stage` (transform, optional timeout), `Decision` (records a boolean per value), `Filter` (short-circuits the flow), `Background` (fire-and-forget side effect), `Recovery` (positional error handler), `FanOut` (parallel split-join: branches run concurrently on workers, join combines in declaration order; a branch failure fails the fan-out, recoverable downstream). Every link carries `Guard`s (`decision id` + `expected`) for lane routing. `Splice` (BEFORE/AFTER/REPLACE) names the runtime-edit positions. FanOut breaks fusion runs (it is a dispatch boundary) and, due to Java inference limits, callers should type the branches list explicitly (`List<Function<T, R>> branches = List.of(...)`) so the join lambda infers `List<R>`.
- **`application/facade`** — `DefaultNioEngine`; `AbstractNioFlow` (shared builder + fork logic) extended by `DefaultNioFlow` (shared definition) and `ExecutionNioFlow` (per-request execution); `DefaultCondition`/`DefaultBranch`/`DefaultCases` (forks).
- **`infrastructure`** — optional adapters over `compileOnly` dependencies: `OpenTelemetryMetrics` (metrics SPI → otel histograms/counters/gauge; only loads if the consumer brings `opentelemetry-api`).

### DefaultNioEngine: the event loop

Two rules define it:

1. **Each execution is pinned to one boss, and only that boss touches its orchestration state.** `Execution.advance`/`recover` always run on the execution's boss; workers hand results back via `whenCompleteAsync(..., boss)`. This is the serialization mechanism — no locks in the hot path. Concurrent executions spread across the boss pool (EventLoopGroup-style affinity), so one boss is never a JVM-wide ceiling.
2. **The boss never runs user code** — `Stage`/`Background`/`Recovery` functions go to the virtual-thread workers. Exception by design: `Decision` predicates (and `Filter` predicates not absorbed into a fused run) run on the boss, so they must stay cheap and non-blocking (same rule as Netty handlers). A predicate that throws on the boss fails the value through the recovery path — never the boss task (a dead boss task means a forever-hanging request future).

Hot-path rules the benchmarks enforce (see `tests/`):

- `advance` must stay **iterative**, never recursive: cheap links (Decision/Filter/Background/guard-skips) are walked in a loop on the boss, and a recursive version overflows the boss stack on deep chains, killing the task and leaving the request future hanging forever (regression: `DeepChainStressTest`).
- **Stage fusion**: a run of consecutive no-timeout `Stage`s, `Filter`s and `Recovery`s travels boss→worker→boss as ONE composed function (2 thread hops per run, not per link — measured ~5x on an 8-stage chain, ~1.5x on stage-filter-stage). Fused `Filter` predicates evaluate on the worker; a rejection returns the internal `FILTERED` sentinel and completes the flow with `null`. Fused `Recovery`s keep positional semantics inside the run: a failure scans forward for the next in-run `Recovery` and continues from it; with none left it escapes the run and the boss scans the rest of the chain from the run's end — equivalent, because the segment in between was already searched. Guard-skipped links inside the run are stepped over (decisions can't change until the next passing `Decision`, which ends the run). Stages with a timeout dispatch alone.
- No streams or allocations on the per-link path (`passesGuards` is a plain loop — the stream version cost ~20% on fork routing).

Executors are JVM-wide singletons (`SharedExecutors` lazy holder, `commonPool()` style): a pool of daemon bosses (≥2, sized by available processors) + one virtual-thread worker pool shared by every engine created with the default constructor, no matter how many `DefaultNioFlow`s exist. `shutdown(grace)` is a **graceful drain**: it rejects new `call`/`inject` immediately, waits up to the grace for in-flight executions, and returns how many were still running (0 = clean). It only terminates executors that were explicitly passed in (`ownsExecutors`); shared ones survive — closing one flow never starves the others, and stragglers finish on their own threads.

### Concurrency and runtime-editing invariants (the tests enforce these)

- The chain is an **immutable list swapped atomically** (`volatile`); `append`/`splice` build a new list under `synchronized`. Every `call()` snapshots the chain at submission, so a runtime `splice` never affects in-flight executions — the next call sees the new chain.
- `seal()` **validates and compiles** the chain. Validation (`ChainValidator` → `ChainValidationException` with the full problem list) rejects dangling guards, contradictory guards, duplicate anchor names and dead recoveries — a broken definition stops the deploy; a splice over a sealed chain validates too and, when rejected, leaves the previous chain and plan untouched. Compilation produces the dispatch plan (`CompiledChain`): static fusion windows plus precollected unguarded runs, so those dispatches do zero scanning/allocation (~13% less garbage per request; throughput parity — hops dominate). `splice` recompiles once per edit; `append` invalidates; executions match the plan by chain identity, so per-request local chains fall back to interpreting. The plan is an optimization, never a semantic: compiled and interpreted must produce identical results (`DefaultNioEngineCompiledChainTest`).
- Each `call()` gets its own `Execution` (chain snapshot, decisions map, result future): concurrent requests share nothing. The decisions map is only ever touched on the boss.
- `seal()` blocks `append` (frozen definition) but **not** `splice` — splice *is* the runtime-edit operation, anchored on `Stage`/`Background` names. `release()` re-opens appending.
- `Recovery` is positional: it catches failures (including `Stage` timeouts via `orTimeout`) from links upstream of it; execution continues after it with the recovered value. With no matching recovery, the failure reaches `errorHandlers` and the call's future. Declared fluently via `recover(fn)` / `recover(name, fn)` on `NioFlow` and `Lane` — a lane-scoped recover inherits the lane's guards and only catches failures of values routed through that branch. Named recoveries are splice anchors like stages.
- Resilience composes in layers: **timeout per attempt → `Retry` over attempts → `recover()` as the final net**. `Retry` is native (attempts + backoff + multiplier; Resilience4j remains an optional adapter concern): no-timeout retries loop inline on the virtual worker (`LockSupport.parkNanos` backoff) and never break fusion; timeout+retry chains per-attempt `orTimeout` futures with non-blocking backoff scheduling. Retries are observable via `NioFlowMetrics.stageRetried`.
- `Filter` short-circuits by completing the raw engine future with the public `FlowSignal.FILTERED` sentinel; engine exits (`await`, complete handlers) and flow-level `execute()`/`executeAsync()` map it to `null`, while `executeResult()` returns a sealed `FlowResult<T>` (`Completed(value)` — even genuinely null — vs `Filtered`) for callers that need the distinction. `Background` never waits and never fails the flow; a throwing effect reports to `errorHandlers` only.
- `inject`/`await` are the fire-and-forget pair (results queue up in `inFlight`); `call` is the request/response form returning a `CompletableFuture`. At the flow level, `execute()` blocks and `executeAsync()` returns the future (`execute()` IS `executeAsync().join()`) — return the future from a controller for a non-blocking endpoint.
- `inFlight` is unbounded by default; `new DefaultNioEngine(capacity, OverflowPolicy)` bounds it with BLOCK (park the producer), DROP (discard, reported to error handlers) or FAIL (throw `RejectedExecutionException`). Admission happens BEFORE the execution starts — rejecting an already-run value is not backpressure — and the slot frees when `await()` collects the result.
- Observability: `engine.metrics(NioFlowMetrics)` installs the metrics SPI (no-op defaults, null = zero instrumentation). The engine pushes execution latency classified as completed/failed/filtered, per-stage latency (timed on the worker inside fused runs), applied recoveries, dropped values and queue depth. Callbacks run on engine threads: keep them fast and never throw.

### DefaultNioFlow: shared definition vs per-request execution

The root `DefaultNioFlow` is the **shared definition** (a Spring singleton bean): `handle`/`background`/`adapt`/`filter` on it append to the engine's shared chain. `just(input)` opens an **independent ephemeral execution** (`ExecutionNioFlow`): it lazily copies the shared chain only if local links are added, and `execute()` runs it via `engine.call(input, context, chain)` — never sealing or mutating anything shared. That is why N concurrent requests can each do `just(...)...execute()` on the same bean.

`just()` returns `NioFlow<I, T>` typed by the *pipeline's current type* (not the input's), so an `adapt` in the shared definition correctly types the steps chained after `just()`. The typed entry point is the factory:

```java
@Bean(destroyMethod = "close")
public NioFlow<String, Integer> flow() {
    return DefaultNioFlow.from(String.class)   // anchors I = T = String
            .handle(String::trim)
            .adapt(String::length);            // bean type declares input and output
}
// per request: flow.just("  hi  ").handle(n -> n * 2).execute()  → Integer
```

`execute()` without `just()` throws; `justAll` only exists on the root (injects through the shared chain, collect with `engine.await()`).

### Forks: when() and match()

Both are sugar over `Decision`/`Guard` — the engine knows nothing about forks. A lane is a *guarded view* of the same flow (`AbstractNioFlow.withGuards`) wrapped in the restricted `Lane<T>` interface: every link declared inside the lane's `UnaryOperator<Lane<T>>` carries `Guard(decisionId, expected)`, so nested forks compose guards automatically and forks work identically on the shared definition and inside a `just()` execution.

- `when(pred).then(lane).otherwise(lane)`: appends one `Decision`; the lanes require `true`/`false`. Chaining after the fork (on `Branch` or after `otherwise`) is the main line — unguarded, runs for every value.
- `match().is(pred, lane)...otherwise(lane)` is **first-match-wins**: case k's `Decision` carries guards requiring all previous cases `false` (so it isn't even evaluated after a match), its lane additionally requires its own decision `true`, and `otherwise` requires all `false`. A skipped `Decision` records nothing, and an absent decision fails any guard on it — that's what makes the semantics hold.
- `Lane<T>` exposes only step-building operations (`handle`/`background`/`adapt`/`filter`/`recover`/`fanOut`/`use` and nested `when`/`match` via `LaneCondition`/`LaneBranch`/`LaneCases`) — no `just`, no `execute`, no `close`. The type going into and out of a lane is `T` (an `adapt` inside a lane must return to `T`, enforced by the `UnaryOperator` signature).
- **Sub-flows**: `Segment<T, R>` defines a reusable chain piece over `Lane<T>` ending at `Lane<R>`; `use(segment)` embeds it inline with the caller's guards (lane-scoped inside forks), segments compose via `lane.use(...)`, and they are build-time only — zero runtime footprint (the embedded links compile/fuse like inline ones). Prefer segments to keep large shared definitions readable (see the example's `NioFlowConfig`: platform / fraudGate / shippingTier / notifications).
