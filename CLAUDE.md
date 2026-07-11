# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Working language

Think and implement in English (code, identifiers, commit messages), but respond to the user in Spanish.

## Project

nio-flow is a Java concurrency library: a fluent, typed pipeline API (`NioFlow<I, T>`) over an event-loop engine (one boss thread orchestrates, virtual-thread workers run user code). Zero required runtime dependencies (Resilience4j and OpenTelemetry are `compileOnly` integrations). Requires a modern JDK — uses virtual threads and pattern matching over sealed types; developed on JDK 25.

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

Tests are JUnit 6 (Jupiter), under `core/src/test/java/dev/nioflow/application/facade/`.

## Architecture

Three packages under `core/src/main/java/dev/nioflow/`, dependency direction strictly inward (`application` and `infrastructure` depend on `core`, never the reverse). Interfaces live in `core.facade`; implementations in `application.facade` with a `Default` prefix.

- **`core/facade`** — public contracts. `NioFlow<I, T>` is the fluent typed API: `I` is the input type (`just` is compile-checked against it), `T` is the value's type at the current point of the chain; `adapt(Function<T, R>)` is the only step that changes `T` (returning `NioFlow<I, R>`), everything else preserves it. `NioEngine` is the engine contract behind it — untyped (`Object`) on purpose; all unchecked casts are encapsulated in `DefaultNioFlow`. `Condition`/`Branch`/`Cases` are fork contracts, **not implemented yet** (`when()`/`match()` throw `UnsupportedOperationException`).
- **`core/model`** — the chain model: `Link` is a sealed interface permitting `Stage` (transform, optional timeout), `Decision` (records a boolean per value), `Filter` (short-circuits the flow), `Background` (fire-and-forget side effect), `Recovery` (positional error handler). Every link carries `Guard`s (`decision id` + `expected`) for lane routing. `Splice` (BEFORE/AFTER/REPLACE) names the runtime-edit positions.
- **`application/facade`** — `DefaultNioEngine` and `DefaultNioFlow` (plus its inner `ExecutionNioFlow`).
- **`infrastructure`** — reserved for optional adapters (currently empty).

### DefaultNioEngine: the event loop

Two rules define it:

1. **Only the boss thread touches orchestration state.** `Execution.advance`/`recover` always run on the boss; workers hand results back via `whenCompleteAsync(..., bossExecutorService)`. This is the serialization mechanism — no locks in the hot path.
2. **The boss never runs user code** — `Stage`/`Background`/`Recovery` functions go to the virtual-thread workers. Exception by design: `Decision` and `Filter` predicates run on the boss, so they must stay cheap and non-blocking (same rule as Netty handlers).

Executors are JVM-wide singletons (`SharedExecutors` lazy holder, `commonPool()` style): one daemon boss + one virtual-thread pool shared by every engine created with the default constructor, no matter how many `DefaultNioFlow`s exist. `shutdown()` only terminates executors that were explicitly passed in (`ownsExecutors`); shared ones survive so closing one flow never starves the others.

### Concurrency and runtime-editing invariants (the tests enforce these)

- The chain is an **immutable list swapped atomically** (`volatile`); `append`/`splice` build a new list under `synchronized`. Every `call()` snapshots the chain at submission, so a runtime `splice` never affects in-flight executions — the next call sees the new chain.
- Each `call()` gets its own `Execution` (chain snapshot, decisions map, result future): concurrent requests share nothing. The decisions map is only ever touched on the boss.
- `seal()` blocks `append` (frozen definition) but **not** `splice` — splice *is* the runtime-edit operation, anchored on `Stage`/`Background` names. `release()` re-opens appending.
- `Recovery` is positional: it catches failures (including `Stage` timeouts via `orTimeout`) from links upstream of it; execution continues after it with the recovered value. With no matching recovery, the failure reaches `errorHandlers` and the call's future.
- `Filter` short-circuits by completing the flow with `null`. `Background` never waits and never fails the flow; a throwing effect reports to `errorHandlers` only.
- `inject`/`await` are the fire-and-forget pair (results queue up in `inFlight`); `call` is the request/response form returning a `CompletableFuture`.

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
