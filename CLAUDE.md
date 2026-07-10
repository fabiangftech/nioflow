# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Working language

Think in English and implement in English (code, identifiers, Javadoc, commit messages), but respond to the user in Spanish.

## Project

nio-flow is a Java concurrency library: a fluent, chain-based pipeline API where values are injected into a declared chain of stages and flow through it asynchronously, io_uring style. Zero required runtime dependencies (Resilience4j and OpenTelemetry are `compileOnly` integrations).

## Build and test

The Gradle project lives in `core/` — run all Gradle commands from there, not the repo root:

```bash
cd core
./gradlew build                                          # compile + test
./gradlew test                                           # all tests
./gradlew test --tests 'dev.nioflow.unit.NioFlowBatchTest'                 # one class
./gradlew test --tests 'dev.nioflow.unit.NioFlowBatchTest.someTestMethod'  # one method
```

Requires a modern JDK (uses virtual threads; developed on JDK 25). Tests are JUnit 6 (Jupiter).

## Architecture

Three layers under `core/src/main/java/dev/nioflow/`, dependency direction strictly inward (`application` and `infrastructure` depend on `core`, never the reverse):

- **`core/facade`** — the public API contracts. `NioFlow<T>` is the user-facing fluent interface (`just`, `handle`, `submit`, `batch`, `fanOut`, `when`/`match` forks, `filter`, `onErrorResume`, `seal`, `join`, ...). `NioEngine` is the engine contract behind it — untyped on purpose so `adapt` can hand out a differently-typed `NioFlow` view over the same running engine. `Resilience`, `NioFlowMetrics`, `NioFlowTracer` are pluggable policy/observability contracts.
- **`core/model`** — the chain and value model shared by API and engine: `Link`/`Stage` (chain elements), `FlowValue` (an in-flight value with its own cursor into the chain), `FlowContext` (per-value metadata that travels across threads), `Guard`/`Decision` (lane routing for `when`/`match` forks), `Batch`, `FanOut`, `Filter`, `Recovery`, `Backpressure`/`OverflowPolicy`, `Diagnostics`, `StageException`.
- **`application/facade`** — the implementations. `NioEngine` (the ~750-line heart): a boss thread drains a submission queue and hands each value to a handle worker (virtual thread per dispatch by default, optional fixed pool); async `submit` stages launch on the executor without waiting; a completer thread reaps results from a completion queue and re-enqueues the value for its next stage. `NioFlow` (impl) wraps the engine; `ForkBranch`, `MatchCases`, `BatchBuffer` support forks and batching.
- **`infrastructure`** — optional adapters: `OpenTelemetryMetrics`, `LoggingTracer` (trace), `Resilience4j` (resilience policies).

Naming gotcha: the interface and implementation share simple names (`dev.nioflow.core.facade.NioFlow` vs `dev.nioflow.application.facade.NioFlow`, same for `NioEngine`) — implementations reference the interfaces fully qualified.

### Core semantics (invariants the tests enforce)

- The chain is declared once; each `just`/`justAll` injects a value that walks the chain in order. Order is preserved per value, never across values — a value blocked on slow IO must not delay values behind it.
- Errors short-circuit only the failing value, delivered to `onError` handlers non-blockingly; `onErrorResume` is a positional recovery link (only catches failures declared upstream of it). `join()` rethrows a failure once, then clears it, so the flow stays usable.
- Values reaching the end of the chain park; appending a new link resumes parked values. `seal()` freezes the chain (further links throw) and releases finished values instead of parking them, keeping long-running flows flat in memory.
- Filtered-out values leave deliberately: no `onComplete`/`onError`, don't count toward `join()`, free their backpressure slot.
- Fork lanes (`when`/`then`/`otherwise`, `match`/`is`) record decisions on the value; guards make it skip links of other lanes. Stages after the fork are the main line and run for every value.

## Tests

All tests live in `core/src/test/java/dev/nioflow/unit/`, roughly one class per feature (`NioFlowBatchTest`, `NioFlowSealTest`, ...). `unit/utils/` has recording test doubles (`CapturingLogger`, `RecordingMetrics`, `RecordingTracer`) — reuse them for observability assertions.
