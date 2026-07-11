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
./gradlew test --tests 'dev.nioflow.unit.deprecated.DefaultNioFlowBatchTest'                 # one class
./gradlew test --tests 'dev.nioflow.unit.deprecated.DefaultNioFlowBatchTest.someTestMethod'  # one method
```

Requires a modern JDK (uses virtual threads; developed on JDK 25). Tests are JUnit 6 (Jupiter).

## Architecture

Three layers under `core/src/main/java/dev/nioflow/`, dependency direction strictly inward (`application` and `infrastructure` depend on `core`, never the reverse):

- **`core/facade`** — the public API contracts. `NioFlow<T>` is the user-facing fluent interface (`just`, `call`, `handle`, `submit`, `batch`, `fanOut`, `when`/`match` forks, `filter`, `onErrorResume`, structural edits `remove`/`replace`/`insertBefore`/`insertAfter`, `scoped`, `release`, `seal`, `join`, ...). `call` is the request/response form of injection: it returns a `CompletableFuture` resolved with that value's own outcome (failed on terminal error, cancelled when the value leaves via `filter`/empty `fanOut`/DROP backpressure) — the fit for request-driven callers like web handlers sharing one long-lived flow. `NioEngine` is the engine contract behind it — untyped on purpose so `adapt` can hand out a differently-typed `NioFlow` view over the same running engine. `Resilience`, `NioFlowMetrics`, `NioFlowTracer` are pluggable policy/observability contracts.
- **`core/model`** — the chain and value model shared by API and engine: `Link`/`Stage` (chain elements), `FlowValue` (an in-flight value with its own cursor into the chain), `FlowContext` (per-value metadata that travels across threads), `Guard`/`Decision` (lane routing for `when`/`match` forks), `Batch`, `FanOut`, `Filter`, `Recovery`, `Backpressure`/`OverflowPolicy`, `Diagnostics`, `StageException`.
- **`application/facade`** — the implementations. `DefaultNioEngine` (the heart): a boss thread drains a submission queue and hands each value to a handle worker (virtual thread per dispatch by default, optional fixed pool); async `submit` stages launch on the executor without waiting; a completer thread reaps results from a completion queue and re-enqueues the value for its next stage. The chain is versioned copy-on-write: each value captures the current version at injection and finishes on it; `splice` (structural edits) starts a new version, remembers REPLACE-spliced segments as named *regions* so re-editing swaps whole segments, and releases parked values of the old version. `DefaultNioFlow` wraps the engine; `ForkBranch`, `MatchCases`, `BatchBuffer` support forks and batching; `RecordingNioEngine` collects the links a structural-edit segment declares without touching the live chain; `ScopeNioEngine` backs `scoped()` — an ephemeral caller-private chain (snapshot of the shared chain + the scope's own links) whose buffered injections run on the live engine at `join()`/`call`, waiting only for the scope's values; `AutoScopedNioFlow` (via `DefaultNioFlow.autoScoped()`) is the per-call facade where every fluent chain opens its own scope and shared-chain operations (`join`/`seal`/`release`/edits) throw on the facade.
- **`infrastructure`** — optional adapters: `OpenTelemetryMetrics`, `LoggingTracer` (trace), `Resilience4j` (resilience policies).

Naming note: interfaces live in `core.facade` (`NioFlow`, `NioEngine`), implementations in `application.facade` with a `Default` prefix (`DefaultNioFlow`, `DefaultNioEngine`).

### Core semantics (invariants the tests enforce)

- The chain is declared once; each `just`/`justAll` injects a value that walks the chain in order. Order is preserved per value, never across values — a value blocked on slow IO must not delay values behind it.
- Errors short-circuit only the failing value, delivered to `onError` handlers non-blockingly; `onErrorResume` is a positional recovery link (only catches failures declared upstream of it). `join()` rethrows a failure once, then clears it, so the flow stays usable.
- Values reaching the end of the chain park; appending a new link resumes parked values. `seal()` freezes the chain (further links and structural edits throw) and releases finished values instead of parking them, keeping long-running flows flat in memory. `release()` only does the latter: finished values are released but the chain stays appendable and editable — the mode for a shared, runtime-editable service flow.
- Structural edits (`remove`/`replace`/`insertBefore`/`insertAfter`) anchor on named stages, run under the engine lock (safe from any thread) and never disturb values in flight: each value finishes the chain version it was injected into.
- `scoped()` hands out an ephemeral caller-private chain over the shared engine: links declared on it never touch the shared chain, concurrent scopes never interfere, its `join()` waits only for its own values, and scope `onComplete`/`onError` observe only those. Global concerns (structural edits, `metrics`, `trace`, `close`) belong to the shared flow — the first three throw on a scope, `close` is a no-op.
- Filtered-out values leave deliberately: no `onComplete`/`onError`, don't count toward `join()`, free their backpressure slot.
- `background(effect)` is fire-and-forget: the consumer launches on the executor and the value moves on immediately — `join()`/`call` never wait for it; a throwing effect reports to `onError` (and metrics) but never fails the value. `submit` is for async work that produces the result and is always awaited.
- Fork lanes (`when`/`then`/`otherwise`, `match`/`is`) record decisions on the value; guards make it skip links of other lanes. Stages after the fork are the main line and run for every value.

## Tests

All tests live in `core/src/test/java/dev/nioflow/unit/`, roughly one class per feature (`NioFlowBatchTest`, `NioFlowSealTest`, ...). `unit/utils/` has recording test doubles (`CapturingLogger`, `RecordingMetrics`, `RecordingTracer`) — reuse them for observability assertions.
