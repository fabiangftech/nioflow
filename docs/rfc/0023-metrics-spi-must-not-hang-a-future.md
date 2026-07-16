# RFC 0023 — A throwing metrics SPI must not hang a future

- **Status**: ✅ Implemented — every `NioFlowMetrics` call in `Execution` routed through a guard; 6 deterministic tests
- **Target**: `core/` (`DefaultNioEngine.Execution` — `reportExecution`, `reportFork`, `settleAsync`, and every per-stage/retry/recovery report)
- **Depends on**: RFC 0009 (the boss loop that swallows the throw)
- **Severity**: **High** — a reachable hung request future, the exact failure mode the engine is otherwise obsessive about preventing
- **Sibling of**: RFC 0024 (the other terminal-path hardening), RFC 0029 (handlers on the boss)
- **Realized by**: `DefaultNioEngine.Execution.meter(Runnable)` (cold reports) + `meterStageCompleted(String, long)` (the per-stage path, guarded without a lambda so a metrics-enabled flow allocates nothing extra); every `metrics.*` call in the execution now goes through one of them. Tests: `DefaultNioEngineMetricsHardeningTest`. SPI javadoc (`NioFlowMetrics`) updated to promise containment; S1181 entry in `tools/sonarlint/README.md` extended.

## The finding

`Execution.reportExecution` calls the user-supplied metrics sink **without a
try/catch**:

```java
if (metrics != null) {
    long elapsed = System.nanoTime() - startNanos;
    if (error != null)            metrics.executionFailed(error, elapsed);
    else if (value == FILTERED)   metrics.executionFiltered(elapsed);
    else if (value == CANCELLED)  metrics.executionCancelled(elapsed);
    else                          metrics.executionCompleted(elapsed);   // NOT guarded
}
```

The call chain is `complete(value)` → `finishBookkeeping(value, null)` →
`reportExecution(...)`. If the sink throws, the exception propagates back out of
`complete()` **before `result.complete(value)` ever runs**. `finished` was
already set to `true`, so nothing retries; `BossLoop.run` merely hands the
Throwable to the uncaught-exception handler and keeps ticking. **The caller's
future never completes.** A blocking `execute()` waits forever; a
`executeAsync()` returned to a controller never responds.

This is asymmetric and clearly unintended: the `completeHandlers` loop **right
below it** (and `notifyError`) *are* hardened against throwing user code. The
metrics SPI — `OpenTelemetryMetrics`, or any consumer sink — is equally
user-supplied and equally capable of throwing (a sink bug, a sink that throws
during its own shutdown, an exporter that fails). It just was not wrapped.

Two more instances of the same shape:

- **Keyed executions make it worse.** The throw also skips `releaseKey()` at the
  end of `complete()`/`fail()`, so one throwing metric call hangs the **entire
  key lane**, not just one request — every later same-key execution is stuck
  behind a head that will never release.
- **`settleAsync` on the boss** calls `metrics.stageCompleted(...)` before
  `advance(resume, nextValue)`; a throw there skips the advance and hangs the
  future the same way.
- **`reportFork`** is likewise unguarded — no hung future there (a fork has no
  caller future), but the fork's `notifyError` is silently skipped and the
  drain slot still decrements in the `finally`, so a failed fork can go
  unreported.

## Why it blocks production

A metrics sink is the *first* thing a production deployment installs
(`engine.metrics(otel)`). The one component every serious operator adds is the
one component whose failure can wedge a request permanently. A hung future is
not a degraded response — it is a leaked thread on the caller side, a socket
held open, a health check that never fires. Under an exporter outage that makes
the sink throw, the blast radius is every in-flight request, and for keyed
traffic, every future request on those keys.

## The fix

Wrap every metrics-SPI call the same way the handler calls are already wrapped:
a throw from the sink is reported through `notifyError` (the sink is not allowed
to break the flow), and completion proceeds. The invariant to state and test:
**no method on `NioFlowMetrics` can prevent `result` from completing or a key
lane from releasing.**

Two implementation options, in order of preference:

1. **A guarded shim at the single call site.** Route every sink call through one
   private helper that swallows-and-reports:

   ```java
   private void meter(Runnable sinkCall) {
       if (metrics == null) return;
       try { sinkCall.run(); }
       catch (Throwable failure) { notifyError(failure); }
   }
   ```

   and rewrite `reportExecution`/`reportFork`/`settleAsync`/`stageCompleted`
   sites as `meter(() -> metrics.executionCompleted(elapsed))`. One place to get
   right, symmetric with `completeHandlers`. Costs one lambda per report on the
   already-cold reporting path (not the per-link hot path), so no benchmark
   concern — but confirm with the `NioFlowBenchmark` allocation gate that the
   *hot* path is untouched (it is: metering only runs at terminal + per-stage,
   and per-stage already allocates the timing).

2. **Complete the future first, then meter.** Move `result.complete(value)`
   ahead of `finishBookkeeping`. This removes the hang even if a sink throws,
   but it **reorders the contract** (`CLAUDE.md` guarantees bookkeeping runs
   *before* the future completes, so a joining caller observes metrics already
   pushed). That ordering is load-bearing for tests and for
   `onComplete`-before-`execute()-returns`. **Rejected for that reason** —
   option 1 keeps the ordering and fixes the bug.

Prefer option 1: guard the sink, keep the ordering.

## Testing

An RFC 0020-style deterministic unit test, `DefaultNioEngineMetricsHardeningTest`:

- Install a `NioFlowMetrics` whose `executionCompleted` throws. Assert
  `just(x).execute()` still returns the value, and the throw surfaced through an
  installed `onError` handler.
- Same for `executionFailed` on the recovery-exhausted path, and for
  `stageCompleted` on an async stage (guards `settleAsync`).
- **Keyed regression**: a throwing `executionCompleted` on a keyed execution
  must still release the lane — submit two executions on the same key, make the
  first sink call throw, assert the second completes (with an `orTimeout` so a
  hang is a visible failure, not an infinite wait).
- A throwing `forkCompleted` must still decrement `forksInFlight` and reach
  `onError`.

## Risks

- **Swallowing a sink's own signal.** A sink that throws to signal
  back-pressure would have that swallowed and merely reported. That is correct:
  a metrics sink is a side effect, never a control-flow input — the same stance
  the handlers already take.
- **Masking a sink bug.** The throw is reported through `notifyError`, not
  discarded, so a broken sink is visible in the error stream rather than in a
  hung request. That is the trade we want.

## Results

Option 1 shipped as designed. Every `metrics.*` call inside `Execution` — the
terminal classification (`executionCompleted/Failed/Filtered/Cancelled`), the
fork reports, the per-stage `stageCompleted` on both the blocking and the async
paths, the retry and recovery counters, and `forkStarted` — now routes through a
guard, so no `NioFlowMetrics` method can hang a future, skip an advance, misroute
a good value into `recover()`, or leave a key lane stuck.

Two implementation notes worth recording (the "implementation is the review"
effect):

- **The per-stage path uses a lambda-free guard.** The draft's single
  `meter(Runnable)` shim would have allocated a captured `Runnable` per stage for
  a metrics-enabled flow — the library measures per-op bytes and gates them, so
  the hot `stageCompleted` call got a dedicated `meterStageCompleted(name,
  elapsed)` helper instead. The allocation gates only run without metrics
  installed, but the principle (no new per-stage allocation for metrics users)
  is the house rule, so `meter(Runnable)` is reserved for the cold, once-per-
  execution reports.
- **Extracting the classifier kept `reportExecution` under the complexity
  limit.** Wrapping the four-way outcome branch in the `meter` lambda pushed
  `reportExecution` from 15 to 16 cognitive complexity (SonarLint S3776). The
  branch moved to a private `classifyExecution(value, error, elapsed)`, so the
  lambda is one call and the method drops back under the limit. The only new
  static-analysis findings are the two documented S1181 `catch (Throwable)` in
  the guards — the same deliberate pattern the handler code already carries.

`cd core && ./gradlew test` green (full suite); `cd reactive && ./gradlew test`
green (composite against the changed core); SonarLint diff over `core` is exactly
the two documented S1181 and nothing else.
