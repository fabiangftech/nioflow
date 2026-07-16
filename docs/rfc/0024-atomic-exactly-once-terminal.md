# RFC 0024 — An atomic exactly-once terminal, so shutdown never double-counts the drain

- **Status**: ✅ Implemented — `finished` is an `AtomicBoolean` CAS; 2 tests (a deterministic clean-drain, a barrier-driven race bug-hunter)
- **Target**: `core/` (`DefaultNioEngine.Execution` — `finished`, `complete`, `fail`, `cancel`)
- **Depends on**: RFC 0007 (cancellation, one of the two off-boss writers), RFC 0009 (the boss model the guard assumes)
- **Severity**: **Medium** — narrow (dedicated-engine shutdown only), but it defeats the exact guarantee `shutdown(grace)` sells
- **Sibling of**: RFC 0023, RFC 0026 (the other off-boss shutdown paths)
- **Realized by**: `Execution.finished` changed from a `volatile boolean` check-then-set to a `final AtomicBoolean` — `complete`/`fail` gate on `compareAndSet(false, true)`, `cancel` reads `finished.get()`. A new package-private test hook `DefaultNioEngine.inFlightCount()` exposes the drain sum so a double decrement is observable as a negative value. Tests: `DefaultNioEngineShutdownRaceTest`.

## The finding

The exactly-once completion guard is a plain `volatile boolean` with a
**non-atomic check-then-set**:

```java
private volatile boolean finished;

private void complete(Object value) {
    if (finished) return;
    finished = true;            // read-then-write, not atomic
    finishBookkeeping(value, null);
    result.complete(value);
    if (laneHeld) releaseKey();
}
```

In steady state this is correct because `complete`/`fail` only ever run on the
one boss thread that owns the execution. The field's own comment says as much —
*"the only off-boss writer is the resume-rejection path"*. **That comment
undercounts by one.** There are **two** off-boss terminal paths, and they can
race each other:

1. `resumeOnBoss(...)` catches a `RejectedExecutionException` and calls
   `fail(rejected)` **on a worker thread** — reached when an engine-owned boss
   executor was shut down mid-flight.
2. `cancel()` catches a `RejectedExecutionException` from `boss.execute(...)`
   and calls `complete(CANCELLED)` **on the caller thread** — reached when the
   same boss is gone.

During `shutdown(grace)` of a `dedicated(...)` engine, both fire for the same
execution concurrently: a worker finishing a stage hits a rejected
`resumeOnBoss` while an outside `cancel()` also finds the boss gone. Both read
`finished == false`, both proceed. `result` (a `CompletableFuture`) tolerates
the double completion — but `finishBookkeeping` runs **twice**, so
`activeExecutions.decrement()` is called twice for one execution.

The drain counter is now corrupt. `awaitDrain` polls `activeExecutions.sum()`
and returns when it reaches zero — so it can report a **clean drain (0)** while
other executions are still in flight, because one execution decremented for two.
`shutdown(grace)` returning `0` is documented to mean "everything reported";
here it can lie, which is precisely the promise the graceful drain exists to
keep.

## Why it blocks production

Graceful shutdown is a production-only code path — it is what a rolling deploy,
a pod eviction, or a `dedicated` engine's `shutdown()` runs on every release. A
drain that reports clean while work is still running means a deploy can cut a
straggler mid-flight and believe it didn't. The bug is invisible in
steady-state tests and in every benchmark; it only shows under the exact
conditions (dedicated engine, in-flight workers, concurrent cancel, executor
rejection) that a real shutdown creates. "Narrow" here means "only happens in
production."

## The fix

Make the terminal transition atomic so `finishBookkeeping` runs exactly once
regardless of thread. Replace the `volatile boolean` with an `AtomicBoolean`
(or a `VarHandle` CAS on an `int`, to match the `BossLoop` style and dodge
S3077) and gate both terminals on winning the CAS:

```java
private final AtomicBoolean finished = new AtomicBoolean();

private void complete(Object value) {
    if (!finished.compareAndSet(false, true)) return;
    finishBookkeeping(value, null);
    result.complete(value);
    if (laneHeld) releaseKey();
}

private void fail(Throwable error) {
    if (!finished.compareAndSet(false, true)) return;
    finishBookkeeping(null, unwrap(error));
    result.completeExceptionally(error);
    if (laneHeld) releaseKey();
}
```

Now the loser of the race returns before touching `activeExecutions`, so the
drain counter decrements exactly once. The steady-state cost is a single
uncontended CAS per execution terminal — off the per-link hot path entirely, on
a path that already allocates the report. Confirm with the existing
`NioFlowBenchmark` / `SyncStageBenchmark` allocation gates that the hot path is
unchanged (it is: the terminal is not per-link).

`cancelled` and `pendingCall` are unaffected — this RFC is only about the
`finished` transition. The comment at the field should be corrected to name
both off-boss writers, so the next reader doesn't re-derive the wrong count.

Fix RFC 0026 (the off-boss `releaseKey` recursion) in the same pass: once
`finished` is a CAS, the double-`releaseKey` half of that finding is already
closed, leaving only its recursion/visibility half.

## Testing

`DefaultNioEngineShutdownRaceTest` (RFC 0020 style, deterministic where
possible, `orTimeout` on every joined future):

- A `dedicated(1)` engine, an execution parked on a controllable async stage,
  then `shutdown(0)` and an outside `cancel()` driven to overlap (a
  `CyclicBarrier` shared by the cancel thread and a worker-side hook). Assert
  `activeExecutions` reaches exactly zero — not below — and `shutdown` returns a
  count consistent with what actually finished.
- A stress variant: N dedicated engines, each with a keyed backlog, cancelled
  and shut down concurrently in a loop; assert `activeKeyLanes() == 0` and the
  drain count is never negative (a negative sum is the smoking gun of a double
  decrement).
- Guard against regression of the *ordering* contract: bookkeeping still runs
  before `result` completes (an `onComplete` that records a timestamp, asserted
  earlier than the `join()` return).

## Risks

- **A second CAS on an already-hot object.** It is one uncontended CAS on the
  terminal, not per link; the allocation and throughput gates will show it is
  free. If a future profile ever disagrees, the `VarHandle`-int form has the
  same cost as the current volatile write plus a compare.
- **Behavioural change on double-terminal.** Today a double `complete` silently
  no-ops the future but double-runs bookkeeping; after the fix it no-ops both.
  That is strictly the intended semantics — nothing relied on bookkeeping
  running twice.

## Results

Shipped as the `AtomicBoolean` form (clearer than a `VarHandle` int, and a final
object field sidesteps S3077 — no new static-analysis findings). The field
comment now names **both** off-boss writers, correcting the "only one" the old
comment claimed.

- **It closes half of RFC 0026 for free, as predicted.** With a CAS terminal,
  the two off-boss paths can no longer both reach `releaseKey()` — the loser
  returns first — so the *double-release* symptom of RFC 0026 is already gone.
  What 0026 still has to fix is the other half: the off-boss `releaseKey`
  recursing on a worker stack and reading a racy deque. This RFC does not touch
  that; it is sequenced first precisely so 0026 lands on a clean terminal.

- **The race is a bug-hunter, and it says so.** The cross-thread double-terminal
  (a worker's rejected `resumeOnBoss` racing an outside `cancel` with the boss
  gone) cannot be forced deterministically through the public API — there is no
  seam to pause a worker mid-`resumeOnBoss`. So the test drives three threads
  (shutdown, cancel, async-completion) to overlap on a `CyclicBarrier`, over 120
  iterations of a fresh `dedicated(1)` engine, with the precise oracle that the
  drain counter (exposed by the new `inFlightCount()` hook) must never go
  **negative** — the unmistakable signature of one execution decrementing twice.
  A deterministic companion (`aCleanDedicatedShutdownDrainsToExactlyZero`) pins
  the ordinary contract: 50 executions drain to exactly zero and `shutdown`
  returns 0.

`cd core && ./gradlew test` green (full suite, including the drain and dedicated-
pool tests); `cd reactive && ./gradlew test` green; SonarLint diff over `core` is
empty.
