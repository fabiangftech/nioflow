# RFC 0026 — The off-boss key-lane release at shutdown must not recurse or race

- **Status**: ✅ Implemented — successor handoff posted to the boss, iterative drain when the boss is gone, lane queue made concurrent; 2 tests
- **Target**: `core/` (`DefaultNioEngine.Execution.releaseKey`, `KeyLane.waiting`)
- **Depends on**: RFC 0024 (the atomic terminal — closes the double-release half of this finding)
- **Severity**: **Medium** — dedicated-engine shutdown with a keyed backlog only; contradicts two stated invariants at once
- **Sibling of**: RFC 0024 (same off-boss shutdown corner)
- **Realized by**: `releaseKey` now hands the lane to its successor as a fresh boss task (`boss.execute`) instead of an inline `advance`, and on rejection (the boss is gone) drains the whole backlog in `drainLaneOnShutdown` — a loop, never recursion. `KeyLane.waiting` changed from `ArrayDeque` to `ConcurrentLinkedQueue` so the off-boss `poll` is safe (FIFO and lock-freedom preserved). Test hook `keyLaneDepth(key)` added. Tests: `DefaultNioEngineKeyedShutdownTest`.

## The finding

`releaseKey()` hands the key lane to the next same-key execution:

```java
private void releaseKey() {
    KeyLane lane = keyLanes.get(key);
    if (lane == null) return;
    Execution next = lane.waiting.poll();   // ArrayDeque — not thread-safe
    if (next == null) {
        keyLanes.remove(key);
    } else {
        next.laneHeld = true;
        next.advance(0, next.input);         // drives the successor inline
    }
}
```

`KeyLane.waiting` is a plain `ArrayDeque` and `lane.active` a plain field,
deliberately un-synchronized because the design guarantees they are only ever
touched by the key's boss (deterministic affinity — `bossFor(key)`). That
guarantee holds in steady state. It **breaks during shutdown of a dedicated
engine**, where `releaseKey` is reachable off the boss via the same worker-thread
`fail()` path RFC 0024 describes (`resumeOnBoss` rejects → `fail(rejected)` on a
worker → `complete`/`fail` sees `laneHeld` → `releaseKey`). Two distinct
problems in that corner:

1. **Unbounded recursion on a worker stack.** The off-boss `releaseKey` calls
   `next.advance(0, next.input)` directly on the **worker** thread. That
   successor dispatches → `resumeOnBoss` rejects again → `fail` → `releaseKey`
   → the *next* successor's `advance` → … The whole key backlog unwinds as
   **stack recursion on a single worker thread** — the precise thing the
   "`advance` must be iterative, never recursive" invariant (and
   `DeepChainStressTest`) exists to prevent. A deep enough backlog overflows the
   worker stack; the boss's stack was protected, but this path runs on a worker.

2. **A racy deque read.** `lane.waiting` and `lane.active` were last written on
   the (now-dying) boss. A worker reaching `releaseKey` has no guaranteed
   happens-before to a `waiting.add(next)` that a producer performed after this
   worker was dispatched, so the `poll()` can observe a torn or stale deque.

RFC 0024's atomic `finished` closes the *double-release* symptom (two terminals
can't both call `releaseKey`), but neither the recursion nor the visibility race
is addressed by that CAS — they are properties of `releaseKey` running off-boss
at all.

## Why it blocks production

Keyed execution is the feature operators reach for to get Kafka-style per-key
ordering — and the systems that use it (ordered event processing, per-account
serialization) are exactly the ones that carry a **backlog** per key. A rolling
deploy shutting down a `dedicated` engine that still holds a keyed backlog is a
routine production event, not an exotic one. A stack overflow on a worker
during drain, or a torn deque read that drops or duplicates a queued execution,
turns an orderly shutdown into data loss or a crash — under load, at the worst
possible moment.

## The fix

The root cause is that `releaseKey` — which touches lane state the boss owns —
runs on a worker when the boss executor has been shut down. Two layers:

1. **Never advance the successor inline; always post it to its boss.** Replace
   the direct `next.advance(0, next.input)` with a boss hop:

   ```java
   } else {
       next.laneHeld = true;
       try {
           boss.execute(() -> next.advance(0, next.input));
       } catch (RejectedExecutionException gone) {
           next.fail(gone);   // iterative unwind via the queue, not the stack
       }
   }
   ```

   This turns the recursion into iteration: each successor's failure is posted,
   not called, so the backlog unwinds through the executor/queue with O(1)
   stack. When the boss is gone, `next.fail(gone)` still hands *its* successor
   off the same way — a chain of posts, never a chain of frames. (The engine's
   own executors are single-consumer `BossLoop`s that reject once shut down, so
   the fallback is what runs during drain; the point is it recurses through the
   heap-backed queue, not the call stack.)

2. **Confine lane mutation to the boss, or make it safe off it.** Preferred:
   ensure `releaseKey` is only *ever* invoked on the key's boss. Since a
   terminal can fire off-boss during shutdown, post the *whole* release to the
   boss when we are not already on it — cheap, and it restores the "one thread
   touches lane state" invariant the un-synchronized `ArrayDeque` relies on.
   Where that is impractical (the boss is gone), the lane is being torn down
   anyway; drain it under the group's own terminal-once guarantee so no queued
   execution is left un-failed. Either way, `activeKeyLanes()` must reach zero.

Sequence this **after RFC 0024**: the atomic terminal removes the double-entry,
leaving `releaseKey` with a single well-defined caller per terminal, which makes
the "post to boss / iterative unwind" fix clean.

## Testing

Extend `KeyedExecutionStressTest` and add `KeyedShutdownDrainTest`:

- Submit a deep same-key backlog (thousands) to a `dedicated(1)` engine, then
  `shutdown(0)` while workers are mid-flight. Assert **no `StackOverflowError`**
  anywhere (the recursion regression), every future settles (value or rejection,
  never a hang — `orTimeout`), and `activeKeyLanes() == 0` after drain.
- A deterministic variant that forces the off-boss `fail` path (a controllable
  async stage + an executor shut down at a chosen instant) and asserts the
  successor still ran or failed exactly once — never skipped, never doubled.
- Regression: steady-state per-key FIFO ordering is unchanged (the existing
  submitted-sequence == processed-sequence oracle).

## Risks

- **A boss hop where there was an inline call.** In steady state `releaseKey`
  already runs on the boss, so posting `next.advance` to the same boss is one
  extra enqueue on the boss's own MPSC queue — measured by the keyed benchmark;
  expected negligible, and it removes a subtle "successor runs inside
  predecessor's terminal frame" coupling that is worth losing regardless.
- **Interaction with RFC 0024.** These two must land together or 0024 first;
  0024 alone leaves the recursion, this alone leaves the double-entry. The index
  records the ordering.

## Results

Shipped both layers, and the fix turned out to help the *steady-state* path too.

- **Posting the successor to the boss is universally correct, not just a
  shutdown fix.** The inline `advance` recursed one frame per queued execution —
  off-boss during shutdown (a worker's stack, unprotected) *and*, it turns out,
  on the boss itself for an all-inline (`handleSync`) chain, whose successors run
  to completion without ever yielding to the boss loop. Posting each handoff as a
  fresh boss task makes both O(1) stack. FIFO is unchanged (a same-key arrival
  still finds the lane active and queues behind), confirmed by the existing keyed
  tests and a new parked-head ordering test.

- **The concurrent queue closes the visibility race.** During `shutdown` there is
  a real window where the boss consumer is still draining queued `run()` tasks
  that `add` to `waiting` while a rejected `resumeOnBoss` reaches `poll` from a
  worker. `ConcurrentLinkedQueue` makes that safe with proper happens-before,
  where the `ArrayDeque` gave a torn or stale read; it stays lock-free, so the
  keyed lane's single-writer steady state pays nothing structural.

- **The test is not vacuous — verified.** Enrolling a 20 000-deep same-key
  backlog behind a parked head, then cancelling the head off the boss, exercises
  the exact cascade. Temporarily restoring the inline-recursive release makes the
  test fail (the backlog is left un-drained / the stack blows), and the iterative
  version drains it to zero — so the test genuinely guards the regression. A
  second test pins that the posted handoff keeps strict per-key FIFO.

Half of this landed for free with RFC 0024: the atomic terminal already stopped
the *double* release, so this RFC only had to make the *single* release
recursion-free and race-free. `cd core && ./gradlew test`, `cd reactive &&
./gradlew test`, and the `tests/` keyed stress suite are all green; SonarLint
diff over `core` is empty.
