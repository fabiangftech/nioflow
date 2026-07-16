# RFC 0026 — The off-boss key-lane release at shutdown must not recurse or race

- **Status**: 🔵 Proposed
- **Target**: `core/` (`DefaultNioEngine.Execution.releaseKey`, and the off-boss `fail()` path in `resumeOnBoss`)
- **Depends on**: RFC 0024 (the atomic terminal — closes the double-release half of this finding)
- **Severity**: **Medium** — dedicated-engine shutdown with a keyed backlog only; contradicts two stated invariants at once
- **Sibling of**: RFC 0024 (same off-boss shutdown corner)

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
