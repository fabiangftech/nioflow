# RFC 0010 — The last hop: complete on the worker

- **Status**: **Rejected — measured a throughput regression on the hot path** (implemented on a branch, benchmarked, reverted). See [Measured: why this backfires](#measured-why-this-backfires).
- **Target**: `core/` (`application.facade`), `tests/`
- **Depends on**: RFC 0007 (cancellation) — implemented; interacts with the `finished` guard
- **Part of**: the throughput series (0009–0017)

## Measured: why this backfires

Implemented in full (worker-side terminal, `finished` as a CAS, keyed executions keep the boss hop) and benchmarked before/after **back-to-back in both orderings** to cancel machine drift. It is a **consistent regression** on the canonical synchronous request/response path:

| Benchmark | boss-loop (0009 HEAD) | with 0010 | Δ |
| --- | --- | --- | --- |
| `engineCall` @1 | ~93 ops/ms | ~68 | **−27%** |
| `fluentExecute` @1 | ~94 | ~75 | **−20%** |
| `perRequestBuilder` @8 | ~88 | ~62 | **−30%** |
| `perRequestBuilder` @32 | ~76 | ~57 | −25% |
| `engineCall` @8 / @32 | ~84 / ~72 | ~75 / ~68 | −11% / −6% |

For **−48 B/op** of allocation (−8%, the one thing that did improve). Not a trade worth making. The direction reproduced with 0010 running first *and* second, so it is the code, not thermal ordering.

**Why the RFC's premise inverted: RFC 0009 changed the economics it assumed.** This RFC was written when the boss hop cost an `unpark` syscall (~5 µs) — removing it looked like a clear win. But 0009 turned the boss into a hot, spinning `BossLoop`: the hop back to it is now a CAS-enqueue that a spinning platform thread picks up in nanoseconds and immediately completes the future, waking the caller. Moving that terminal onto the **virtual worker** instead means the worker completes the future and then unmounts from its carrier — and on the synchronous path (caller blocked on `join()`), a virtual thread waking the caller and unwinding is *slower and noisier* than the hot boss doing it. So 0010 removes a hop that 0009 already made nearly free, and pays a virtual-thread completion cost that exceeds the saving.

**The lesson is the point of measuring:** two throughput changes that each look additive on paper can be anti-synergistic. 0009 captured the win in this region; 0010, layered on top, gives it back. The non-throughput benefits (boss headroom under high async concurrency, `onComplete` isolation) are real but do not show up on these benchmarks and do not justify a 6–30% hit on the most common path. If they are ever wanted, they need a different design — e.g. gated on complete-handlers actually being installed — argued on its own merits, not as a throughput RFC.

Everything below is the original proposal, kept for the record.

---

## Summary

The canonical chain fuses into one worker run and ends there — yet it still costs **three** thread hops, because the terminal (`complete()`) is posted back to the boss for no reason but habit. `complete()` touches no orchestration state (except a keyed execution's lane). Let the worker that finished the run complete the execution itself: **three hops become two.**

## The hop being removed

```mermaid
sequenceDiagram
    participant B as boss
    participant W as worker
    Note over B,W: today — 3 hops
    B->>W: dispatch fused run
    W->>W: applyRun(...) → value
    W-->>B: resumeOnBoss(advance→complete)
    B->>B: complete(value)
    Note over B,W: proposed — 2 hops
    B->>W: dispatch fused run
    W->>W: applyRun(...) → value
    W->>W: complete(value) directly
```

The trigger is exact: the fused run consumed the rest of the chain, i.e. `resumeAt == links.size()`. Then the worker holds the final value and there is nothing left to advance.

## What `complete()` actually touches

Reading `DefaultNioEngine:1016`: the `finished` flag, the metrics sink, the complete handlers, the result future — and, **only for keyed executions**, `releaseKey()`. None of the first four is orchestration state. Only the key lane is.

```mermaid
flowchart TD
    W[worker finishes run] --> K{keyed?}
    K -- yes --> HOP[resumeOnBoss → complete on boss]
    K -- no --> CAS{CAS finished false→true}
    CAS -- won --> DONE[metrics · handlers · result.complete]
    CAS -- lost --> STOP[cancel already terminated it]
    HOP --> RK[releaseKey walks the lock-free lane deque]
```

## Design

### 1. `finished` becomes a CAS

Today `if (finished) return; finished = true;` (`DefaultNioEngine:1017`) is a check-then-set that is safe *only because every writer is the same boss* — which is exactly why `cancel()` posts its terminal **to** the boss (`DefaultNioEngine:1065`). Completing on a worker makes it a real race against `cancel()`, so exactly-once must be enforced properly: one `VarHandle` CAS per execution. Nanoseconds against the microseconds of the hop it removes.

### 2. Keyed executions keep the hop

`releaseKey()` walks the `KeyLane` deque, which is lock-free **because** only its key's boss touches it. A keyed execution completes on the boss, as today. The optimization applies to unkeyed traffic — the common case.

### 3. The async path keeps its boss hop

An `AsyncStage` completes on **whatever thread the remote call completes on** — a Netty event loop, an `HttpClient` selector. Running complete handlers (user code) there would be strictly worse than the boss. So this RFC's shortcut is for the **blocking fused run only**, which finishes on a worker we own.

## A side effect, and it is an improvement

Complete handlers (`onComplete(...)`) are user code. Rule 2 says the boss never runs user code — yet today the terminal path runs them on the boss, quietly breaking its own rule. Moving them to the worker means a slow `onComplete` delays **one** request, not every execution pinned to that boss.

## Invariants

- **Only the boss touches orchestration state.** The terminal touches none (keyed: still on the boss).
- **Exactly-once completion.** Now enforced by CAS instead of by single-writer discipline — strictly stronger.
- **Cancellation reaches the terminal.** `cancel()` still posts to the boss; the CAS is the arbiter between it and a worker-side complete.

## Testing

- **Exactly-once race**: hammer `cancel()` and the terminal from two threads on the same execution; assert exactly one of `complete`/`cancelled` is observed and the drain returns 0.
- **Handler thread**: assert `onComplete` runs off the boss for an unkeyed execution and on the boss for a keyed one.
- Full existing suite unchanged (`DefaultNioFlowCancellationTest`, `KeyedExecutionStressTest`).

## Gate

| Benchmark | Must |
| --- | --- |
| `fluentExecute`, `engineCall` | improve (removes 1 of 3 hops) |
| `perRequestBuilder` keyed path | unchanged (keeps its hop) |
| `-prof gc` | not rise |

## Risks

- **Which thread runs `onComplete` changes** (boss → worker) for unkeyed executions. The docs promise "callbacks run on engine threads", still true; the specific thread changes. Release note, since anything relying on boss affinity in a callback relied on something never promised.
- **The CAS is now load-bearing for correctness, not just perf.** It is covered by the exactly-once race test above.
