# RFC 0007 — Cooperative cancellation

- **Status**: Proposed
- **Target**: `core/` (`core.model`, `core.facade`, `application.facade`, `infrastructure.reactive`)
- **Depends on**: **RFC 0006 (`AsyncStage`)** — see [Why this depends on 0006](#why-this-depends-on-0006)
- **Closes**: the risk row `0002:458` — *"Cancellation leaks work: a client disconnects and the pipeline keeps charging a card"*

## Summary

The engine has no cancellation. A `grep` for `cancel|interrupt|CancellationException` over `core/src/main/java` finds the `TimerWheel`, the `inFlight` queue's `InterruptedException` handling, and one javadoc sentence — nothing else. `ReactiveCancellationTest` says it out loud:

> *"the engine has no cancellation. A cancelled Mono abandons the RESULT, not the work — the execution runs to its end."*

Which means: **a client disconnects, and the pipeline charges the card anyway.** RFC 0002 named this and deferred it ("real cancellation is Future work and needs engine support"). This is that engine support.

Three parts:

1. a `volatile boolean cancelled` on `Execution`, checked in the loop `advance` already runs;
2. `FlowSignal.CANCELLED` as a terminal state, with `executionCancelled` on the metrics SPI and a `Cancelled` case on `FlowResult`;
3. a handle to cancel with — `NioStep.executeCancellable()` in core (no Reactor in the signature), and a `executeMono()` that finally disposes what it started.

Cancellation is **cooperative, not preemptive**, and this RFC is explicit about the difference rather than hopeful about it.

## Why this depends on 0006

A parked virtual thread cannot be woken. Today a `handleMono` stage sits inside `mono.block()` (`Blocking.java:37`), and there is nothing the engine can do about it: no flag it can read, no future it can complete, nothing to `dispose()`. Cancelling an execution whose only in-flight work is a parked worker would stop the chain *after* the call finishes — which is not cancellation, it is bookkeeping.

RFC 0006's `AsyncStage` is what turns the in-flight remote call into a **`CompletionStage`** — a handle you can cancel, which (verified in `MonoToCompletableFuture:40-48`) tears down the subscription and releases the connection. That is the difference between "the chain stops advancing" and "the card is not charged".

Without 0006 this RFC is worth little. With it, it is worth the feature.

## The plumbing gap nobody has noticed yet

Even the modest fix — "propagate the Mono's cancel to the future" — **does not work today**, and it is worth seeing why before designing anything.

```java
// DefaultReactiveStep:85
public Mono<T> executeMono() {
    return Mono.fromFuture(delegate::executeAsync);
}

// ExecutionNioFlow:238
public CompletableFuture<T> executeAsync() {
    return rawFuture().thenApply(v -> v == FlowSignal.FILTERED ? null : (T) v);
}
```

`Mono.fromFuture` does cancel the future it was handed on dispose — but the future it was handed is the **dependent** one (`thenApply`'s). `execution.result` never hears about it. The engine's own `call()` returns `execution.result` straight to the caller (`DefaultNioEngine:291`) and then **nothing observes it**: no `whenComplete`, no `isCancelled()` check anywhere.

So the first thing this RFC needs is a handle that actually reaches the execution.

## Design

### 1. The flag, checked where the loop already is

`advance` (`DefaultNioEngine:1094`) is the iterative walk over the links, and every off-boss resume path — dispatch, batch, fan-out, timeout/retry, recovery — funnels back into it. It already tests one condition per link. It gains a second:

```java
private void advance(int index, Object value) {
    Object current = value;
    while (index < links.size()) {
        if (cancelled) { finishCancelled(); return; }   // ← between links, on the boss
        Link link = links.get(index);
        if (passesGuards(link)) { ... }
        index++;
    }
    complete(current);
}
```

`cancelled` is a `volatile boolean` on `Execution`, written from any thread and read **only on the boss** — so rule 1 (only the execution's boss touches its orchestration state) holds unchanged, and no lock enters the hot path.

Cancelling also does what it can to the work in flight: if the execution is currently inside an `AsyncStage`, its `CompletionStage` is cancelled.

### 2. What cancellation reaches — and what it does not

| In flight when the cancel arrives | What happens |
| --- | --- |
| Boss-side links (`Decision`, `Filter`, guard-skips, `handleSync`) | stops at the next link boundary — microseconds |
| An **`AsyncStage`** (RFC 0006) | its `CompletionStage` is cancelled → **the remote call dies**, the connection is released |
| A `Stage` on a worker (JDBC, `Blocking.await`) | **not interrupted.** The worker runs to completion; its result is discarded at the next boundary |
| A parked `batch` group | the execution leaves the group; the bulk still runs for the others |
| A `Fork` already spawned | **unaffected.** A fork is detached by definition (RFC 0001); cancelling the parent does not cancel the child |

Row three is the limitation and it does not get softened: **cancellation is cooperative.** A blocking JDBC call is not interruptible, and this RFC does not pretend to make it so — `Thread.interrupt` on a virtual worker running arbitrary user code is a worse bug than the one it fixes. What cancellation buys is that the chain stops advancing, no *further* stage runs, and the in-flight remote call on an async stage is genuinely cancelled. The card is not charged because `charge` is the stage that never gets invoked.

Row five is a deliberate contract, not an oversight: RFC 0001 made forks detached on purpose. "Cancel my children" is a different contract and needs its own argument.

### 3. The terminal state

`FlowSignal` gains `CANCELLED` beside `FILTERED`:

```java
public enum FlowSignal { FILTERED, CANCELLED }
```

- **Metrics**: `NioFlowMetrics.executionCancelled(long nanos)`, a default no-op like every other method on the SPI.
- **`FlowResult`**: a third case beside `Completed(value)` and `Filtered` — `executeResult()` is the API that already exists to tell "nothing" apart from "nothing on purpose", and this is a third kind of nothing.
- **Bookkeeping still runs.** `finishBookkeeping(...)` fires before completion (`DefaultNioEngine:951`), so the drain slot is released and `shutdown(grace)` still returns 0 stragglers — the property `ReactiveCancellationTest:76` pins today stays pinned.
- **`executeAsync()`** completes with a `CancellationException`; **`executeMono()`** emits nothing (a disposed subscriber gets no signal anyway — Reactor's contract, not ours).

### 4. The handle

Core first, with no Reactor in the signature — the same discipline `NioStep.with(Context.Key, value)` followed in RFC 0002:

```java
// core/facade/NioStep.java
Cancellable<T> executeCancellable();

// core/facade/Cancellable.java
public interface Cancellable<T> {
    CompletableFuture<T> future();
    void cancel();               // idempotent; a finished execution ignores it
}
```

A non-reactive caller who wants to abandon a request gets this too — an HTTP server on virtual threads whose client disconnected has exactly the same problem.

And then the reactive terminal becomes what a Reactor user already assumes it was:

```java
// DefaultReactiveStep
public Mono<T> executeMono() {
    return Mono.defer(() -> {
        Cancellable<T> handle = delegate.executeCancellable();
        return Mono.fromFuture(handle.future())
                   .doOnCancel(handle::cancel);
    });
}
```

Laziness is preserved (`Mono.defer` — one execution per subscription, so `.retry()`/`.repeat()` still re-run the pipeline), and the dispose now reaches the execution instead of a dependent future.

## The behavior change

`ReactiveCancellationTest:97` currently asserts that a cancelled request reports **`executionCompleted`**. Under this RFC it reports **`executionCancelled`**, and a dashboard that counts completions will see its numbers move.

That is a real, user-visible change, and it ships as one: a release note and a minor version, not a silent correction. The new metric is additive, so a consumer who ignores it sees a cancelled request in neither bucket — which is at least not a lie.

## Testing

**Core (no Reactor):**

- `CancellationTest` — `executeCancellable().cancel()` stops the chain at the next link boundary; the drain slot is released; `shutdown(grace)` returns 0; the execution reports `executionCancelled` and **not** `executionCompleted`; `executeResult()` returns `Cancelled`.
- **The limitations are asserted, so they cannot silently become promises**: a blocking stage in flight is *not* interrupted (it runs to its end, its result is discarded), and a fork already spawned still completes and still reports `forkCompleted`.
- Cancelling an already-finished execution is a no-op; cancelling twice is a no-op; a cancel racing a completion never leaves the future hanging (the `finished` guard already serializes this on the boss).
- With RFC 0006 in place: cancelling an execution parked on an `AsyncStage` **cancels the `CompletionStage`** — a stub observes the cancellation.

**Reactive:**

- `ReactiveCancellationTest` — **updated**: a disposed subscription now reports `executionCancelled`, and an in-flight `handleMonoAsync` call is cancelled at the client (reactor-netty releases the connection).
- The existing assertions that must keep passing: a cancelled execution never hangs the engine, and the next request is served normally.

**Benchmark:**

- `NioFlowBenchmark` before/after. The volatile read added to `advance`'s loop runs once per link, and this codebase has fought for 20 % on `passesGuards` before. **If it moves the number, the flag folds into the existing `finished` field** (one volatile, two meanings) rather than adding a second read. That is the gate.

## Risks

| Risk | Mitigation |
| --- | --- |
| **Users read "cancellation" as preemptive** and are surprised the JDBC call finished. | Asserted by a test (a blocking stage in flight is not interrupted), stated in the semantics table, and the metric is `executionCancelled`, not `executionAborted`. Preemption is not on the roadmap. |
| **The volatile read taxes the hot path.** | Benchmark gate above; the fold-into-`finished` fallback is the plan, not an improvisation. |
| **`executionCompleted` → `executionCancelled` breaks a dashboard.** | Release note, minor version, additive metric. |
| **A cancel racing a completion double-completes the future.** | The `finished` field already guards `complete()`/`fail()` on the boss, and cancellation goes through the same door. Test pins it. |
| **Cancellation without RFC 0006 looks like it works** (the chain stops) while the expensive remote call still runs. | Which is why the dependency is declared, not implied. Shipping this alone would sell a promise the engine cannot keep. |

## Future work

- **A cancellable `fork`** — a different contract (RFC 0001 made detachment the point), and it needs its own argument.
- **Cancellation of a `batch` bulk** when every member of the group has been cancelled. Today the bulk runs regardless; nobody has asked for it yet.
