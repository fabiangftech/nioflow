# RFC 0025 — Subscription cancellation must not run on the TimerWheel thread

- **Status**: ✅ Implemented — both cancel sites offloaded to a worker; `TimerWheel` contract tightened; 3 deterministic tests
- **Target**: `core/` (`DefaultNioEngine.Execution.attemptAsyncCall` — the timeout action; `Execution.cancel`; `TimerWheel` contract)
- **Depends on**: RFC 0006 (the async stage whose call is cancelled), RFC 0007 (cancellation)
- **Severity**: **Medium-High** — a JVM-wide latency/liveness risk that only shows under real network teardown, so benchmarks never see it
- **Sibling of**: the timer-wheel design in RFC 0009's counter/timer family
- **Realized by**: `DefaultNioEngine.Execution.cancelOffThread(CompletionStage)` — snapshots the in-flight call at the call site and runs `cancel(...)` on a worker (inline fallback if the workers are gone). Both callers switched to it: the async-timeout action (was on the shared timer thread) and `Execution.cancel()` (was on the caller's thread). `TimerWheel` javadoc tightened to state the never-block contract and name the offload. Tests: `DefaultNioFlowAsyncTimeoutIsolationTest`.

## The finding

The `TimerWheel` is a single-thread, single-writer design shared by **every
engine in the JVM**. Its own javadoc states the rule plainly:

> Actions must be cheap and never user code.

`fireDue` runs each due action **inline on the one wheel worker thread**:

```java
if (timeout.deadlineTick <= tick) {
    timeout.action.run();   // on the nio-flow-timer thread
}
```

The async-stage timeout breaks the rule. In `attemptAsyncCall`, the timeout
action does more than complete a future exceptionally — on expiry it
**cancels the in-flight call**:

```java
TimerWheel.Timeout budget = ... schedule(stage.timeout().toNanos(), () -> {
    boolean expired = attemptResult.completeExceptionally(new TimeoutException(...));
    if (expired) {
        cancel(pendingCall);   // toCompletableFuture().cancel(false)
    }
});
```

`cancel(pendingCall)` calls `call.toCompletableFuture().cancel(false)`. For the
canonical async stage — a reactive `mono.toFuture()` — cancelling that future
**disposes the Reactor subscription**, which runs reactor-netty's
connection-teardown logic: returning a pooled channel, possibly closing a
socket, possibly firing user `doOnCancel`/`doFinally` hooks. That is neither
cheap nor free of user code, and it runs on the shared timer thread.

The consequence is cross-tenant coupling with no back-pressure: while one
subscription's teardown is in progress, the timer thread is not ticking, so
**every other pending stage timeout and every batch window in the entire JVM is
stalled** behind it. One slow connection teardown becomes a latency spike for
unrelated flows in unrelated engines. Under a network partition — exactly when
timeouts fire in bursts and teardowns are slowest — the stall compounds.

## Why it blocks production

The wheel is a JVM-wide singleton by design (that is its whole efficiency
argument versus `orTimeout`'s per-call heap). That makes its worker thread a
shared, un-isolated resource. A design that puts potentially-blocking network
teardown on that thread turns the shared timer into a shared point of latency
coupling — the failure mode is "unrelated services on the same JVM get slow
together when any one of them times out a hung upstream," which is the worst
kind of production incident: correlated, cross-boundary, and absent from every
local test.

## The fix

The timer thread's job is to **win the race and complete the attempt future
exceptionally** — that part is cheap and must stay inline. The *cancellation of
the underlying call* is the expensive part and must be handed off. Complete the
future on the wheel thread, then dispatch the `cancel` to a worker:

```java
TimerWheel.Timeout budget = ... schedule(stage.timeout().toNanos(), () -> {
    boolean expired = attemptResult.completeExceptionally(new TimeoutException(...));
    if (expired) {
        CompletionStage<Object> toCancel = pendingCall;
        try {
            workersExecutorService.execute(() -> cancel(toCancel));
        } catch (RejectedExecutionException gone) {
            // Workers gone mid-shutdown: cancel inline; the wheel is dying anyway.
            cancel(toCancel);
        }
    }
});
```

The timeout still fires promptly and `recover()` still sees the
`TimeoutException` at the same instant — only the socket teardown moves off the
timer thread onto a virtual worker, where blocking is expected and isolated.
This preserves the Dekker handshake in `invokeAsync`: the `attemptResult` CAS is
still the single arbiter of who cancels, so the call is cancelled exactly once
regardless of which thread does it.

Apply the same offload to the **outside `cancel()`** path
(`Execution.cancel()`), which also calls `cancel(pendingCall)` — today on the
caller's thread. The caller's thread is less shared than the timer thread, but
a client-facing thread should not run connection teardown either; route it
through a worker with the same inline-on-shutdown fallback. The `boss.execute`
that posts the terminal already exists there and stays.

Optionally, tighten the `TimerWheel` javadoc into an enforced contract note:
actions passed to `schedule` may only complete futures and enqueue work, never
call user code or block — and point at this RFC as the reason.

## Testing

`DefaultNioFlowAsyncTimeoutIsolationTest`:

- A stage whose async call, when cancelled, blocks for a fixed interval
  (simulating slow teardown). Fire its timeout, and concurrently schedule a
  *second, unrelated* short timeout on the shared wheel. Assert the second
  timeout still fires within a tight bound — proving the teardown did not stall
  the wheel. Without the fix this test fails (the second timeout waits behind
  the teardown); with it, it passes.
- Assert the cancelled call is still cancelled exactly once (the existing
  `AsyncRunCancellationStressTest` oracle: every future settles, `cancelled > 0`).
- Assert `recover()` still receives a `TimeoutException` (not a wrapper, not a
  `CancellationException`) — the offload must not change what the recovery sees.

## Risks

- **An extra worker hop on the timeout path.** Timeouts are the cold path by
  definition; one virtual-thread dispatch to run the teardown is negligible and
  buys isolation. No hot-path benchmark is affected — the wheel action on the
  happy path is still just `budget.cancel()`.
- **Ordering of the cancel relative to `recover`.** `recover` runs when the
  attempt future completes (already async, on the boss); the cancel now runs on
  a worker in parallel. They are independent — the recovery does not depend on
  the teardown finishing — so parallelism is correct and, in fact, faster for
  the request.

## Results

Shipped as designed: one helper, `cancelOffThread`, with both cancel sites
routed through it — the timer-thread one (`attemptAsyncCall`) and the
caller-thread one (`Execution.cancel`). The Dekker handshake in `invokeAsync` is
untouched, so the attempt future's CAS is still the sole arbiter of who cancels
and the call is cancelled exactly once; the only change is *which thread* runs
the teardown.

Two things worth recording:

- **The snapshot matters, and the helper's signature enforces it.**
  `cancelOffThread(pendingCall)` reads the volatile field *at the call site*
  (the timer thread / the caller thread) and passes the value; the worker lambda
  captures that snapshot, not the field. So a retry that later republishes
  `pendingCall` can never redirect an in-flight cancel to the wrong call — the
  same property the inline version had for free by reading the field once.
- **The test turns "off the timer thread" into a deterministic assertion.** An
  `ObservingFuture` (a `CompletableFuture` whose `cancel` records — and can block
  on — the thread that runs it) makes the fix falsifiable three ways: the
  timeout cancel does not run on `nio-flow-timer`, the outside cancel does not
  run on the caller thread, and a *blocking* teardown on one execution no longer
  stalls a second execution's timeout on the shared wheel (without the fix the
  second future never completes and the test hangs). No `Mono` needed, so the
  test lives in core.

`cd core && ./gradlew test` green (full suite, plus the `tests/` cancellation
stress); `cd reactive && ./gradlew test` green; SonarLint diff over `core` is
empty (a first pass tripped one S125 on a comment that read as code — the comment
was reworded, not suppressed).
