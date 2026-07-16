# RFC 0031 — Admission control must cover `call()`, not just `inject()`

- **Status**: 📋 Proposed
- **Target**: `core` (`DefaultNioEngine`), plus the docs that describe backpressure
- **Depends on**: RFC 0009 (the boss model), RFC 0024 (the atomic terminal the permit release must ride on)
- **Severity**: **High** — the primary request/response path (everything the reactive facade runs) has no admission control, whatever capacity was configured
- **Realized by**: gating `submit()` behind `admit()` and releasing the permit inside `finishBookkeeping` (so it frees on *every* terminal — value, filter, cancel, failure), OR — if `call()` is intended to stay unbounded — documenting that precisely and adding a separate concurrency limit for the request/response path.

## The finding

`admit()`/`inFlightPermits` are only reached from `inject()` (`DefaultNioEngine.java:244`) and released only by `await()` (`releasePermit`, `DefaultNioEngine.java:284`). The request/response path — `call()` (`:291`) → `submit()` — **never calls `admit()`**.

So `new DefaultNioEngine(capacity, OverflowPolicy.FAIL/DROP/BLOCK)` bounds *only* the fire-and-forget `inject`/`await` queue. This matters because the entire reactive facade — `executeMono`/`executeAsync` → `callCancellable`/`call` — runs on `call()`. A WebFlux endpoint under load has **no admission control at all**, regardless of the configured capacity. `activeExecutions` (the drain counter) is bumped for every submission but is never used as a bound.

`CLAUDE.md`'s backpressure story ("`new DefaultNioEngine(capacity, OverflowPolicy)` bounds it with BLOCK/DROP/FAIL") reads as a property of the engine; it is a property of `inject` only, and nothing in the docs or Javadoc says so. `DefaultNioEngineBackpressureTest` exercises only `inject`, which is why the gap is invisible.

## Why it blocks production

Backpressure is the one thing that keeps a service from converting a downstream slowdown into an OOM. An operator who reads "bound in-flight work with a capacity + OverflowPolicy," wires a WebFlux controller around `executeMono`, and expects that bound to hold — gets an unbounded engine. Under a downstream stall every arriving request starts an `Execution` (and, on the reactive path, parks a virtual worker retaining ~3.6 KB), with nothing to say "no." The symptom is a heap climb that the configured "capacity" was supposed to prevent, which is the worst kind of surprise: the safety was configured, acknowledged by the constructor, and silently not applied to the path that needed it.

## The options

1. **Gate `submit()` behind `admit()` too (recommended).** Acquire the permit at admission for `call()` exactly as `inject()` does, and release it inside `finishBookkeeping` (not in `await`, which the request/response path never calls). Releasing in `finishBookkeeping` is the right seam because it already runs on *every* terminal — value, `FILTERED`, `CANCELLED`, failure — so no path can leak a permit. BLOCK parks the *caller's* submitting thread (correct for `inject`, but for `call()` on a Netty event-loop thread that is itself a hazard — see risks), DROP fails the returned future with `RejectedExecutionException`, FAIL throws from `call()`. Admission stays *before* the execution runs, preserving the "rejecting an already-run value is not backpressure" invariant.

2. **A separate concurrency limit for `call()`.** Keep `inject`'s queue bound as-is and add a distinct `maxInFlightCalls` (semaphore released in `finishBookkeeping`) that only the request/response path consults. Cleaner if the two paths genuinely want different limits, at the cost of a second knob.

3. **Document the gap and stop there (minimum, do regardless).** State plainly in `CLAUDE.md`/`scaling.md` that capacity bounds only `inject/await`, and that `call()` (hence the whole reactive facade) is unbounded — so operators front it with their own limiter (WebFlux concurrency, a bulkhead, a `RateLimit` stage). Honest, but it leaves the constructor's promise misleading.

Recommended: **option 1**, with the OverflowPolicy semantics for `call()` spelled out (especially BLOCK — see risks), and **option 3's doc fix** shipped alongside so the behavior is never ambiguous again.

## Testing

- A `DefaultNioEngineBackpressureCallTest`: with `capacity=N` and `FAIL`, the (N+1)-th concurrent `call()` (held open by a latch in a stage) fails with `RejectedExecutionException`; with `DROP` its future completes exceptionally and `valueDropped()` fires; with the stages released, the permit frees and a later `call()` is admitted.
- A leak-shape test: run K > capacity calls that each complete (value / filter / cancel / failure) and assert the permit count returns to full — the `finishBookkeeping` release must hold on *all four* terminals.
- Regression against RFC 0021 gates: admission is one `tryAcquire` on the submit path (off the per-link hot path), so `engineCall` throughput/allocation stays flat.

## Risks

- **BLOCK on a `call()` from an event-loop thread parks that thread.** Parking a Netty/boss thread is exactly what the library forbids elsewhere. Either reject BLOCK for `call()` (document it as an `inject`-only policy), or make the `call()` bound non-blocking (FAIL/DROP only). Call this out explicitly in the API.
- **Changing `call()` to sometimes reject is a behavior change.** Today `call()` never rejects for capacity; callers that pass a capacity today get `inject`-only bounding and would start seeing rejections on `call()`. Since capacity is opt-in and its *documented purpose* is bounding, this is a fix, not a break — but note it in the changelog (RFC 0037).
- **Permit release must be exactly-once.** It rides the same terminal as the drain counter; reuse the RFC 0024 `finished` CAS so a double terminal cannot double-release.
