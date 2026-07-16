# RFC 0029 — Completion/error handlers on the boss: offload, bound, or say so plainly

- **Status**: ✅ Implemented — option 3 (accurate docs) + option 1 (BlockHound gate on handlers); option 2 deferred to the documented workaround
- **Target**: `CLAUDE.md` (the claim), the example's BlockHound suite (the gate) — no runtime change
- **Depends on**: RFC 0009 (the boss model), RFC 0023 (the handler-throw hardening this builds on)
- **Severity**: **Medium** — a documented-but-underspecified availability coupling; a slow handler stalls every execution on a shared boss
- **Sibling of**: RFC 0023 (handler *throws*), this (handler *blocks*)
- **Realized by**: the `CLAUDE.md` "boss never runs user code" claim rewritten to "never runs *stage* user code" and to name the handlers as boss-run-by-design; a new BlockHound test `aBlockingCallPlantedInACompletionHandlerTripsItToo` (example suite) that plants a `Thread.sleep` in an `onComplete` handler and asserts it trips, exactly as the existing `handleSync` one does. No engine change — the boss loop already runs handlers under `BossLoop.run` (disallowed by the BlockHound integration), so the guarantee was already mechanical; it just was not tested or claimed accurately.

## The finding

`CLAUDE.md`'s headline is "**the boss never runs user code** — Stage/Background/
Recovery functions go to the virtual-thread workers." Taken at face value that
is the property that makes the boss model safe: a boss orchestrates many
executions, so nothing user-supplied may run on it.

But `onComplete`/`onError` handlers **are** user code, and they run **on the
boss**. `reportExecution` executes the `completeHandlers` loop (and `notifyError`
the `errorHandlers` loop) inside `complete()`/`fail()`, which run on the boss. A
slow handler — a logging call that blocks on I/O, a metrics push, a handler that
does real work — stalls the boss, and therefore **every other execution pinned
to that boss**, for its whole duration. On the shared boss pool that is a
JVM-wide coupling: one flow's slow `onComplete` adds latency to unrelated flows
that merely hashed to the same boss.

A second, subtler instance: because bookkeeping (and the future's completion)
run on the boss, a caller's `executeAsync().thenApply(expensive)` runs
`expensive` on the boss too (the default `CompletableFuture` executor is the
completing thread). That is a standard `CompletableFuture` footgun, but it
interacts badly here — the completing thread is a shared boss, not a throwaway
pool thread.

`CLAUDE.md` does say "callbacks run on engine threads: keep them fast and never
throw." So this is **documented** — but the *mechanical* guarantees the rest of
the boss model enjoys (validation rejects `handleSync` + timeout/retry;
BlockHound marks bosses non-blocking so a blocking `handleSync` trips a test) do
**not** extend to handlers. The claim "the boss never runs user code" is
therefore false as stated, and the safety net that makes it *true* for stages is
absent for handlers.

## Why it blocks production

Handlers are where operators put the things that fire on every request: audit
logging, metric emission, trace-span closing, cache updates. Under the "keep
them fast" honor system, the first handler that does a synchronous log-append or
a blocking exporter flush turns a shared boss into a shared latency source — and
the symptom (p99 spikes on flows that share a boss with a noisy one) is
correlated across unrelated code, the hardest kind to diagnose. A property this
load-bearing should be enforced or scoped, not left to a doc line.

## The options

Pick one; they trade isolation against overhead and against the existing
`onComplete`-fires-before-`execute()`-returns contract.

1. **Bound it with BlockHound, keep it on the boss (cheapest, honest).** Mark the
   shared bosses `NonBlocking` (they already are in the example's `BlockHoundTest`
   for `handleSync`) and extend the same guard to the handler loops, so a
   *blocking* handler trips in test exactly as a blocking `handleSync` does. This
   does not offload anything — it makes "keep them fast" **mechanical** instead of
   aspirational, matching how the stage rule is enforced. Cheapest, no runtime
   change, and it turns the doc caveat into a falsifiable invariant. Recommended
   as the floor.

2. **Offload handlers to a worker (full isolation, contract change).** Run the
   `completeHandlers`/`errorHandlers` loops on the worker pool rather than the
   boss. This removes the coupling entirely, but it **breaks the ordering
   contract**: today a `just()`-scoped `onComplete` fires *before* `execute()`
   returns, and the shared-definition handlers run before the result future
   completes. Offloading makes them concurrent with the caller's continuation.
   Only worth it if the isolation matters more than that ordering — likely a
   per-handler opt-in (`onComplete(handler, ASYNC)`) rather than a blanket change.

3. **Scope the claim precisely in the docs (minimum, do regardless).** Rewrite
   the `CLAUDE.md` line to "the boss never runs **stage** user code; completion/
   error handlers and `handleSync` run on the boss by design — keep them
   non-blocking, and the BlockHound gate enforces it." An accurate claim is worth
   more than an aspirational one, and it is what a production operator needs to
   reason about their handlers.

Recommended combination: **option 3 always** (tell the truth), **option 1** to
make the truth enforceable, and **option 2 as an opt-in** for the operator who
genuinely needs a heavy handler and accepts the ordering change.

## Testing

- **BlockHound handler gate** (option 1): plant a `Thread.sleep` in an
  `onComplete` handler; assert it trips the boss's non-blocking guard, exactly as
  `BlockHoundTest` does for a boss-inlined `handleSync`. This is the deliverable
  that makes "keep them fast" real.
- If option 2 opt-in ships: assert an `ASYNC` handler runs off the boss (thread
  name assertion) and a default handler still fires before `execute()` returns
  (the ordering contract for the default path is preserved).
- Regression: RFC 0023's throwing-handler hardening still holds (a throwing
  handler is reported, never hangs a future) on whichever thread the handler runs.

## Risks

- **Option 1 surfaces existing "slow" handlers as test failures.** That is the
  point — a handler that blocks was already a latent production stall; failing in
  test is strictly better than spiking in prod. Real, intentionally-blocking
  handlers move to the option-2 opt-in.
- **Option 2's ordering change.** Called out above; why it is an opt-in and not a
  default. The `onComplete`-before-`execute()` guarantee is relied upon by tests
  and by callers, and must not change silently.
- **Doc-only (option 3 alone) fixes nothing mechanical.** It is the minimum, not
  the fix; ship it *with* option 1.

## Results

Shipped option 3 + option 1, and the implementation revealed that option 1 needed
**zero** engine code — only a test.

- **The guarantee was already mechanical; it was just untested and mis-claimed.**
  The example's `NioFlowBlockHoundIntegration` already marks boss threads
  (`nio-flow-boss-*`) non-blocking and `disallowBlockingCallsInside(BossLoop,
  "run")` — and the handler loops run inside `complete`/`fail`, which run as a
  boss task under `BossLoop.run`. So a blocking handler *already* tripped
  BlockHound; nobody had planted one to prove it. The new test does, and it
  passes (4 tests, 0 failures), so "keep handlers fast" is now a falsifiable
  invariant, matching the `handleSync` one beside it. The BlockingOperationError
  the handler raises is caught by the RFC 0023 guard and routed to the error
  handlers — which the test asserts.

- **The claim is now accurate.** `CLAUDE.md` said "the boss never runs user
  code", full stop; it now says "never runs *stage* user code" and lists what
  does run on the boss by design (Decision/Filter predicates, `handleSync`, and
  the `onComplete`/`onError` handlers), all held to the non-blocking rule the
  BlockHound gate enforces. The observability note gained the same correction and
  the pragmatic escape hatch.

- **Option 2 (offload as an opt-in) was deferred, on purpose.** A per-handler
  `ASYNC` mode would add API surface (and a reactive-mirror override, RFC 0030
  territory) and complicate the `onComplete`-before-`execute()` ordering
  contract, to serve a need the docs now cover in one line: *for a genuinely
  heavy handler, hand the work to your own executor* (`onComplete(v ->
  myPool.execute(() -> heavy(v)))`). That keeps the boss free without the engine
  owning a second execution context or a new contract. If a concrete case ever
  needs first-class support, option 2 is specified above and can land then.

No `core`/`reactive` source changed, so there is nothing new for those SonarLint
gates; the example test suite (BlockHound armed) is green.
