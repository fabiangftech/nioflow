# RFC 0020 — Bug-hunting with deterministic unit tests in `core/` and `reactive/`

- **Status**: 📝 Proposed
- **Target**: `core/src/test/…` and `reactive/src/test/…` (JUnit only — no `tests/`, no JMH)
- **Depends on**: the whole implemented surface (RFC 0001–0017); it adds no feature, it probes them
- **Part of**: test hardening, not the throughput series — the guardrail that keeps the series honest
- **Realized by** (proposed): new `*…Test` methods added to the matching per-feature
  test classes (and a few new probe classes) under `core/` and `reactive/`; each
  found bug lands as a **red test first**, then the fix, then the test stays as a
  regression guard. No engine or API change is a *goal* — any that a probe forces
  is a bug being fixed.

## Summary

nio-flow tests along two axes today: **functional coverage** lives in `core/` and
`reactive/` as deterministic JUnit (one class per feature), and **bug-hunting
stress + JMH** lives in `tests/` (races, deep chains, hot splice, fork storms).
`CLAUDE.md` is explicit that the stress tests *complement, not replace* the
functional coverage — but nobody has run a **deliberate, adversarial pass** over
the functional side. The existing `core/` tests were written to *demonstrate each
feature works*, not to *break it*.

This RFC is that pass. It is a campaign to find bugs using **only deterministic
unit tests** in the two published modules: take each invariant the engine
documents, write the test that would **falsify** it, and run it. A test that
fails is a bug; a test that passes becomes a permanent guard. No timing, no
concurrency, no throughput — those are `tests/`' job and are *probabilistic*. A
unit test is *reproducible*: it names the exact input that breaks and fails the
same way on every machine and every CI run.

It ships tests (and the fixes those tests force), nothing else.

## Why unit-tests-only, and what that deliberately excludes

The value of constraining the hunt to deterministic unit tests is the same reason
the split exists:

- **Reproducible, not flaky.** A race in `tests/` reproduces "sometimes, under
  load, on some machines". A unit test that constructs the exact chain and input
  fails **every** run — so it can gate CI (`.github/workflows/build.yml` builds
  every module on every PR) without ever going amber.
- **It pinpoints.** A stress failure says "a future hung"; a unit test says
  "`match()` with two overlapping predicates routed to the second case". The
  second is a bug report with a line number.
- **It is cheap.** The whole `core/` + `reactive/` suite runs in seconds, so the
  guard is free to keep forever — unlike a JMH gate, which is minutes.

**Out of scope by construction** (and why): anything whose failure mode is a
**race** or a **timing window** — boss/worker interleavings, splice-vs-in-flight,
fork storms, keyed-lane head-of-line under contention, the `TimerWheel`'s
late-fire. Those are genuinely non-deterministic and belong in `tests/` where a
probabilistic harness with `orTimeout` can surface them. Trying to force them into
a unit test produces exactly the flaky guard this RFC exists to avoid. If a probe
here can only be made to fail "sometimes", it is misfiled and moves to `tests/`.

## The method: invariant → falsifier → red → fix → guard

Every probe follows one shape, so the campaign is a checklist, not a vibe:

1. **State the invariant** from the docs/code (the tables below enumerate them).
2. **Write the test that must fail if the invariant is false** — the adversarial
   input, not the happy path. Prefer the *boundary* (empty, size-1, the bitset
   edge), the *equivalence* (compiled vs interpreted must agree), and the
   *negative* (the guard that should reject).
3. **Run it.** Green → the invariant holds, keep the test as a guard and move on.
   Red → a bug; capture the minimal reproducer, fix the code, keep the test.
4. **The test stays.** Found or not, the falsifier is now a regression guard in
   the matching per-feature class (`DefaultNioFlowBranchingTest`,
   `ReactiveMonoSemanticsTest`, …), never a catch-all.

Two techniques carry most of the weight, because the engine's own design hands
them to us:

- **Differential testing — compiled vs interpreted.** The docs promise "compiled
  and interpreted must produce identical results" (`CompiledChain` is an
  optimization, never a semantic). Any chain can be run both ways (sealed →
  compiled plan; per-request local link → interpreted fallback). Asserting the
  two agree on result **and** recorded decisions, across a generated family of
  chains, is a bug-finder that needs no oracle beyond "they must match".
- **Fusion equivalence.** The same chain with a timeout on one stage (dispatches
  alone) vs without (fuses into a run) must produce identical results, including
  `Filter` cuts inside a run and positional `Recovery` inside a run. The fused
  and unfused paths are separate code; the result is contractually the same.

## Hunting grounds — `core/`

Each row is an invariant the engine documents and a deterministic probe that
would falsify it. **Bold** = the class the test belongs in.

| Invariant (from `CLAUDE.md` / code) | Falsifying probe |
| --- | --- |
| Compiled plan ≡ interpreted result **and** decision trace | Generate chains (stage/filter/decision/recovery mixes), run sealed vs per-request-local, assert equal. **`DefaultNioEngineCompiledChainTest`** |
| Fusion windows step over *guard-failed* links; a passing `Decision` ends the run; an unguarded non-fusable link is a hard boundary | Chain a `match()` case whose `Decision` is ruled out mid-window; assert the value skips it and the result equals the unfused chain. **`DefaultNioEngineFusionTest`** |
| Fused `Recovery` keeps positional semantics (scan forward in-run; escape to boss scan when none left) | Multi-`recover` chain, inject failure before/between/after each recovery, fused vs `timeout`-split; results identical. **`DefaultNioFlowRecoverTest`** |
| `match()` is first-match-wins; case k requires all previous `false`; a skipped `Decision` records nothing; absent id fails any guard | Overlapping predicates (two `true`), all-`false` → `otherwise`, exactly-one; assert the first match wins and later cases never evaluate. **`DefaultNioFlowBranchingTest`** |
| Decision bitset: 2 bits/id, sized to highest id; ids past limit → overflow map; unrecorded id fails guards | A `match()` wide enough to cross the bitset word boundary; per-request `when`/`match` growing the engine counter past the sealed limit. **`DefaultNioEngineDecisionBitsetTest`** |
| `Filter` → `execute()` null; `executeResult()` distinguishes `Completed(null)` vs `Filtered` vs `Cancelled` | A stage returning a genuine `null` vs a filter cut; assert `FlowResult` arms differ. **`DefaultNioFlowResultTest`** / **`DefaultNioFlowFilterTest`** |
| `Batch`: wrong-sized/failed bulk fails every member, recoverable per execution; each member gets its own element | Bulk returns the wrong arity, or throws; assert every batched caller fails and each `recover` sees its own value. **`DefaultNioFlowBatchTest`** |
| `Fork`: guard-closed sub-chain, ids compacted `0..n-1`, never keyed, never notifies `completeHandlers`, context copy isolates writes | A fork whose sub-chain writes context; assert parent context unchanged and no complete handler fired. **`DefaultNioFlowForkTest`** |
| `ChainValidator` rejects dangling guards, contradictory guards, duplicate anchor names, dead recoveries; a rejected splice leaves prior chain+plan untouched | Feed each malformed shape; assert `ChainValidationException` lists the problem **and** the live chain is unchanged after a rejected splice. **`DefaultNioEngineValidationTest`** |
| `Context` null until first put or handed map; name-based interop with `engine.call(input, map)`; contextual stage unwrap at single apply point | A pipeline reading a key seeded via the handed map vs `with()`; assert `with()` wins on a shared name and plain stages pay one `instanceof`, not a map alloc. **`DefaultNioFlowContextTest`** |
| Region edit: `use(name, segment)` span by link identity; `replaceRegion` swaps atomically; anonymous-name counter shared (no anchor/guard collision) | Replace a region containing a branch; assert routing still holds and no anchor name clashes with the recorded segment. **`DefaultNioFlowRegionTest`** |
| Positional `Recovery` catches `Stage` timeouts too; with no match, failure reaches `errorHandlers` and the future | Timeout upstream of a `recover`; assert it is caught; remove the recover, assert the future completes exceptionally. **`DefaultNioFlowTimeoutTest`** |
| `Retry` attempt count + backoff multiplier progression (the deterministic part — count, not wall-clock) | A stage failing N−1 times; assert exactly N invocations and `stageRetried` fired N−1 times. **`DefaultNioFlowRetryTest`** |
| `executeResult` sealed `FlowResult` is exhaustive over `Completed | Filtered | Cancelled` | A `switch` in-test over all three arms; a cancelled execution yields `Cancelled`, not a stage failure. **`DefaultNioFlowResultTest`** |

## Hunting grounds — `reactive/`

Reactive probes stay deterministic with `Mono.just` / `Mono.error` /
`Mono.never().timeout(...)` and `StepVerifier` — no real network, no sleeps.

| Invariant | Falsifying probe |
| --- | --- |
| `ReactiveMirror`: every core method has a covariant override on the mirror; every reactive-only step of `ReactiveStep` is also on `ReactiveLane` | The existing reflection test — extend its coverage to any method added since it last ran. **`ReactiveMirrorTest`** |
| `Blocking.await` unwraps Reactor's `ReactiveException`, so a `mono.timeout`/`Mono.error` reaches `recover()` as the raw exception | Assert `recover` sees `TimeoutException`/the original, never a Reactor wrapper. **`ReactiveBlockingHygieneTest`** / **`ReactiveMonoSemanticsTest`** |
| `defaultBudget` applies to every step declaring none — main line, `just()`, branch, fork; an explicit per-step budget wins; the budget *cancels* the subscription | A `Mono.never()` under a default budget inside a branch and inside a fork; assert `recover` sees `TimeoutException` and the subscription was cancelled. **`ReactiveDefaultBudgetTest`** |
| `pipe` fails the whole stream on one bad element; `pipeResilient` drops + reports once and continues; `concurrency`/`prefetch` validated at **build** time | One poison element mid-stream: `pipe` errors, `pipeResilient` emits the rest and the error handler saw it exactly once; an invalid concurrency throws at build, not first element. **`ReactivePipeTest`** / **`ReactivePipePrebuiltTest`** |
| `adaptFlux(call, maxItems)`: `take(maxItems+1)` cancels the source; overrun → `FlowOverflowException` recoverable; uncapped overload still present | A source of `maxItems+1`; assert the overflow is a stage failure `recover` catches and only one extra element was pulled. **`ReactiveStreamingTest`** |
| `executeFlux` = `executeMono().flatMapMany(tail)`: lazy, one-execution-per-subscription, empty-Flux filter cut, `onError` before the tail subscribes | Subscribe twice → two executions; a filtered execution → empty Flux; an upstream error → the tail never subscribes. **`ReactiveStreamingTest`** / **`ReactiveMonoSemanticsTest`** |
| `propagate(keys)` lifts **only** the named keys from Reactor context to `Context`, per subscription, no write-back | Seed two keys, propagate one; assert the other is absent in the stage and nothing is written back to the subscriber context. **`ReactiveContextTest`** |
| Cancellation travels the `CANCELLED` door; `applyRun` reads the flag **between fused links** (so `handleMono` + `handle("charge")` fused does not charge after cancel) | Cancel between the two fused stages; assert the second never runs. **`ReactiveCancellationTest`** |
| `handleMono` appends an ordinary `Stage` — it fuses, retries, rate-limits, lands in a lane, reports metrics | A `handleMono` inside a `when` lane with a `recover`; assert lane guards apply and a failure routes to the lane's recover. **`ReactiveFlowTest`** / **`ReactiveDelegationTest`** |

## What counts as a bug here

- **A real bug**: any probe that fails deterministically — wrong result, wrong
  routing, a swallowed error, a guard that should reject and does not (or vice
  versa), compiled ≠ interpreted, fused ≠ unfused.
- **Not a bug, still valuable**: a probe that passes closes an untested corner —
  it ships as a guard, and the campaign's output is measured in *both* fixes and
  new guards.
- **Misfiled**: a probe that only fails intermittently is a race — it does not get
  "fixed" with a sleep or a retry; it moves to `tests/` as a stress test.

## Definition of done

The campaign is a pass over the two tables, tracked as a checklist:

1. Every invariant row has at least one committed falsifier, red-then-fixed or
   green-as-guard.
2. `cd core && ./gradlew test` and `cd reactive && ./gradlew test` are green
   (the reactive run also re-checks the mirror contract — a core method added
   during a fix must gain its covariant override, per RFC 0008).
3. Each bug found is one commit: the red test, then the minimal fix, so `git log`
   reads as a bug ledger.
4. `tools/sonarlint/run.sh core` and `tools/sonarlint/run.sh reactive` introduce
   no new findings over the touched files (judge by the diff, per `CLAUDE.md`).

## Non-goals

- **No new engine feature or API.** Any behaviour change is a bug fix a probe
  forced, documented in that commit — not a design change smuggled in.
- **No JMH, no benchmark gate.** This is test-only; the hot-path numbers are the
  series RFCs' concern. (Same exemption as the docs RFC 0018 — not every RFC ships
  a benchmark; a *feature* does.)
- **No concurrency/stress/timing tests.** Those stay in `tests/`; this RFC is the
  deterministic complement, not a competitor.
- **No coverage-percentage target.** Chasing a line-coverage number rewards
  touching code, not breaking it. The metric is invariants-probed and bugs-found.

## Testing

The tests *are* the deliverable. The meta-guardrail: every probe is deterministic
(no `Thread.sleep`, no wall-clock assertion, no dependence on scheduling order),
so it fails the same way everywhere or it does not belong here. The differential
and fusion-equivalence probes are self-checking — they need no hand-computed
oracle, only "these two paths must agree", which is exactly where latent bugs
hide.

## Risks

- **A probe that is subtly non-deterministic becomes a flaky guard** — the very
  thing the module split avoids. Mitigation: the "misfiled → move to `tests/`"
  rule, applied the first time a probe goes amber, not the third.
- **Over-fitting to current behaviour.** A falsifier that asserts an incidental
  implementation detail (a particular fusion boundary, an allocation count) breaks
  on a legitimate refactor. Mitigation: assert the **contract** (compiled ≡
  interpreted, fused ≡ unfused, the documented semantic) — never the mechanism.
- **The hunt ends without finding much.** That is a *success signal*, not a failed
  RFC: the deliverable is the permanent guard set, and "the invariants hold under
  adversarial probing" is worth knowing and worth keeping green.
