# RFC 0032 — Break up the `DefaultNioEngine` god class; unify the three execution drivers

- **Status**: ✅ Resolved — Phases A + B1 shipped (`Execution` now a top-level class); C reconsidered and closed by a test, not a unification (see below); B2 (flatten the nested drivers) left as low-value
- **Target**: `core` (`DefaultNioEngine` and the types it nests) — a structural refactor, no behavior change
- **Depends on**: RFC 0009, RFC 0011 (the plan), RFC 0013 (async fusion) — the machinery being reorganized
- **Severity**: **Medium** — maintainability/correctness-risk, not a live defect: the duplication is where the *next* bug will hide
- **Realized by**: extracting `CompiledChain`, the value records, `SharedExecutors` (A) and `Execution` (B1) to top-level classes (as the code style already demands for new code). The proposed driver unification (C) was reconsidered and NOT done — the three drivers are genuinely distinct and a shared helper would harm the hot path; the invariant it aimed to protect (RFC 0007's cancellation rule in every driver) is instead enforced by a test in the one driver where it was untested.

## Progress

**Phase A — done** (commit "extract self-contained value types from DefaultNioEngine"). The engine-agnostic nested types are now their own top-level files: `CompiledChain` (with `maxDecisionId` moved in beside its `compile`/fusion logic), `ChainVersion`, `Region`, `Prepared`, `RejectedCall`, and the `SharedExecutors` holder (`createBossPool`/`NIO_FLOW_BOSS` made package-private for it). Pure extraction, full core suite green, SonarLint over the diff clean. `DefaultNioEngine` drops 2647 → 2423 lines and loses six nested types.

**Phase B1 — done** (commit "extract Execution to a top-level class"). `Execution` (~1470 lines, with `FanOutJoin`/`AsyncSettle`/`AsyncRun`/`ExecutionContext` still nested inside it) is now its own top-level file `Execution.java`, holding a `DefaultNioEngine engine` back-reference. It reached the engine almost entirely through IMPLICIT references — exactly ONE explicit `DefaultNioEngine.this` in the whole class — so the extraction qualified ~100+ bare references (fields, the `CANCELLED`/`FILTERED`/`HANDED_OFF`/`MAX_BITSET_DECISION_ID` sentinels, methods `notifyError`/`releasePermit`/`signalKeyLaneVacancy`/`unwrap`/…) with `engine.`/`DefaultNioEngine.` and made ~30 engine members package-private. The qualification was **compiler-driven, not a blind regex**: the compiler listed every unresolved symbol; the only name shared with an `Execution` field (`metrics`) was already disambiguated by that single explicit `DefaultNioEngine.this`, and a `BatchGroup` factory (`engine.batchGroupFor`) replaced the `BatchGroup::new` the top-level class could no longer write. The `Execution` constructor stayed at 7 parameters (the added `engine` traded against moving `context` to a set-once field). Behavior-preserving: the full core + reactive suites pass, and SonarLint over the diff shows **zero new findings** — the S1181/S3077/S3776/S135/S127/S125/S6916 that appear in `Execution.java` are exactly the documented-deliberate ones that RELOCATED from `DefaultNioEngine`. `DefaultNioEngine` drops **2464 → 1042 lines**.

**Phase B2 — deferred (low value): flatten the nested drivers.** `FanOutJoin`/`AsyncSettle`/`AsyncRun`/`ExecutionContext` are still nested inside the top-level `Execution`. They are already OUT of the god class; lifting them to their own files would expose a lot of `Execution` private state (they read its fields heavily) for marginal gain. Do it only if the "no nested types" rule is enforced strictly.

**Phase C — reconsidered, and closed by a test rather than a unification.** Studying the three drivers with B1 done showed the premise was optimistic: they are genuinely distinct, not duplicated.

- `advance` is the boss loop (per-link, dispatches); `applyRun` is the fused blocking-worker driver (inline recovery scan, returns a sentinel); `AsyncRun.drive` is the fused async-worker driver (callback-based, hops to the boss).
- The "shared cancellation gate" is a one-line `cancelled` field read with THREE context-specific responses — `complete(CANCELLED)` on the boss, return the `CANCELLED` sentinel on a blocking worker, `resumeOnBoss(() -> complete(CANCELLED))` on an async worker. There is no common *response* to unify; the check itself is a field read.
- The "recover scan" runs on different structures in different contexts: `recover()` scans the CHAIN and dispatches applyRecovery to a worker; `applyRun` scans the RUN array and applies inline — and that inline-vs-dispatch difference IS the fusion, not incidental duplication.
- The per-stage timing is sync-inline (`timedApply`) vs async-callback (`inlineStage`/`onPending`) — structurally different.

Forcing a shared helper would add indirection to the per-link hot path — exactly what the JMH benchmarks and the S3776 entry in `tools/sonarlint/README.md` deliberately protect ("splitting any of them to please the rule is what the JMH benchmarks guard against"). The DRY win is illusory and the concurrency risk is real.

What C actually *cares* about — RFC 0007's cancellation rule holding in every driver — is a testable invariant, not a structural one. The fused-blocking case was already pinned (`theStageAfterTheCancelledOneNeverRuns`), but the fused-ASYNC driver (`AsyncRun.drive`, cancellation between two fused async stages) was untested — the real "fixed in two of three" gap. That gap is now closed by `aCancelledFusedAsyncRunStopsBeforeTheNextStage` (a per-request pipeline of two consecutive `handleMonoAsync` stages, the first left pending, cancelled while the fused run is parked between them; the second stage must never run). So the invariant is mechanical across all three drivers — enforced where it belongs, without contorting the hot path.

**Phase B2 — left undone (low value):** `FanOutJoin`/`AsyncSettle`/`AsyncRun`/`ExecutionContext` remain nested inside the top-level `Execution`. They are already out of the god class; lifting them would expose a lot of `Execution` private state for marginal gain.

## The finding

`DefaultNioEngine` is **2414 lines with 13 nested types** (`SharedExecutors`, `ChainVersion`, `KeyLane`, `Region`, `Prepared`, `ExecutionHandle`, `RejectedCall`, `BatchGroup`, `Execution`, and nested inside `Execution`: `FanOutJoin`, `AsyncSettle`, `AsyncRun`, `ExecutionContext`). `Execution` alone spans ~1266 lines (`:1034`–`:2300`).

`CLAUDE.md`'s "No nested types" rule explicitly grandfathers these. But grandfathering a rule *this* hard is itself the smell: the rule exists because these types are painful to work with, and this one file is where all of them live.

The concrete correctness angle: `Execution` contains **three distinct execution drivers** with subtly different invariants —

- the boss `advance`/`step` loop (iterative, on the boss),
- `applyRun` (the fused blocking-worker driver),
- `AsyncRun.drive` (the fused async-worker driver).

Each **independently re-implements** the cancellation check (RFC 0007's `cancelled` read must appear in `advance` *and* `applyRun` *and* the async run), the positional recover scan, and the per-stage metric timing. That triplication is enforced only by prose and review — `CLAUDE.md` itself notes the async-run driver "holds the same invariants the boss loop does." The RFC 0007 cancellation bug (cancellation had to be added to `applyRun`, not just `advance`, or a fused `handleMono` + `handle("charge")` charged the card anyway) is exactly the failure mode this shape invites: a rule that must hold in three places, written in three places.

## Why it matters

Every future change to the execution path — a new link type, a fusion tweak, a cancellation-point addition — has to be made in three drivers and kept consistent by hand. The `CompiledChain` record even hand-writes `equals`/`hashCode`/`toString` because arrays break record value semantics (a sign the abstraction is fighting the language while buried in a 2400-line file). A refactor here does not add a feature; it removes a standing tax on every feature that follows, and closes the class of "fixed in two of three drivers" bug that RFC 0007 already demonstrated once.

## The options

1. **Extract the data/value types first (low-risk, do first).** `CompiledChain`, `ChainVersion`, `KeyLane`, `Region`, `Prepared`, `ExecutionHandle`, `RejectedCall`, `BatchGroup` are mostly self-contained. Move each to its own top-level package-private file. Pure mechanical extraction, the test suite catches any capture mistake, and it shrinks `DefaultNioEngine` substantially before the harder move.

2. **Extract `Execution` (and its inner drivers) to a top-level class (the main move).** `Execution` needs access to engine state (the version, the drain counter, the metrics, the boss array). Extract it to a top-level `Execution` that holds a reference back to the engine (or a small `EngineContext` seam of exactly what it needs), and lift `FanOutJoin`/`AsyncRun`/`ExecutionContext` to top-level too. Larger, but the boundary is clean: `Execution` already only touches engine state through a handful of fields.

3. **Unify the three drivers behind one helper (the correctness payoff).** Pull the shared per-step logic — read-cancelled-flag, scan-forward-for-recovery, time-the-stage — into one place that all three drivers call, so the RFC 0007-style "add the check in every driver" hazard becomes "add it once." This is the change that turns a prose invariant into a structural one.

Recommended sequencing: **1 → 2 → 3.** Ship option 1 on its own (it is safe and immediately halves the file); take 2 and 3 together since 3 is far easier once `Execution` is its own type.

## Testing

- No new behavior, so the whole existing suite is the test — in particular `DefaultNioEngineCompiledChainTest` (compiled ≡ interpreted), `DefaultNioFlowFusionEquivalenceProbeTest` (fused ≡ single-dispatch, blocking), `DefaultNioFlowCancellationTest`, and the shutdown/drain suite must stay green with zero edits.
- **Add the missing async-fusion equivalence probe** (currently a gap): assert a chain that produces a fused *async* run yields identical results and ordering to the single-dispatch async path, mirroring the blocking `FusionEquivalenceProbe`. If option 3 unifies the drivers, this probe protects the unification.
- RFC 0021 gates flat throughout (a refactor must not move an allocation onto the hot path).

## Risks

- **Accidental semantic change during extraction.** The inner classes capture `this` (the engine) implicitly; making that reference explicit can subtly change which field is read when. Mitigation: extract in small commits, each green against the full suite; do not combine extraction with any logic edit.
- **`Execution`'s back-reference.** A naive `Execution(engine)` re-exposes everything; prefer a narrow interface (`EngineContext`) of exactly the members `Execution` uses, which also documents the coupling.
- **Scope creep.** This is a refactor, not a redesign — resist "improving" the drivers' behavior while moving them. Behavior changes belong in their own RFCs (0031, 0038–0041).
