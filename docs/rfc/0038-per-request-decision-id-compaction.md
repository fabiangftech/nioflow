# RFC 0038 — Compact per-request decision ids so branching never falls off the bitset

- **Status**: ✅ Implemented — option 1 (compact per-request ids like a fork), guarded so it costs nothing below the bitset limit
- **Target**: `core` (`AbstractChain`, `ExecutionNioFlow`, `DefaultNioEngine`)
- **Depends on**: RFC 0011 (the plan / decision bitset)
- **Severity**: **Medium** — a long-uptime hazard and a hot-path allocation cliff for per-request branching; the fork path already shows the fix
- **Realized by**: compacting a per-request pipeline's decision ids to `0..n-1` in its cached plan (`ExecutionNioFlow.State.prepared` → `AbstractChain.compactDecisionsIfBeyond`), reusing the fork path's `compactDecisions`, but only when a decision id would fall off the bitset (`> MAX_BITSET_DECISION_ID`) so the common case pays exactly the `List.copyOf` it always did.

## The finding

`nextDecision()` is `decisionIds.getAndIncrement()` (`DefaultNioEngine.java:561`) — an `AtomicInteger` that is **never reset**. Every per-request `when`/`match` calls it through `AbstractChain.appendDecision` (`:292`). Two consequences:

1. **Counter exhaustion.** A service doing per-request branching at, say, 100k branches/s exhausts `Integer.MAX_VALUE` in roughly 6 hours, after which ids go negative. Correctness survives the wrap (a negative `maxDecisionId` makes `decisionBits` null → the `decisionsOverflow` HashMap path, and `passesGuards` treats out-of-range as `false`), so this is not a crash — but it is a real behavioral cliff.

2. **The allocation cliff at 511.** Past `MAX_BITSET_DECISION_ID` (511), the engine drops every per-request-branching execution off the fast `long[]` bitset onto a per-execution `HashMap<Integer, Boolean>` — allocation plus boxing on the hot path — and because the counter only ever climbs, once a busy service passes 511 it **never comes back**. Every subsequent per-request `match()` pays the overflow-map cost for the life of the JVM.

The fork path already recognizes and solves exactly this: `AbstractChain.compactDecisions` (`:201`, used at `:189`) remaps a `Fork`'s sub-chain decision ids to `0..n-1` because they are private to that child and the engine-wide counter would bloat its bitset. The **per-request pipeline path does not get the same treatment** — yet its decisions are equally private (a `just()` execution's local `when`/`match` are not shared with anything).

## Why it matters

Per-request branching is a first-class, advertised feature (`just(x).when(...).match(...)`). A service that leans on it hits the 511 cliff early and silently — throughput and allocation quietly degrade with no error, no log, and no obvious cause, because the chain still produces correct results. And the fix already exists in the codebase for the analogous fork case, so this is closing a known gap with a known technique, not inventing one.

## The options

1. **Compact per-request decision ids to `0..n-1` (recommended).** When a `just()` execution adds local `when`/`match` links, draw their decision ids from a per-execution (or per-local-chain) counter starting at 0, exactly as `compactDecisions` does for a fork. A per-request pipeline's decisions are private, so this is safe: the ids never need to be globally unique, they only need to be unique *within that execution's chain*. Result: per-request branching always fits the bitset (unless a single request declares > 511 branches, which is absurd), the overflow map is never touched for it, and the engine-wide counter is never consumed by per-request work.

2. **Reset/recycle the engine counter (rejected).** Resetting `decisionIds` is unsafe — the *shared* chain's decisions use the same counter and are long-lived, so recycling risks id collisions between a shared decision and a per-request one. Compaction (option 1) sidesteps this precisely because it makes per-request ids local, not global.

3. **Do nothing but document the cliff (minimum).** Note in `scaling.md` that heavy per-request branching degrades past 511 distinct decisions and to prefer shared-definition branching. Honest, but it declines to fix a hot-path regression the codebase already knows how to fix.

Recommended: **option 1.** It removes both the overflow-map cost and the counter-exhaustion concern in one move, reuses proven machinery, and leaves the shared-definition path (whose decisions *are* long-lived and correctly counted) untouched.

## Testing

- A test that a per-request pipeline with many `match()` cases uses ids `0..n-1` (assert `maxDecisionId` stays small and `decisionBits` is non-null / the overflow map is never allocated), across repeated executions — the counter must not climb.
- A correctness test that compacted per-request branching routes identically to the pre-change behavior (first-match-wins, otherwise, nested branches) — compaction must be transparent.
- **The currently-missing overflow test:** a per-request chain that *does* exceed 511 (constructed deliberately) still produces correct results via the overflow map — the fallback stays correct even though option 1 makes it unreachable for normal branching.
- RFC 0021 gates: per-request-branching allocation drops (no more overflow HashMap) or stays flat; it must not regress.

## Risks

- **Interaction with runtime splice/region edits** on a per-request chain: compacted local ids must stay consistent if the local chain is edited. Per-request pipelines are ephemeral (not sealed/spliced the way the shared definition is), so this is likely a non-issue — but test a per-request `when` after a local `use(segment)` to be sure.
- **Two id spaces (shared global vs per-request local)** must never be read against the same bitset. The fork path already runs this way safely (its ids are compacted and its bitset is private); mirror that isolation exactly for per-request chains.
- **Very deep single-request branching** (> 511 in one request) still needs the overflow map as a correctness fallback — keep it, do not delete it; option 1 makes it rare, not impossible.

## Results

Shipped option 1, guarded. No hot-path change (compaction is build-time, once per
per-request pipeline, and only when needed); `BranchRoutingBenchmark` (shared-
definition branching, whose `just()` adds no local links and so never reaches the
compaction seam) stayed flat at ~92 ops/ms.

- **The compaction is conditional.** `ExecutionNioFlow.State.prepared` now compiles
  `AbstractChain.compactDecisionsIfBeyond(links, MAX_BITSET_DECISION_ID)` instead
  of `List.copyOf(links)`. Below the limit — the common case, and the whole warm-up
  of any service — it *is* the old `List.copyOf` (a single id scan, no rebuild), so
  there is no allocation regression for per-request branching. Once the engine-wide
  counter has climbed past 511, compaction kicks in, renumbers to `0..n-1`, and the
  execution rides the bitset instead of allocating a per-execution overflow
  `HashMap<Integer,Boolean>` (boxing every decision) for the rest of the JVM's life.

- **It reuses the fork machinery.** `compactDecisions` (made package-private) and
  its guard/link remapping are exactly what a fork already used; a per-request
  chain's decisions are equally private to one execution, so the same remap is
  safe. `MAX_BITSET_DECISION_ID` was promoted from `Execution` to a
  `DefaultNioEngine` constant so the plan and the bitset agree on the one limit.

- **Counter exhaustion is defused too.** Even if the engine-wide `AtomicInteger`
  wraps to negative after `Integer.MAX_VALUE` per-request decisions, compaction
  maps whatever ids exist (negative included) to `0..n-1`, so the executed chain
  never depends on the raw id's magnitude. The counter still climbs — it only has
  to hand out ids unique *within one chain being built* — but nothing reads it as a
  bitset index for a per-request execution anymore.

- **Tests:** `DefaultNioFlowBranchingCompactionTest` — `compactDecisions` renumber
  + guard remap; `compactDecisionsIfBeyond` skips below the limit and compacts
  above it; a per-request `when()`/`match()` built on a counter pushed past 511
  compiles to a plan whose `maxDecisionId` is `0`/`1` (proven white-box via two
  package-private test seams, `DefaultNioEngine.planMaxDecisionId` and
  `ExecutionNioFlow.preparedForTest`, because the bitset and the overflow map route
  identically and hide the difference from the public API); and routing stays
  correct through the compacted plan. The existing
  `DefaultNioEngineDecisionBitsetTest` (overflow-map correctness, kept as the
  fallback) and the fork suite stay green. SonarLint over the diff is clean.

- **Not done:** approach (b) — drawing per-request ids from a local counter so the
  engine-wide one never grows — was not taken. It is more invasive (every
  `appendDecision` on a per-request chain would need a local counter threaded
  through building) for no additional correctness: compacting the finished chain,
  the way forks do, already fits the bitset and defuses exhaustion.
