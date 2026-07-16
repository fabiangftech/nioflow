# RFC 0040 — `laneHeld` visibility on the off-boss shutdown terminal

- **Status**: ✅ Implemented — option 1 (`volatile laneHeld`)
- **Target**: `core` (`DefaultNioEngine.Execution`)
- **Depends on**: RFC 0007 (cancellation), RFC 0024 (the atomic terminal), RFC 0026 (off-boss key-lane release)
- **Severity**: **Low** (narrow: dedicated engine + keyed + external cancel of the lane head + concurrent worker rejection during shutdown) — but the fix is nearly free
- **Realized by**: making `laneHeld` `volatile` (it is off the hot path — written only when a keyed execution takes its lane), and adding a shutdown-race test that cancels a keyed lane head mid-drain.

## The finding

`laneHeld` (`DefaultNioEngine.java:1049`) is a **non-volatile** boolean. It is written on the boss in `run()` / `releaseKey` (`laneHeld = true`) and read in `complete()` / `fail()` (`:1194`, `:1205`) to decide whether to call `releaseKey()`. In steady state everything runs on one boss thread, so there is no visibility question.

But `cancel()` (`:1225`) explicitly documents an **off-boss terminal**: when `boss.execute` is rejected during dedicated-engine shutdown, `complete(CANCELLED)` runs on the **caller's thread**. The RFC 0024 `finished` CAS orders the two racing terminals against each other (so bookkeeping runs once), but the CAS establishes a happens-before edge only for the writes that precede *it* — there is no edge carrying the boss's earlier `laneHeld = true` write to that caller thread. If the cancel terminal wins the CAS and reads a **stale `false`**, `releaseKey()` is skipped: the lane's successors are never handed the lane, they hang, and the drain never reports clean.

This is extremely narrow — it needs a dedicated engine, a keyed execution holding its lane, an external `cancel()` of that head, *and* the boss gone (shutdown) so the terminal runs off-boss — which is why the steady-state tests never reach it. But the consequence (a permanently stuck lane and a drain that lies about being clean) is exactly the class RFC 0024 and RFC 0026 exist to prevent, and the fix is a one-word change.

## Why it matters

The production-hardening cluster (RFC 0023–0030) was built on the principle that shutdown/cancel corners must be *mechanically* correct, not correct-in-practice. `laneHeld` is a data race by the letter of the Java Memory Model on a code path the code itself documents as off-boss. Even if it is unobservable on current hardware, an unsynchronized cross-thread read of a boss-written flag is precisely the kind of latent bug that surfaces on a weaker memory model or after an unrelated reordering — and leaving it undocumented invites someone to "optimize" the CAS placement and expose it.

## The options

1. **Make `laneHeld` `volatile` (recommended).** It is written only when a keyed execution takes its lane and read only at the terminal — well off the per-link hot path — so a volatile read/write costs nothing measurable. This gives the boss's `laneHeld = true` a happens-before edge to any thread that later reads it, closing the race for the off-boss terminal.

2. **Route the terminal's `releaseKey` decision through the boss-owned state only.** Restructure so the lane-release decision is never made off-boss — e.g. the off-boss cancel terminal always attempts `releaseKey` (which is already safe to call off-boss per RFC 0026, and is a no-op if the lane was never held or already released). This removes the reliance on reading `laneHeld` cross-thread at all, at the cost of a slightly less precise "did I hold it" check. Heavier than option 1 for the same outcome.

3. **Piggyback the flag on an existing synchronized edge.** Fold `laneHeld` into a field already published by the `finished` CAS (e.g. encode "held" into the terminal reason). More invasive and easy to get subtly wrong; option 1 achieves the same guarantee plainly.

Recommended: **option 1** — the minimal, obviously-correct fix for a flag this cold.

## Testing

- A `DefaultNioEngineKeyedShutdownTest` case: a dedicated engine, a keyed execution holding its lane parked on a never-answering async stage, `shutdown()` so the boss is gone, then an external `cancel()` of the head — assert the successor is released (or the lane retired) and `shutdown(grace)` reports clean, not a stuck lane. This falsifies the bug (an `orTimeout` on the joined futures turns the hang into a visible failure, RFC 0020 style).
- Regression: `DefaultNioEngineShutdownRaceTest` and the keyed suite stay green.
- RFC 0021 gates flat (a cold-field volatile is not on the hot path).

## Risks

- **The race may be genuinely unobservable today**, tempting a "won't fix." The cost of fixing is one keyword; the cost of a memory-model bug in shutdown is a hang no one can reproduce. Fix it and move on.
- **Do not over-correct** by making other cold `Execution` fields volatile reflexively — audit which are actually read off-boss (this one is, via the documented cancel path) and fix those, leaving boss-only fields plain.

## Results

Shipped option 1. `Execution.laneHeld` is now `volatile`. Off the hot path
(written once, when a keyed execution takes its lane; read at the terminal), so
the cost is nil — the full core suite and the RFC 0021 gates are unmoved.

- **The race is closed by the JMM, not by luck.** The boss writes `laneHeld = true`
  in `run()`/`releaseKey`; the off-boss cancel terminal (`complete(CANCELLED)` on
  the caller's thread, when `boss.execute` is rejected at a dedicated engine's
  shutdown) reads it to decide whether to `releaseKey()`. The `finished` CAS orders
  the two racing terminals but carries no happens-before for that earlier boss
  write, so a stale `false` was possible — and would skip `releaseKey()`, strand
  the lane's successors, and stop `shutdown(grace)` from ever reporting clean. The
  volatile read now sees the boss's write.

- **Tests:** a focused `aKeyedHeadCancelledOffBossAtShutdownReleasesItsSuccessor`
  (a keyed head parked on a never-completing async call, one successor queued, the
  dedicated engine shut down so the boss is gone, then the head cancelled off-boss)
  asserts the successor is released and the lane does not leak. The existing
  `aDeepKeyedBacklogDrainedOffBossAtShutdownDoesNotOverflowTheStack` (RFC 0026,
  20k backlog) already exercised the same off-boss path and asserts the whole
  backlog drains — the behavioural consequence a stale `laneHeld` would break.
  Both stay green (on a strong-memory-model host the stale read is unlikely to
  manifest, which is exactly why the fix is by-the-JMM, not by-observation).

- **Not needed: making other cold `Execution` fields volatile.** Only `laneHeld`
  is read on the documented off-boss path; the rest stay plain (boss-only).
