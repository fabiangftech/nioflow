# RFC 0021 — Regression-hunting with JMH gates in `tests/`

- **Status**: 📝 Proposed
- **Target**: `tests/` (the JMH benchmarks and a new gate harness) — no `core/`/`reactive/` change
- **Depends on**: the throughput series (RFC 0009, 0011–0017) — the measured facts this locks in
- **Sibling of**: RFC 0020 (deterministic unit-test bug-hunt) — same shape, other axis: 0020 pins *correctness*, this pins *performance*
- **Realized by** (proposed): relative **ratio gates** and **allocation gates** over the
  existing `tests/src/main/java/dev/nioflow/benchmark/*` benchmarks, checked by a
  small harness that runs JMH with `-rf json` and asserts the gates; a fast CI
  canary plus the full manual run. No new engine feature.

## Summary

Every throughput RFC (0009, 0011–0017) rests on a **measured number** — "fusion
made the links free" (1-stage and 32-stage cost the same), "async-stage fusion
lands within ~3% of blocking" (74.4 vs 72.4 ops/ms), "boss-inlining is 248 B/op
against 727" — and each RFC ran the benchmark **once**, cited the number, and
moved on. Nothing re-runs them. A refactor that quietly re-introduces a thread
hop, an allocation on the per-link path, or a stream in `passesGuards` would pass
every unit test in `core/` and `reactive/` — because the result is still
*correct*, just slower or fatter — and ship.

This RFC is the performance counterpart to RFC 0020. Where 0020 turns each
documented **correctness** invariant into a deterministic unit test that would
falsify it, this turns each documented **performance** invariant ("links are
free", "compiled ≡ interpreted throughput", "the boss is not a JVM-wide ceiling")
into a JMH **gate** that fails when the invariant regresses. It adds no engine
behaviour; it adds the harness that keeps the throughput series honest after the
fact, not just at the moment each RFC landed.

## What JMH catches that a unit test cannot

A unit test asserts *what* the engine computes; a benchmark asserts *how much it
costs to*. The regressions this RFC hunts are invisible to `core/`/`reactive/`:

- **A lost thread hop / lost fusion.** Two consecutive `handle`s must fuse into
  one boss→worker→boss round trip. If a change makes them dispatch separately the
  result is identical and every unit test stays green — throughput just halves.
- **An allocation on the hot path.** The context map must stay null until first
  put; `passesGuards` must be a plain loop, not a stream; `call()` must return the
  raw future with no dependent `whenComplete`. Each of these is a *number of bytes
  per op*, not a behaviour — only `-prof gc` sees it.
- **A scaling regression.** The engine's founding fact is that a 32-stage chain
  costs about what a 1-stage chain costs (fusion). A change that makes cost grow
  per link is correct and catastrophic; only the `@Param({"1","8","32"})` sweep
  shows it.
- **The boss becoming a ceiling.** Concurrent executions must spread across the
  boss pool. A regression that serializes them JVM-wide keeps every result correct
  and collapses contended throughput; only the `@Threads(8)` benchmark shows it.

## The method: relative gates, because they survive the machine

The trap with benchmark gates is absolute numbers: "≥ 60 ops/ms" passes on the
author's laptop and fails on a slow CI runner, so the gate gets muted and stops
guarding. This RFC's two gate kinds are **machine-independent** — the same reason
RFC 0020's differential probes need no hand-computed oracle:

1. **Ratio gates.** Assert a *relationship between two benchmarks measured in the
   same run*, never an absolute. Fusion's speedup, compiled-vs-interpreted parity,
   async-within-X%-of-blocking, contended-scales-with-cores — all ratios. A slow
   runner slows both sides equally, so the ratio holds; only a real regression
   moves it. This is the load-bearing idea, and it is exactly the
   "two paths must agree (or keep their ratio)" oracle 0020 uses for correctness.
2. **Allocation gates.** `-prof gc` reports `gc.alloc.rate.norm` — **bytes per op**,
   which is deterministic to within a handful of bytes across machines (it counts
   allocations, not time). So `≤ 800 B/op on the plain sealed chain` is a portable,
   low-variance gate where a throughput floor would be flaky. Allocation is where
   most hot-path regressions actually show up first.

Each gate is stated as an invariant, its benchmark, and the falsifying
threshold — a checklist, not a vibe, per 0020's shape.

## Gate inventory

**Bold** = the existing benchmark class the gate reads. Thresholds carry margin
(they catch a *regression*, not normal noise); the harness runs enough iterations
that the margin is comfortable.

### Ratio gates (machine-independent)

| Performance invariant (from the series / `CLAUDE.md`) | Gate |
| --- | --- |
| Fusion makes links free: a 32-stage chain costs ≈ a 1-stage chain | `engineCall[32] ≥ 0.5 × engineCall[1]` throughput (cost grew < 2×, not < 32×). **`NioFlowBenchmark`** |
| Stage fusion is a real speedup (~5× on 8 stages) | fused 8-stage ≥ 2× the same chain with per-stage timeouts (dispatch-alone). **`FusionBenchmark`** |
| The compiled plan is an optimization with parity, never a slowdown | `compiled ≥ 0.9 × interpreted` throughput (compiled never loses). **`CompiledChainBenchmark`** |
| Async-stage fusion lands within ~3% of blocking (RFC 0013's gate) | `fourAsyncReactiveStages ≥ 0.95 × fourReactiveStages`. **`ReactiveBenchmark`** |
| The boss is not a JVM-wide ceiling: contended spreads across the pool | `engineCallContended ≥ 3 × engineCall[1]` on an 8-core box (scales, not serializes). **`NioFlowBenchmark`** |
| Branch routing stays off streams (the stream `passesGuards` cost ~20%) | routed throughput within its band vs a plain chain. **`BranchRoutingBenchmark`** |

### Allocation gates (`-prof gc`, bytes/op)

| Performance invariant | Gate |
| --- | --- |
| Plain sealed chain ≈ 727 B/op | `≤ 850 B/op`. **`NioFlowBenchmark.engineCall`** |
| Boss-inlined chain ≈ 248 B/op | `≤ 320 B/op`. **`SyncStageBenchmark`** |
| Context map stays null until first put (~13% less garbage) | a no-context chain allocates `≤` a fixed floor; a contextual one only pays on first put. **`ContextBenchmark`** |
| Fan-out is lock-free / no `CompletableFuture` tree bloat (RFC 0012) | fan-out B/op `≤` its band. **`FanOutBenchmark`** |
| Reactive async in-flight ≈ 489 B vs 3 173 B parked (RFC 0015) | the async path allocates `<` the parking path. **`ReactiveBenchmark`** |

## CI strategy: a canary that fits, a full run that does not

Full JMH is minutes; unit tests are seconds. So the split mirrors the module
split:

- **CI canary (every PR).** A fast pass — `-f 1 -wi 1 -i 2` on the ratio gates plus
  `-prof gc` on the allocation gates — run by the harness, which parses the JSON
  and fails the build if any gate trips. Allocation gates carry the canary because
  they are low-variance even at one iteration; ratio gates use wide margins so a
  short, noisy run does not false-positive. The canary catches a *2× regression*,
  not a 5% drift — which is the point (a silent halving is the failure mode).
- **Full run (nightly / pre-release / on-demand).** `./gradlew jmh` as today, all
  iterations, tighter margins, the numbers that a release RFC cites. This is where
  a real ~5% regression is caught and where new absolute numbers are recorded.

The harness is a plain `main` (or a JUnit test in `tests/src/test`) that shells
JMH with `-rf json -rff build/jmh.json`, reads the result, and asserts the gate
table. No new dependency: JMH already emits JSON.

## Non-goals

- **No engine or API change.** This is `tests/` only — benchmarks and a gate
  harness. The hot path is not touched.
- **No new micro-optimization.** The RFC locks in the numbers the series already
  won; it does not chase new ones. A gate that *could* be tighter is not a bug.
- **No absolute-throughput gate in CI.** Absolute ops/ms is machine-bound and
  flaky; CI gates are ratios and allocations. Absolute numbers live in the full
  run and in release RFCs.
- **No correctness testing.** That is RFC 0020's axis; a benchmark that returns a
  wrong-but-fast answer is out of scope here (and would be caught there).
- **No replacement of the stress tests.** `tests/src/test` stays the home of race
  and hang hunting; this RFC is about cost, not correctness under contention.

## Testing

The gates *are* the deliverable, and the meta-guardrail is that each is
**relative or allocation-based**, never an absolute time — so it fails on a real
regression and not on a slow afternoon. A gate that goes amber under normal noise
is miscalibrated (widen its margin or raise its iteration count) exactly as a
flaky unit probe in 0020 is misfiled: fixed the first time it flickers, not the
third. The ratio gates are self-checking — they compare two benchmarks in the
*same* JVM run, so machine speed cancels, which is what makes them portable enough
to keep forever.

## Risks

- **Benchmark noise trips a gate falsely.** The mitigation is the two gate kinds:
  allocation is near-deterministic, and ratios cancel machine speed. Absolute
  throughput — the flaky kind — is deliberately kept out of CI.
- **A gate ossifies a number that should move.** A legitimate optimization that
  changes a ratio (e.g. a new fusion strategy) has to update the gate in the same
  commit — the gate is a claim about the design, and changing the design updates
  the claim. This is a feature: the diff shows the performance contract changing.
- **The canary misses a small regression.** By design — it catches halvings, not
  5% drift. The full run is where 5% is caught; the canary only promises no
  *catastrophic* regression slips a PR. The RFC states this out loud so nobody
  reads a green canary as "performance unchanged".
- **`gc.alloc.rate.norm` varies with JVM/GC flags.** The harness pins the run
  configuration (same JDK, same GC) so the allocation gate compares like with
  like; a JDK bump re-baselines the allocation numbers in that same commit.
