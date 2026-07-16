# RFC 0021 — Regression-hunting with JMH gates in `tests/`

- **Status**: ✅ Implemented — harness + 8 gates (7 on the canary, contention full-only), wired into CI (`build.yml`)
- **Target**: `tests/` (the JMH benchmarks and a new gate harness) — no `core/`/`reactive/` change
- **Depends on**: the throughput series (RFC 0009, 0011–0017) — the measured facts this locks in
- **Sibling of**: RFC 0020 (deterministic unit-test bug-hunt) — same shape, other axis: 0020 pins *correctness*, this pins *performance*
- **Realized by**: `tests/src/main/java/dev/nioflow/gates/JmhGates.java` — a harness
  that runs the existing benchmarks through the JMH **Runner API** (which returns
  `RunResult` with the throughput score *and* the GC profiler's `gc.alloc.rate.norm`
  bytes/op, so no JSON parsing) and asserts 5 **ratio gates** + 3 **allocation
  gates**; the `jmhGates` Gradle task (`./gradlew jmhGates`, or `-PgatesMode=full`).
  See **Results** below for what implementing changed from this draft.

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

Thresholds are `canary / full` — the canary is wide (it catches a regression,
not machine-load noise); the full pass is tighter.

| Performance invariant (from the series / `CLAUDE.md`) | Gate (implemented) |
| --- | --- |
| Fusion makes links free: a 32-stage chain costs ≈ a 1-stage chain | `engineCall[32] ≥ 0.5 × engineCall[1]` (cost grew < 2×, not < 32×). **`NioFlowBenchmark`** |
| Boss-inlining is a real speedup (~2.2×) | `syncSingle ≥ 1.15 / 1.6 × workerSingle`. **`SyncStageBenchmark`** |
| The compiled plan keeps parity, never a slowdown | `plain8Compiled ≥ 0.6 / 0.85 × plain8Interpreted`. **`CompiledChainBenchmark`** |
| Async-stage fusion lands within band of blocking (RFC 0013) | `fourAsyncReactiveStages ≥ 0.75 / 0.9 × fourReactiveStages`. **`ReactiveBenchmark`** |
| The boss is not a JVM-wide ceiling: contended spreads across the pool | `engineCallContended[32] ≥ 1.5 × engineCall[32]` — **full pass only** (a multi-core claim; skipped on the 2-core CI canary). **`NioFlowBenchmark`** |

### Allocation gates (GC profiler, bytes/op — near-deterministic)

| Performance invariant | Gate (implemented) |
| --- | --- |
| Plain sealed chain (measured ~617 B/op) | `engineCall[1] ≤ 1000 / 850 B/op`. **`NioFlowBenchmark`** |
| Boss-inlined chain (measured 248 B/op) | `syncSingle ≤ 420 / 320 B/op`. **`SyncStageBenchmark`** |
| No per-link allocation: fusion keeps allocation flat over links | `engineCall[32] ≤ 1.3 × engineCall[1]` B/op (measured ratio ≈ 1.0). **`NioFlowBenchmark`** |

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

The harness (`JmhGates`) is a plain `main` that drives JMH through its **Runner
API** — `new Runner(options).run()` returns `Collection<RunResult>`, each with the
primary throughput score and (with `GCProfiler` added) the `gc.alloc.rate.norm`
secondary result in bytes/op. That is strictly better than shelling out and
parsing `-rf json`: no file, no JSON, the numbers arrive as doubles. No new
dependency — `jmh-core` is already on the `tests/` classpath.

## Results

The harness ships with **8 gates (5 ratio, 3 allocation)** over four existing
benchmarks, verified green on the canary. Measured baselines on the dev machine:
`engineCall[1]` 617 B/op, `syncSingle` 248 B/op (matching `CLAUDE.md`'s numbers),
boss-inlining ~1.9–2.2×, compiled parity ~1.0–1.2×, async-vs-blocking ~1.0×,
allocation flat across links (616 vs 617 B/op).

**Implementing the draft changed it in two ways worth recording** — the same "the
implementation is the review" effect RFC 0020 had:

- **The draft cited a `FusionBenchmark` that does not exist.** Fusion's speedup is
  actually measured by `NioFlowBenchmark`'s stage sweep (1/8/32) and by
  `SyncStageBenchmark`'s `syncSingle` vs `workerSingle`. The gate now reads the
  real benchmark (boss-inlining, ~2.2×) instead of a phantom one.
- **The draft's "async allocates less than parking" gate confused two metrics.**
  RFC 0015's *489 B vs 3 173 B* is **retained heap per in-flight request** (measured
  by the `ReactiveHeapProbeTest` unit test), not *allocation rate per op*. By
  bytes/op the async path allocates **more** (four dispatch round trips vs one
  fused run), so the gate was measuring the wrong thing and was dropped. The
  retained-heap invariant stays guarded where it belongs — in the heap probe, an
  RFC 0020-style unit test — and this RFC added a correct allocation gate instead:
  **fusion keeps allocation flat over links** (`engineCall[32]` ≈ `engineCall[1]`
  B/op), which directly guards "no allocation on the per-link path".

**Calibration is the real work of a gate.** Throughput ratios compress toward 1.0
on a busy machine, so every ratio threshold carries a wide canary margin (the
canary catches a *broken* invariant — inlining collapsing to ~1.0× — not a few
percent of drift); the allocation gates need no such margin because bytes/op is
near-deterministic. The contention gate uses the 32-stage denominator, not the
1-stage one, because JMH leaves the very first benchmark in a fork nearly cold and
that noise would swamp a 1-stage denominator.

SonarLint over `tests/` is clean on the new file except `S106` (console output),
documented in `tools/sonarlint/README.md` as deliberate — a gate harness whose job
is to print a report and set an exit code.

**CI wiring is done.** A `jmh-gates (canary)` job in `.github/workflows/build.yml`
runs `./gradlew jmhGates` on every push and PR, separate from the build matrix
because it measures cost, not correctness. It runs the 4 core-count-insensitive
ratio gates plus the 3 allocation gates — all portable to a shared 2-core runner.
The **contention gate is full-pass only**: "the boss spreads across the pool" is a
multi-core claim, and 8 threads cannot scale on a 2-core runner, so asserting it
there would be meaningless; it runs in `-PgatesMode=full` on a real multi-core box
(nightly / pre-release). The canary's job is to catch a *catastrophic* regression
on a PR, and it needs cores it can trust to do it.

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
