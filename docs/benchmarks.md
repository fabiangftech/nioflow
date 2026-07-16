# Benchmarks

nioflow's design is a performance argument: an event loop that never blocks, a
chain whose links **fuse** so a long pipeline costs about what a short one does,
and an async stage that holds no thread. This page is the evidence — the numbers
those claims rest on, **how to reproduce every one of them**, and which of them
[CI enforces so they cannot silently regress](#what-is-enforced).

Nothing here is a number you have to take on faith. Each row names the benchmark
that produces it and the command that runs it. Run them on your own hardware; the
**ratios** should hold even where the absolute numbers do not.

## Run them yourself

The benchmarks live in the `tests/` module (JMH, in `tests/src/main/java`):

```bash
cd tests
./gradlew jmh                                                # full suite (minutes)
./gradlew jmh -PjmhArgs='-f 1 -wi 3 -i 5 NioFlowBenchmark'   # one benchmark
./gradlew jmh -PjmhArgs='-f 0 -wi 1 -i 1 SyncStageBenchmark' # a quick smoke
```

And the regression **gates** — the subset of these numbers CI holds to a
threshold ([RFC 0021](rfc/0021-jmh-regression-gates.md)):

```bash
./gradlew jmhGates                    # fast canary (relative + allocation gates)
./gradlew jmhGates -PgatesMode=full   # the tighter full pass
```

The gate harness prints a measured table and fails with a non-zero exit code if
any performance invariant regressed.

## Headline results

> The numbers below were measured on an **Apple M1 (8 cores), OpenJDK 25.0.3,
> macOS 14.6.1**, on **2026-07-15**, with `./gradlew jmhGates -PgatesMode=full`.
> They are an illustration on one machine — the **ratios** are the claim, and they
> travel; the absolute ops/ms do not. Re-run the command to get your own.

| The claim | Measured here | Ratio (the portable part) | Benchmark |
| --- | --- | --- | --- |
| **Fusion makes the links free** — a long chain costs about what a short one does | 1-stage **88.6** ops/ms vs 32-stage **58.8** ops/ms | 32× longer → **~1.5× slower**, not 32× | `NioFlowBenchmark.engineCall` |
| **Boss-inlining a pure-CPU stage skips both thread hops** | `handleSync` **224.8** vs worker `handle` **77.9** ops/ms | **2.9×** faster | `SyncStageBenchmark` |
| **The compiled plan keeps parity** — it is an optimization, never a slowdown | compiled **81.9** vs interpreted **78.9** ops/ms | **1.04×** (parity) | `CompiledChainBenchmark` |
| **An async stage lands within a hair of blocking** on throughput | async **74.9** vs blocking **72.4** ops/ms | **~1.0×** (within ~3.5%) | `ReactiveBenchmark` |
| **The boss is not a JVM-wide ceiling** — concurrent work spreads across the pool | 8-thread **109.3** vs 1-thread **58.8** ops/ms (32 stages) | **1.86×** on 8 cores | `NioFlowBenchmark.engineCallContended` |

### The allocation story

Throughput is half of it; the other half is garbage. Measured the same run
(`gc.alloc.rate.norm`, **bytes allocated per operation**):

| The claim | Measured here | Benchmark |
| --- | --- | --- |
| A plain sealed chain is lean, and **fusion keeps allocation flat over links** | **616 B/op** at 1 stage *and* at 32 stages | `NioFlowBenchmark.engineCall` |
| **Boss-inlining allocates far less** than a worker hop | **248 B/op** vs **688 B/op** | `SyncStageBenchmark` |
| **The compiled plan produces less garbage** than interpreting | **688 B/op** vs **832 B/op** (~17% less) | `CompiledChainBenchmark` |

### Two numbers cited, not re-run here

These come from their own measurements (a unit heap probe and a tail-latency
benchmark), not from today's throughput run, so they are attributed to their
source rather than restated as if freshly measured:

- **The async stage parks no worker.** An in-flight request retains **489 B** on
  the async path against **3 173 B** parked on the same Mono — a 6.5× lighter
  footprint under load. Measured by `ReactiveHeapProbeTest`; see
  [RFC 0006](rfc/0006-async-stage.md) and [RFC 0015](rfc/0015-async-routed-pipe.md).
- **A dedicated pool protects tail latency.** Beside a noisy neighbour, giving a
  latency-critical flow its own boss pool cut **p99.9 by ~48%**. Measured by
  `DedicatedPoolBenchmark`; see the scaling notes in
  [Scaling & ordering](scaling.md).

## What is enforced

Most of the headline ratios are not just measured once — they are **gates in CI**
([RFC 0021](rfc/0021-jmh-regression-gates.md)). A pull request that regresses one
fails the build. The gates are **relative** (a ratio between two benchmarks in the
same run) or **allocation-based** (bytes/op), because those survive a slow shared
CI runner where an absolute ops/ms floor would only produce flakiness.

| Invariant | Gated on every PR? |
| --- | --- |
| Fusion makes the links free (`engineCall[32]` vs `[1]`) | ✅ canary |
| Boss-inlining is a speedup (`syncSingle` vs `workerSingle`) | ✅ canary |
| Compiled plan keeps parity | ✅ canary |
| Async stage within band of blocking | ✅ canary |
| No per-link allocation (32-stage B/op ≈ 1-stage) | ✅ canary |
| Boss is not a JVM-wide ceiling (contention) | ▲ full pass only — a multi-core claim, meaningless on a 2-core CI runner |

The one-off measurements — the retained-heap footprint and the dedicated-pool tail
latency — are pinned by their own tests (`ReactiveHeapProbeTest` and the stress
suite), not by a throughput gate.

## How to read these honestly

- **The ratio is the claim; the absolute is an illustration.** "32 stages cost
  ~1.5× a 1-stage chain" is true on any machine; "88.6 ops/ms" is true on an M1 on
  one afternoon. Trust the first, reproduce the second.
- **Allocation is the low-variance number.** Bytes-per-op counts allocations, not
  wall-clock, so it barely moves across machines — which is why it makes a good
  gate and a trustworthy figure. Throughput ratios compress toward 1.0 on a busy
  machine, so give them room.
- **Bytes-per-op is not retained heap.** The async stage allocates a little *more*
  per operation (four dispatch round trips where blocking fuses into one) yet
  retains far *less* in flight (**489 B** vs **3 173 B**). They are different
  measurements answering different questions — throughput garbage vs memory held
  per concurrent request — and only the second is why you reach for an async stage
  at high concurrency.
- **If a claim has no benchmark, it is not on this page.** Every number here traces
  to a class in `tests/`. A performance claim without one is a gap for a design RFC
  to close, not a line to add here.
