# RFC 0022 — A benchmarks page: the performance claims, documented as evidence

- **Status**: ✅ Implemented — `benchmarks.md` with numbers measured on an M1, linked in sidebar + navbar
- **Target**: `docs/` (the docsify site) — a new `benchmarks.md`, plus `_sidebar.md`/`_navbar.md`
- **Depends on**: RFC 0021 (the JMH gates that produce and enforce the numbers) and the throughput series (0009, 0011–0017) that measured them
- **Sibling of**: RFC 0018/0019 (docs work) — same axis: what the reader sees
- **Realized by**: `docs/benchmarks.md` under **Deep dive** in `_sidebar.md` and in the
  navbar (both `_navbar.md` and the mirror in `index.html`); numbers measured on an
  Apple M1 / JDK 25.0.3 with `./gradlew jmhGates -PgatesMode=full`. See **Results**.

## Summary

nioflow's pitch is a performance one — "typed pipelines on an **event loop**",
"fusion makes the links free", "no worker parks on an async call". Every one of
those claims is backed by a JMH benchmark and a measured number, and RFC 0021 now
**enforces** a subset of them as CI gates. But a reader of the docs site cannot
see any of it: the numbers live scattered across RFCs 0009–0017 (design records,
not a reader's destination), and the `tests/` benchmarks are a source tree, not a
page. The site says "it's fast" and asks the reader to trust it.

This RFC adds the page that turns the claim into **evidence**: a single
`benchmarks.md` that names each benchmark, states what it measures, shows the
headline result, and — the load-bearing part — tells the reader **exactly how to
reproduce it** and **which RFC 0021 gate keeps it from regressing**. It documents
the numbers as measurements a reader can re-run, not as marketing a reader must
believe. It changes no engine or benchmark code; it is a docs page and two nav
edits.

## Why a page, and why now

- **The evidence exists but is invisible.** `CLAUDE.md` and the throughput RFCs
  carry the numbers (fusion ~5× on an 8-stage chain, boss-inlining 248 B/op vs
  727, async 489 B in-flight vs 3 173 parked). A reader evaluating the library
  should not have to read eight design RFCs to find them.
- **RFC 0021 made them durable.** Before 0021 a docs number was a claim frozen at
  writing time. Now the headline ratios are **gated in CI**, so a benchmarks page
  can say "this is measured, it is enforced, here is the gate" — the number is not
  a snapshot, it is a contract. That is the difference between docs that rot and
  docs that stay true, and it is why this page is worth writing now and was not
  before.
- **Reproducibility is the whole point.** A benchmark number nobody can re-run is
  indistinguishable from a made-up one. The page's spine is the command that
  produces each number (`./gradlew jmh ...`, `./gradlew jmhGates`), so a skeptical
  reader confirms it on their own hardware in minutes.

## What the page contains

One page, `docs/benchmarks.md`, in the reader's voice (not the RFC's), with these
sections:

- **How to run them.** The three commands up front — `cd tests && ./gradlew jmh`
  (full suite), `-PjmhArgs='...'` (one benchmark), and `./gradlew jmhGates` (the
  regression gates from RFC 0021). Reproduction is the first thing, not a footnote.
- **The headline results**, as a small table: the claim, the benchmark that shows
  it, the measured number, and the reproduction target. Rows are the load-bearing
  facts:
  - *Fusion makes the links free* — a 1-stage and a 32-stage chain cost about the
    same (`NioFlowBenchmark`).
  - *Boss-inlining is ~2.2×* and allocates 248 B/op vs 727 (`SyncStageBenchmark`).
  - *The async stage parks no worker* — 489 B in-flight vs 3 173 B parked, within
    ~3% of blocking throughput (`ReactiveBenchmark`, `ReactiveHeapProbeTest`).
  - *Compiled ≡ interpreted* — throughput parity, ~13% less garbage
    (`CompiledChainBenchmark`).
  - *A dedicated pool protects tail latency* — p99.9 −48% beside a noisy neighbor
    (`DedicatedPoolBenchmark`).
- **What is enforced.** A short section pointing at RFC 0021: which of the above
  are CI gates and which are one-off measurements, so the reader knows which
  numbers cannot silently regress.
- **How to read a benchmark number honestly.** The disclosure that makes the page
  evidence and not marketing (see below): the machine/JDK the absolute numbers
  came from, and why the **ratios** — not the absolute ops/ms — are the portable
  truth.

## The honesty rules (what keeps this from being marketing)

- **Absolute numbers carry their machine.** Every ops/ms or B/op on the page is
  stamped with the JDK and CPU it was measured on, and dated. A number without its
  machine is not evidence.
- **Ratios are the claim; absolutes are an illustration.** The page leads with the
  relationships (fused vs unfused, async vs parked, compiled vs interpreted) —
  exactly the machine-independent gates RFC 0021 enforces — and treats the raw
  ops/ms as a concrete example, not a promise. This is the same reason 0021's CI
  gates are ratios: they survive a different machine, and so does the page's
  argument.
- **Reproduction over assertion.** Wherever the page states a number it names the
  command that produces it. The reader is invited to disprove it, which is what
  makes it worth stating.
- **B/op ≠ retained heap.** The page keeps the two memory stories distinct (the
  trap RFC 0021's implementation hit): *allocation rate per op* (`gc.alloc.rate.norm`,
  from JMH) is not *retained heap per in-flight request* (from `ReactiveHeapProbeTest`).
  The async stage allocates a little more per op and retains far less in flight;
  conflating them is how "489 B" gets misquoted.

## Results

`docs/benchmarks.md` ships with **real, dated, machine-stamped numbers** — not
placeholders. They were measured on an **Apple M1 (8 cores), OpenJDK 25.0.3,
macOS 14.6.1, 2026-07-15** via `./gradlew jmhGates -PgatesMode=full` (all gates
green, including the multi-core contention gate the M1 can actually exercise):

- Fusion makes links free: `engineCall` 88.6 ops/ms at 1 stage, 58.8 at 32 — a
  32× longer chain is only ~1.5× slower, and allocation is flat (616 B/op at both).
- Boss-inlining: `syncSingle` 224.8 vs `workerSingle` 77.9 ops/ms (2.9×), 248 vs
  688 B/op.
- Compiled parity: 81.9 vs 78.9 ops/ms (1.04×), 688 vs 832 B/op (~17% less garbage).
- Async within band of blocking: 74.9 vs 72.4 ops/ms (~1.0×).
- Boss not a ceiling: contended[32] 109.3 vs single[32] 58.8 ops/ms (1.86× on 8 cores).

Two numbers are **cited, not re-run** here, and labelled as such on the page: the
async retained-heap footprint (489 B vs 3 173 B, from `ReactiveHeapProbeTest` /
RFC 0006 & 0015) and the dedicated-pool tail-latency win (p99.9 −48%, from
`DedicatedPoolBenchmark`) — both need a different measurement mode, so restating
them as freshly measured would be dishonest.

The page leads with reproduction (the three `gradlew` commands), presents the
ratios as the portable claim and the absolutes as a dated illustration, points its
"what is enforced" table at RFC 0021's gates by name, and keeps `B/op` distinct
from retained heap. The documented `jmh -PjmhArgs='...'` and `jmhGates` commands
were run verbatim to confirm they work as printed. It is linked under **Deep dive**
in the sidebar and added to the navbar (both copies kept in sync).

## Non-goals

- **No engine or benchmark change.** This is a docs page over the existing
  `tests/` benchmarks and RFC 0021's gates.
- **No auto-generated numbers in CI.** The page is hand-curated prose with dated,
  machine-stamped figures; it does not wire a job that re-runs `jmh` and rewrites
  the page (that is a live-dashboard idea, and a flaky one — a shared CI runner's
  absolute numbers are noise). Keeping the ratios and the reproduction commands is
  what stays true without automation.
- **No new benchmarks.** If a claim on the site has no benchmark, that is a gap for
  a throughput RFC to fill, not for this page to paper over.
- **No performance tuning.** The page reports; it does not chase numbers.

## Testing

Docs have no unit suite; the guardrails are mechanical and match RFC 0018's:

1. **Every command on the page runs.** `cd tests && ./gradlew jmh -PjmhArgs='-f 0 -wi 1 -i 1 <Name>'`
   and `./gradlew jmhGates` execute as written — a copy-pasted command that fails
   is worse than no page.
2. **Every number traces to a source.** Each figure cites its benchmark class (and,
   where enforced, its RFC 0021 gate), so a reader can regenerate it and a
   maintainer can re-verify it.
3. **The sidebar and navbar link it.** `benchmarks.md` appears under **Deep dive**
   in `_sidebar.md`; the navbar mirror in `index.html` stays in sync (the pattern
   RFC 0019 already maintains).

## Risks

- **Absolute numbers go stale.** The mitigation is structural, not diligence: the
  page's *claims* are ratios (which do not rot) and *commands* (which stay
  runnable); the absolute ops/ms are dated, machine-stamped illustrations a reader
  is told to regenerate. A stale illustration is visibly dated, not silently wrong.
- **The page drifts from the gates.** If RFC 0021's thresholds change, the page's
  "what is enforced" section must move with them. Mitigation: the section points at
  the gates by name rather than restating their thresholds, so it references the
  contract instead of copying it.
- **It reads as marketing.** The honesty rules exist precisely to prevent this: a
  number with no machine, no date, and no reproduction command does not go on the
  page. If a claim cannot be reproduced, it is cut, not softened.
