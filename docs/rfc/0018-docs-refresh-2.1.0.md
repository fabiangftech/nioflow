# RFC 0018 — Documentation refresh for the 2.1.0 release

- **Status**: ✅ Implemented
- **Target**: `docs/` (the docsify site and its `README.md`), `core/` and `reactive/` build versions
- **Depends on**: RFC 0009, 0011–0017 (the throughput series) — the features this refresh documents
- **Part of**: release hygiene, not the throughput series — but it is what ships the series to readers
- **Realized by**: the version bump (`core/build.gradle`, `reactive/build.gradle`,
  `_coverpage.md`, `quickstart.md`, `webflux.md`); the content updates to
  `README.md`, `api-reference.md`, `pipeline-api.md`, `webflux.md`,
  `architecture.md`, `observability.md`; and the release-blocker fix in the
  `springwebflux` example (see Testing).

## Summary

The throughput series (0009, 0011–0017) added a public surface the user-facing
docs never grew to match: the prebuilt `Pipeline` (0011), core's async stage and
`fanOutAsync` (0006, 0012), async-stage fusion (0013), `pipe` over a `Pipeline`
(0014), the async-routed `pipe` and `preferAsync()` (0015), the non-parking
`fanOutMono` (0016) and the `adaptFlux(call)` deprecation (0017). The reference
pages also carry **stale claims** the series invalidated — most visibly the
`handleMonoAsync` throughput numbers, which predate 0013 fusing async runs.

This RFC records the decision to **refresh the whole `docs/` site** to match the
code as it stands and to **cut a `2.1.0` release** (`core` and `reactive` move
from `2.0.0`). It changes no engine behaviour; it is documentation and versions.

## Why a minor release, and why 2.1.0

Everything in the series is **additive and source-compatible**: new methods
(`pipeline`, `handleAsync`/`fanOutAsync`, `preferAsync`), a widened sealed `Link`
(a new `AsyncStage` permit — source-breaking only for a consumer's exhaustive
`switch` over `Link`, which the reference already warns about), and one
`@Deprecated(forRemoval = false)` overload that removes nothing. No behaviour a
`2.0.0` caller relied on changed. So it is a **minor** bump: `2.0.0 → 2.1.0`, the
two artifacts in lockstep as always.

`0010` is not in the release (rejected, measured regression) and `0017`'s
blocking fast-path is not (measured neutral, dropped); everything else is.

## Scope — what the refresh must fix

Two kinds of change, no style change: the docs keep their voice, their tables,
their mermaid, their "one claim, then the reason" shape. Sections may be
reworded and reordered for clarity; nothing is rewritten for its own sake.

### Stale claims to correct

- **`webflux.md` — the `handleMonoAsync` numbers predate 0013.** "four
  dispatches", "20.6 ops/ms", "8 µs per extra dispatch" describe the *unfused*
  async path RFC 0013 replaced. A run of consecutive async stages now fuses (the
  trampoline drives it worker-side, one boss touch per run), landing within ~3%
  of blocking (`fourAsyncReactiveStages` 74.4 vs `fourReactiveStages` 72.4
  ops/ms). The decision table's throughput row is wrong and must be rewritten;
  the heap numbers (3 173 B vs 489 B) stand.
- **`webflux.md` — `fanOutMono` parks nothing now (0016).** "each on its own
  worker" reads as the old blocking fan-out; it decorates core's `fanOutAsync`,
  so N branches hold N futures, not N parked workers.
- **`api-reference.md` / `architecture.md` — the sealed `Link` set is missing
  `AsyncStage`.** Both list eight permits; there are nine.
- **`api-reference.md` — `FlowResult` is missing `Cancelled`** (RFC 0007). It is
  a three-arm sealed type: `Completed | Filtered | Cancelled`.

### New surface to document

- **The prebuilt `Pipeline` (0011)** — `flow.pipeline(segment)` records,
  validates and compiles a fixed per-request pipeline once; `Pipeline.just(x)`
  dispatches off the plan. Belongs in `pipeline-api.md` and `api-reference.md`.
- **Core's async stage (0006) and `fanOutAsync` (0012)** — `handleAsync` /
  `adaptAsync` / `fanOutAsync` are `CompletionStage`-shaped and live in **core**
  (no Reactor): `HttpClient.sendAsync`, the AWS SDK v2, a Cassandra driver. The
  reference tables list none of them.
- **`pipe` over a `Pipeline` and `preferAsync()` (0014, 0015)** — the ingestion
  loop should build once and route async by default; `webflux.md` shows only the
  per-element `BiFunction` form on the parking path.
- **`executeCancellable()` / cancellation in core (0007)** — the reference lists
  the terminals but not the cancellable one.
- **The boss event loop (0009)** — `architecture.md` can name the MPSC
  spin-then-park loop and the async-run trampoline as the mechanisms behind
  "the boss never blocks".

### Everywhere

- **Versions: `2.0.0 → 2.1.0`** in `core/build.gradle`, `reactive/build.gradle`,
  and the coordinate snippets in `_coverpage.md`, `quickstart.md`, `webflux.md`
  and `README.md` (which gains its own compact install block).
- **`observability.md`** — add `executionCancelled` to the classified outcomes
  (0007), and note per-stage timing covers async runs.

## Non-goals

- No engine or API change — this RFC ships docs and version numbers only.
- No new page and no page removed; the sidebar and navbar are unchanged.
- No re-benchmarking — the series' RFCs carry the measured numbers; this refresh
  cites them.

## Testing

Docs have no unit tests; the guardrail is that every code snippet compiles
against the 2.1.0 API and every number cited traces to a series RFC's gate
table. The examples (`examples/springboot-with-nioflow`,
`examples/springwebflux-with-nioflow`) must keep compiling **and their tests must
pass** against the working tree — a release does not ship on red examples.

**Release blocker found and fixed here.** Running the `springwebflux` example's
suite (which had only been compile-checked since RFC 0009, not run) surfaced a
hang: RFC 0009 replaced the boss `ThreadPoolExecutor` with a custom `BossLoop`
that waits for work by spinning (`Thread.onSpinWait`) then parking
(`LockSupport.park`) inside `BossLoop.runLoop`. BlockHound flags both on the boss
(a non-blocking thread), but the example's `NioFlowBlockHoundIntegration` still
allowlisted the old `ThreadPoolExecutor.getTask` — so an idle boss threw an
uncaught `BlockingOperationError`, the boss thread died, and every execution
pinned to it hung forever. The fix moves the allowance to `BossLoop.runLoop` and
disallows `BossLoop.run` (the boss's user-code dispatch), so the loop's own idle
wait is allowed while a `handleSync` stage that blocks still trips — the guarantee
`aBlockingCallPlantedOnABossTripsIt` exists to make. Example suite green: 13/13.

## Risks

- **Doc drift is silent.** The mitigation is this RFC's scope list: each stale
  claim and each new-surface item is named, so the refresh is a checklist, not a
  vibe. A future feature that skips its doc update reopens exactly this gap.
- **Version snippets are duplicated across pages.** `2.1.0` appears in five doc
  files plus two build files; all seven move together here, and the README's
  version badges read the live Maven Central version so they never go stale.
