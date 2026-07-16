# Troubleshooting & operability

The incident you *will* have with an event-loop + virtual-thread engine is a
**hung request**. This page maps the symptoms to the cause and the fix. Read it
before you need it.

## The two thread kinds, and what "hung" looks like on each

| | Bosses | Workers |
|---|---|---|
| Kind | platform threads | virtual threads |
| Name | `nio-flow-boss-N` (dedicated: `nio-flow-boss-dedicated-N`) | unnamed by default |
| Count | `-Dnioflow.bosses=N` (default: cores, floor 2) | one per in-flight stage, unbounded |
| Runs | orchestration + `Decision`/`Filter` predicates, `handleSync`, `onComplete`/`onError` | your `handle`/`background`/`recover` code, and reactive `handleMono` (parked on `mono.block()`) |
| Must never block | **yes** — a blocked boss stalls every request pinned to it | no — blocking is what virtual threads are for |

**A boss must never block.** It is mechanical, not aspirational: the example's
BlockHound gate marks the bosses `NonBlocking`, so a blocking `handleSync`, a
blocking `Decision` predicate, or a blocking `onComplete`/`onError` handler trips
the test suite (RFC 0029). If you copy those patterns into your own service,
carry the BlockHound gate too.

## A request hangs — the playbook

### 1. Take a virtual-thread dump

`jstack` shows the platform threads (the bosses) but not the parked virtual
workers, which is where a reactive hang lives. Use `jcmd`:

```bash
jcmd <pid> Thread.dump_to_file -format=text /tmp/threads.txt
# or, for tooling: -format=json
```

### 2. Read the frames

- **A worker parked on a remote call** shows
  `…Blocking.await → Mono.block → …` (a `handleMono`/`adaptMono` waiting on a
  `Mono` that has not completed). If many workers sit there and never move, the
  upstream is hung.
- **A worker parked on `LockSupport.park` inside `RateLimit.acquire`** is
  admission backpressure, not a bug — it is waiting for a token.
- **A boss** (`nio-flow-boss-N`) parked on its task queue is idle — normal. A
  boss *running* your code (in a `handleSync`/`Decision`/handler frame) that
  never returns is the bug BlockHound exists to catch.

### 3. Match the cause

| Symptom | Cause | Fix |
|---|---|---|
| Workers pile up on `Blocking.await`, memory climbs, count never falls | a reactive step with **no budget** on a hung upstream — the worker (and its `Execution`, ~3.6 KB) is parked forever, and the engine has no cancellation to reclaim it | declare a `defaultBudget` (or a per-step budget) on the reactive flow. Since RFC 0034 this is enforced at build time (`requireBudget` is on by default); a flow reaching production without one predates that or waived it with `allowUnbudgeted()` |
| One key's requests serialize and back up | keyed execution is per-key FIFO; a stalled head blocks its lane | bound the lane with `keyLaneCapacity(maxDepth, policy)` and watch the `keyLaneDepth`/`keyLanesActive` metrics (RFC 0039); a stage timeout on the head stage bounds head-of-line time |
| p99 spikes correlated across unrelated flows | a slow `onComplete`/`onError` handler or a slow `handleSync` on a **shared** boss | move heavy handler work to your own executor; keep boss-run code non-blocking (the BlockHound gate enforces it) |
| `shutdown(grace)` returns non-zero (work still running) | in-flight executions — often a stuck keyed head, a batch still inside its window, or a fork still running — did not finish in the grace | raise the grace, or bound the lane/timeout the stalled stage; the return value is how many were still running |
| Memory climbs under load with a bounded engine, yet nothing is rejected | the capacity bounds the path you configured — check you are bounding the right one: `capacity`+`OverflowPolicy` bounds `inject`/`await` **and** `call`/`callCancellable` (RFC 0031), but only if you passed it | pass `new DefaultNioEngine(capacity, policy)`; front the endpoint with your own limiter if calls originate on an event loop (BLOCK parks the caller) |

## The guardrails, and how to keep them on

- **`defaultBudget` / `requireBudget`** (reactive): a reactive step with no
  budget is a build error by default (RFC 0034). Do not reach for
  `allowUnbudgeted()` on a network-facing flow — it turns off the one thing
  between a hung socket and a leaked worker.
- **The BlockHound gate** (RFC 0029): marks the bosses non-blocking so a blocking
  boss-run function fails a test instead of stalling prod. It lives in the
  example's test scope; carry it into yours.
- **Metrics** (`NioEngine.metrics`): install a `NioFlowMetrics` sink and watch
  `queueDepth`, `keyLaneDepth`/`keyLanesActive`, per-stage latency, and the
  execution latency classified completed/failed/filtered/cancelled. Buildup shows
  here before it is an outage.
- **`dedicated(bossCount)`**: give a latency-critical flow its own boss pool so a
  noisy neighbour's slow boss code cannot stall it, and its `shutdown()` does not
  touch the shared pools.

## When the answer is "not nioflow"

If every stage is a remote call, concurrency is very high, and you use none of
the engine (retry, rate limit, batch, key, fork, recover, splice, metrics), a
worker parked on `Blocking.await` retains ~16× what a pure Reactor chain does.
At 100k concurrent that is the difference between 360 MB and 21 MB. Reach for
plain Reactor there — the [WebFlux page](webflux.md) has the decision tree.
