# RFC 0016 — `fanOutMono` without parked workers

- **Status**: Proposed
- **Target**: `reactive/` (`infrastructure.reactive`)
- **Depends on**: RFC 0012 (`fanOutAsync` in core) — **hard**
- **Part of**: the throughput series (0009–0017)

## Summary

`fanOutMono` over N remote calls parks **N** virtual workers for the duration of the slowest branch — the exact cost RFC 0006 removed from the main line and RFC 0012 removes from `FanOut` itself. Once core ships `fanOutAsync`, `fanOutMono` decorates it instead of blocking N branches: N concurrent calls, zero parked workers.

## Today: one parked worker per branch

```java
// Blocking.branches (reactive/…/Blocking.java:101)
blocking.add(value -> await(budgeted(branch.apply(value), budget)));
```

```mermaid
flowchart TD
    FO["fanOutMono(4 remote calls)"] --> B1["branch 1 → await → PARKED worker"]
    FO --> B2["branch 2 → await → PARKED worker"]
    FO --> B3["branch 3 → await → PARKED worker"]
    FO --> B4["branch 4 → await → PARKED worker"]
    B1 & B2 & B3 & B4 --> J["join(results)"]
```

Four threads' worth of stack retained for one fan-out, for the whole duration of the slowest call.

## Proposed: decorate `fanOutAsync`

```mermaid
flowchart TD
    FO["fanOutMono(4 remote calls)"] --> A1["branch 1 → mono.timeout(budget).toFuture()"]
    FO --> A2["branch 2 → …toFuture()"]
    FO --> A3["branch 3 → …toFuture()"]
    FO --> A4["branch 4 → …toFuture()"]
    A1 & A2 & A3 & A4 -->|no thread held| CD["core fanOutAsync countdown (RFC 0012)"]
    CD --> J["join(results) on the last branch's worker"]
```

Each branch becomes `value -> call.apply(value).toFuture()` — a `CompletionStage`, parking nothing. Core's `fanOutAsync` countdown (RFC 0012) fires the join once all branches complete. Four concurrent remote calls, one execution's worth of heap instead of four threads' worth.

## Design notes

- **The per-branch budget still applies**: `mono.timeout(budget)` before `.toFuture()`, so a hung branch is cancelled (subscription disposed) exactly as on the async main line — strictly better than the block-path abandon.
- **The join is unchanged** — same `Function<List<R>, C>`, same declaration-order results.
- **`Blocking.branches` stays** for the (rare) case a caller explicitly wants blocking fan-out branches; `fanOutMono` simply stops using it.
- **Mirror parity**: `fanOutMono` must keep its override on `ReactiveStep` **and** `ReactiveLane` (fan-out inside a branch/fork). `ReactiveMirrorTest` guards this — the drift it caught once (`adaptMono(call, budget)` missing on the lane) is exactly this failure mode.

## Testing

- **Concurrency**: branches with a controllable latch run concurrently — total ≈ slowest, not sum.
- **No parked workers**: heap probe on a `fanOutMono` shape lands near one execution's floor, not N threads.
- **Budget cancels a hung branch**: reaches `recover()` as `TimeoutException`; a failing branch fails the fan-out, recoverable downstream.
- **In a lane**: `fanOutMono` inside a `when`/`match` branch and inside a `fork` still parks nothing (the lane mirror uses the same async path).
- `ReactiveMirrorTest`, full `reactive/` suite unchanged.

## Gate

| Benchmark | Must |
| --- | --- |
| new `fanOutMono` bench | concurrent; no parked workers |
| heap probe (fan-out shape) | ~1 execution floor, down from N×3 KB |

## Risks

- **Hard dependency on RFC 0012's `fanOutAsync`.** Until it lands, `fanOutMono` stays as-is.
- **Eager subscription** via `.toFuture()` (same as RFC 0015) — a branch Mono with subscribe-time side effects sees them earlier. Documented.
