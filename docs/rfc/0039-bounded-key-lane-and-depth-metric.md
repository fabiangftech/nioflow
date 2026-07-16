# RFC 0039 — Bound the per-key lane, and surface its depth

- **Status**: 📋 Proposed
- **Target**: `core` (`DefaultNioEngine` — `KeyLane`, the metrics SPI)
- **Depends on**: RFC 0026 (the off-boss key-lane release), RFC 0024 (the drain counter it interacts with)
- **Severity**: **Medium** — a hot-key head-of-line hazard: an unbounded FIFO lane can grow without limit and block a clean drain, and operators cannot see it building
- **Realized by**: an optional per-key queue bound (reusing `OverflowPolicy`), and at minimum promoting `keyLaneDepth` from a test hook to a real metric so head-of-line buildup is observable before it becomes an outage.

## The finding

`KeyLane.waiting` (`DefaultNioEngine.java:174`) is an **unbounded** `ConcurrentLinkedQueue`, and keyed execution is strict per-key FIFO with head-of-line blocking: only the lane's head advances; everyone else waits. If a hot key's head stage has no timeout (the default), every subsequent same-key call queues **forever**, each one:

- holding an `activeExecutions` slot, so `shutdown(grace)` can **never drain cleanly** while the head is stuck;
- for the `inject` path, holding an in-flight permit too.

`CLAUDE.md`'s mitigation is "head-of-line per key is the point — stage timeouts bound it." True, but nothing *enforces* or *defaults* a timeout, so the safe configuration is opt-in and the unbounded one is the default. And the lane depth is currently only a test hook (`keyLaneDepth`, `:688`), not a metric — so an operator watching a hot key build up has no signal until the symptom (unbounded memory, a drain that won't complete) is already the incident.

## Why it matters

Keyed execution exists to serialize same-key work (per-account ordering, per-tenant FIFO). Hot keys are the *normal* case for it, not the exception — and a hot key whose head stalls is precisely when the lane grows. With no bound and no metric, the failure is a silent queue climb behind one stuck execution, invisible until it blocks a deploy's graceful drain or exhausts the heap. Both a bound and a gauge are standard for any FIFO admission structure; the lane has neither.

## The options

1. **Optional per-key queue bound reusing `OverflowPolicy` (recommended).** Let the engine (or the keyed step) carry an optional max lane depth with BLOCK / DROP / FAIL semantics, mirroring the `inject` capacity design: past the bound, BLOCK parks the producer, DROP reports the value dropped, FAIL rejects. Off by default (preserving today's behavior), opt-in for services that keep keys hot. This gives keyed execution the same backpressure story the rest of the engine has.

2. **Surface `keyLaneDepth` as a metric (minimum, do regardless).** Promote the existing test hook to the `NioFlowMetrics` SPI — a per-key (or max-across-keys) gauge of lane depth, pushed like `queueDepth` already is. Cheap, non-breaking, and it turns the silent buildup into a dashboard line an operator can alert on *before* the bound matters.

3. **Default a max head-of-line time (softer, riskier).** Give keyed execution a default per-head timeout so a stuck head cannot block its lane forever. Simpler for users but a wrong default breaks slow-but-valid heads; a bound + metric (options 1+2) is more honest than a hidden timeout.

Recommended: **option 2 first** (make it observable — nearly free), then **option 1** (make it bounded — the real fix), leaving the bound off by default so no existing behavior changes.

## Testing

- With option 1 and a bound of N under `FAIL`, the (N+1)-th queued same-key call rejects while the head is held by a latch; releasing the head drains the lane in FIFO order and admits new calls.
- With option 2, a stalled head builds `keyLaneDepth` and the metric reflects it; draining the head returns it to zero and removes the lane (no key leak — the RFC 0026 removal path still holds).
- A drain test: a stuck no-timeout key keeps `activeExecutions > 0`; with a bound + a head timeout the lane drains and `shutdown(grace)` reports clean — documents the interaction between this and the drain (RFC 0024).

## Risks

- **A bound changes keyed semantics** (a rejected same-key call is new behavior). Keep it off by default and opt-in; document that keyed + bound means "ordering *and* backpressure," and that BLOCK on an event-loop thread is the same hazard called out in RFC 0031 (prefer FAIL/DROP for `call()`).
- **Per-key metric cardinality.** A gauge *per key* can explode cardinality for high-key-count workloads; prefer a max-across-active-lanes gauge plus an active-lane count, not one series per key.
- **The bound must not deadlock with the drain.** A BLOCKed producer parked on a full lane during shutdown must be released/rejected when the engine closes, exactly as `admit()`'s BLOCK path must — reuse that shutdown-unpark logic.
