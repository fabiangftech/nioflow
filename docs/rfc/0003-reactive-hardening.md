# RFC 0003 — Reactive hardening: the thread leak, the missing knobs, the stale promises

- **Status**: Proposed
- **Target**: `infrastructure.reactive`, `tests/`, `examples/springwebflux-with-nioflow`, `docs/`
- **Depends on**: RFC 0002
- **Independent of**: RFC 0004–0007. **Ship this one first** — it is days of work, carries no design risk, and closes a thread leak.

## Summary

Six gaps found by auditing the shipped RFC 0002 implementation against its own text. None of them is a design question; every one of them is a thing that is wrong or missing today. One of them can leak a virtual thread permanently.

| # | Gap | Severity |
| --- | --- | --- |
| 1 | `Blocking.await` is `mono.block()` **with no timeout** — an un-budgeted `handleMono` on a Mono that never completes parks a worker **forever** | **leak** |
| 2 | `ReactiveLane.adaptMono` has no budget overload, while `ReactiveStep.adaptMono(call, budget)` does | omission |
| 3 | `pipe` exposes no `prefetch` and validates no `concurrency` | rough edge |
| 4 | One failing element kills the whole `pipe`d stream — the failure mode is a discovery, not a choice | rough edge |
| 5 | The BlockHound allowlist `0002:461` promised was never shipped | stale promise |
| 6 | `0002:499` says the WebFlux example "is an empty Spring Boot skeleton today". It is fully implemented | stale doc |

## 1. The thread leak — `Blocking.await` blocks forever

```java
// Blocking.java:37
static <T> T await(Mono<T> mono) {
    try {
        return mono.block();          // ← no timeout, ever
    } catch (RuntimeException error) {
        ...
    }
}
```

A `handleMono(name, call)` without a budget, over a `Mono` that never completes — a hung connection, a broken server, a `Sinks.One` nobody emits into — **parks a virtual worker permanently**. The engine has no cancellation (RFC 0007 is the fix, and it depends on RFC 0006), so nothing can ever free it. The worker is gone for the life of the JVM.

This is the one place in the current design that leaks a thread with no recovery, and no document names it. It should not survive.

**Proposed** — a default budget, declared on the flow:

```java
// ReactiveFlow
ReactiveFlow<I, O> defaultBudget(Duration budget);
```

Applied at **build time** to any `handleMono`/`adaptMono`/`fanOutMono` that was declared without one: the step becomes `call.apply(v).timeout(budget)`, which cancels the subscription and reaches `recover()` as a `TimeoutException` — the semantics `handleMono(name, call, budget)` already has. Explicit per-stage budgets always win.

Not proposed: a *mandatory* budget. A `Mono.just(...)` or an in-memory cache lookup does not need one, and forcing a `Duration` on every reactive step would be noise. But a flow that talks to the network and declares no default is a flow one hung socket away from a leaked thread, and the javadoc should say exactly that.

## 2. The lane asymmetry

`ReactiveStep:47` has `<R> ReactiveStep<R, O> adaptMono(Function<T, Mono<R>> call, Duration budget)`.
`ReactiveLane:44` has only `<R> ReactiveLane<R> adaptMono(Function<T, Mono<R>> call)`.

So a remote call inside a `when`/`match` branch, or inside a `fork` body, cannot take a budget without dropping to `handleMono`. Pure omission — add the overload. `ReactiveMirrorTest` did not catch it because it only checks that core's methods have covariant overrides; it has nothing to say about the reactive-only methods being symmetric across the three mirrors.

**Also proposed**: extend `ReactiveMirrorTest` with a second assertion — every reactive-only method on `ReactiveStep` that is type-preserving must exist on `ReactiveLane` too. That is what would have caught this, and what will catch the next one.

## 3 and 4. `pipe`

```java
// DefaultReactiveFlow:72
return flux -> flux.flatMap(input -> pipeline.apply(input, just(input)).executeMono(), concurrency);
```

**`prefetch`.** Reactor's `flatMap` takes one; `pipe` does not expose it. For a slow pipeline with an eager source, the default prefetch buffers more than the caller may want. Add the overload; it costs a parameter.

**`concurrency` is not validated.** `pipe(0, …)` throws inside Reactor at *subscribe* time, i.e. at the first element, in a stack trace that mentions `FluxFlatMap`. Validate at build time with the message the caller needs.

**Per-element error isolation.** `ReactivePipeTest:83` pins today's behavior: *one failing element fails the whole stream* unless the pipeline itself recovers. For a request/response `Flux` that is right. For an ingestion loop — Kafka, SSE, a batch import — it is almost never what you want: one poison message stops the consumer.

```java
// ReactiveFlow
<R> Function<Flux<I>, Flux<R>> pipeResilient(int concurrency,
        BiFunction<I, ReactiveStep<I, O>, ReactiveStep<R, O>> pipeline,
        BiConsumer<I, Throwable> onElementError);
// → flatMap(input -> pipeline(...).executeMono()
//                      .onErrorResume(e -> { onElementError.accept(input, e); return Mono.empty(); }),
//            concurrency)
```

The element is reported and dropped; the stream survives. It is `flatMap` + `onErrorResume` and a user could write it — **naming it is the point**: whether one bad message kills the consumer should be a decision someone made, not a property they discovered in production.

## 5. BlockHound

`0002:461` says: *"BlockHound instruments all threads, so the example ships the one-line allowlist for `dev.nioflow` workers."* The example ships no BlockHound at all — the only two hits for the word in the repo are in the RFC itself.

Ship it: `blockhound` as a test dependency of the WebFlux example, plus the allowlist for the worker threads. It is the one **mechanical** proof that no blocking call reaches a Netty event loop, which is the central promise of the whole integration. Right now that promise rests on `OrderEndpointsTest:117` ("stages run on virtual workers, never on Netty") — a good test, but it observes thread names; BlockHound observes the actual park.

Alternatively: strike the sentence from `0002`. But the allowlist is the more valuable artifact, and it is a day of work.

## 6. The stale line in RFC 0002

`0002:499`, under Future work:

> `examples/springwebflux-with-nioflow` **is an empty Spring Boot skeleton today**. It becomes the executable version of this RFC: a controller returning `Mono`, a `Flux` ingestion endpoint, a `WebClient` stage, a fork, and the BlockHound allowlist.

Every item on that list exists today except the BlockHound allowlist (gap 5). The line is stale and it is the kind of staleness that makes a reader distrust the rest of the document. Fix it: mark the example done, and leave the allowlist as the one open item — which this RFC closes.

## Testing

- **The leak, pinned first**: a test that a `handleMono` over a never-completing `Mono` under `defaultBudget(50ms)` releases its worker — the pool serves N more requests afterwards. Without the default budget, that test hangs (which is the bug, and is why the test is written before the fix).
- The default budget reaches `recover()` as a `TimeoutException`, indistinguishable from an explicit one (`ReactiveMonoSemanticsTest:96` already pins that shape for the explicit case — reuse it).
- An explicit per-stage budget **overrides** the default; a stage with no network in it (`Mono.just`) is unaffected.
- `ReactiveLane.adaptMono(call, budget)` applies the budget inside a lane and inside a fork body.
- **`ReactiveMirrorTest` gains the step/lane symmetry assertion** — it must fail against today's `ReactiveLane` and pass after gap 2 is fixed.
- `pipe(0, …)` and `pipe(-1, …)` are rejected at build time with a message naming `concurrency`.
- `pipeResilient`: a poison element is reported to the handler and dropped, the stream completes with the remaining elements, and the engine reports the failure through its own `onError` handlers exactly once (not twice).
- BlockHound: the example's test suite runs with BlockHound installed and the `dev.nioflow` worker allowlist, and a deliberately-blocking `handle` stage does **not** trip it while a blocking call planted on a Netty thread **does**.

**Benchmarks**: none. Nothing here touches the engine or the hot path. `NioFlowBenchmark` must be flat, trivially.

## Risks

| Risk | Mitigation |
| --- | --- |
| **`defaultBudget` silently times out a stage that legitimately takes minutes** (a long export, a slow batch job). | Opt-in: a flow that declares no default keeps today's behavior exactly. And the javadoc states the trade plainly — no default means one hung socket leaks a worker for the life of the JVM. |
| `pipeResilient` swallows failures a caller wanted to see. | The `BiConsumer` is **mandatory** in the signature, not an optional overload — you cannot drop an element without being handed it. |
| BlockHound is famously fragile across JDK versions and instruments everything. | It lives in the **example's test scope only**, never in `core`, never in a consumer's runtime. If it breaks on a JDK bump, the example's tests break — not the library. |
