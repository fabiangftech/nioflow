# RFC 0004 — Streaming out: `executeFlux` and a bounded `adaptFlux`

- **Status**: Proposed
- **Target**: `infrastructure.reactive` only (no `core` change, no new link)
- **Depends on**: RFC 0002
- **Independent of**: every other proposed RFC (0003, 0005, 0006, 0007)

## Summary

`adaptFlux` collects a whole `Flux` into a `List` with **no cap of any kind**:

```java
// DefaultReactiveStep:67
delegate.adapt(value -> Blocking.await(call.apply(value).collectList()))
```

A `Flux` of ten million rows is a `List` of ten million rows. The failure mode is an `OutOfMemoryError` inside a stage, which fails the execution — and, because the heap is gone, probably fails everything else in the JVM too. RFC 0002 documented this (`0002:454`: *"a big stream stays a `Flux`"*), but **documentation is not a bound**, and the method with no cap is the one that autocompletes.

Two additions, both in `infrastructure.reactive`, neither touching the engine:

1. **a bounded `adaptFlux`** that fails fast and loudly instead of dying by OOM;
2. **`executeFlux`** — a streaming terminal, so the *right* answer has a name at the call site where the wrong one currently sits.

The model's line stays exactly where RFC 0002 drew it — **a nioflow value is one object** — but the pipeline no longer has to end *before* the stream in order to respect it.

## Design

### 1. The bounded `adaptFlux`

```java
// ReactiveStep / ReactiveLane
<R> ReactiveStep<List<R>, O> adaptFlux(Function<T, Flux<R>> call, int maxItems);
```

Implementation is `call.apply(v).take(maxItems + 1L).collectList()`, then throw if the list came back longer than `maxItems`:

```java
throw new FlowOverflowException(
        "adaptFlux(" + maxItems + ") — the stream produced more than " + maxItems + " items");
```

`take(maxItems + 1)` **cancels the source** as soon as the bound is exceeded, so the overrun costs one extra element, not the rest of the stream. The failure is an ordinary stage failure: it reaches `recover()` like any other, and the caller decides.

The un-capped overload **stays** (removing it would break every existing call site), with a javadoc that names this one and says why. It is the *unbounded* variant that becomes the one you have to opt into deliberately.

### 2. `executeFlux` — the streaming terminal

```java
// ReactiveStep
<R> Flux<R> executeFlux(Function<T, Flux<R>> tail);
// → executeMono().flatMapMany(tail)
```

It is deliberately tiny — a user could write `flatMapMany` themselves. It exists so that the correct answer is *reachable by autocomplete from the same place as the wrong one*. The engine's part is one value; the stream's part is Reactor's, and nothing gets buffered:

```java
@GetMapping(value = "/orders/{id}/events", produces = TEXT_EVENT_STREAM_VALUE)
Flux<Event> events(@PathVariable String id) {
    return orders.just(id)
            .handle("load", repo::findById)                   // engine: policy, recovery,
            .filter(order -> order.visibleTo(caller()))       // metrics, key, retry — one value
            .executeFlux(order -> events.stream(order.id())); // Reactor: the unbounded part
}
```

The semantics it inherits, and each is a test:

- **lazy** — nothing runs until the `Flux` is subscribed (`executeMono()` is already a `defer`/supplier);
- **one execution per subscription** — so `.retry()` on the `Flux` re-runs the pipeline, like it does on the `Mono`;
- **a `filter()` cut → an empty `Flux`** — the cut completes the execution with no value, `executeMono()` is empty, and `flatMapMany` over an empty `Mono` is an empty `Flux`. The three notions of "nothing" line up, so `switchIfEmpty` works as a 404 exactly as it does today;
- **a pipeline failure → `onError` on the `Flux`**, before the tail is ever subscribed;
- **cancellation of the `Flux`** cancels the tail (Reactor's business) and, once RFC 0007 lands, the execution too.

### What stays a non-goal

**A true 1:N `Link`** — an execution that emits many values — remains out, for RFC 0002's reason and not a new one: it is a different engine. One value in, one value out is what makes the decision bitset, the recovery positions, the batch grouping and the key lane mean anything. `executeFlux` does not bend that rule; it just stops the rule from forcing the pipeline to end early.

## Testing

`core/` (reactor is `testImplementation`), new `ReactiveStreamingTest`:

- **bounded `adaptFlux`**: a stream under the cap collects normally; a stream over the cap fails with `FlowOverflowException`; **the source is cancelled at `maxItems + 1`** (a stub counts the elements it was asked for, so "it does not drain the whole stream" is asserted, not assumed);
- the overflow failure is caught by `recover()` like any other stage failure;
- the un-capped overload behaves exactly as it does today (no regression);
- **`executeFlux` is lazy**: assembling it without subscribing leaves the engine's execution counter at zero;
- one execution per subscription; `.retry(2)` after a failing stage produces three executions;
- a `filter()` cut yields an empty `Flux` (`StepVerifier … verifyComplete()`), and `switchIfEmpty` fires;
- a pipeline failure reaches `onError` and the tail is **never subscribed** (a stub asserts it);
- the tail streams — a `Flux` of 100 000 elements passes through `executeFlux` without the heap growing by 100 000 elements (which is the whole point, and the one thing `adaptFlux` can never do);
- `ReactiveMirrorTest` passes: the new steps have their covariant overrides on `ReactiveStep`/`ReactiveLane`.

**Benchmark** (`tests/`): none needed. Neither addition touches the engine or the hot path — `executeFlux` is one `flatMapMany` on the terminal, and the bounded `adaptFlux` is a `take` on a stream that was already being collected. `NioFlowBenchmark` must be flat, which it will be trivially.

## Risks

| Risk | Mitigation |
| --- | --- |
| **The un-capped `adaptFlux` survives**, so the OOM is still reachable. | It has to survive — removing it breaks every existing call site. But its javadoc now names the bounded overload, `FlowOverflowException` is a documented type, and `docs/webflux.md` gets the rule: *a nioflow value is one object; if you cannot name a bound, do not collect it*. |
| Users reach for `executeFlux` when they wanted `pipe` (a `Flux` **in**, not a `Flux` **out**). | They are opposite directions and the javadoc of each points at the other. `pipe` = many inputs through one pipeline; `executeFlux` = one input, a streaming tail. |
| `FlowOverflowException` is a new public exception type for one method. | It is one class, and the alternative — an `IllegalStateException` a caller cannot distinguish from a user bug — is worse in a `recover()`. |
