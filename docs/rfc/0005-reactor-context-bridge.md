# RFC 0005 — The Reactor context bridge, declared once

- **Status**: Implemented (`ReactiveFlow.propagate`, `ReactiveContextTest`)
- **Target**: `infrastructure.reactive`, plus one `core` terminal — see *What the implementation changed*
- **Depends on**: RFC 0002 (`NioStep.with(Context.Key, value)`, already shipped)
- **Independent of**: every other proposed RFC (0003, 0004, 0006, 0007)

## Summary

Seeding the per-execution `Context` from Reactor's subscriber context works today, and it is boilerplate at every call site:

```java
return Mono.deferContextual(view -> orders.just(id)
        .with(TRACE, view.get("traceId"))                        // ← per controller method
        .handleContextual("charge", (order, ctx) -> psp.charge(order, ctx.get(TRACE)))
        .executeMono());
```

The tell that this is too much ceremony: **the WebFlux example does not do it.** `OrderService.java:31` takes `traceId` as a *method parameter* from the controller and threads it by hand. The documented pattern (`0002:423`, `docs/webflux.md:110`) exists only in the docs.

Proposed: declare the keys once, on the flow, and let `executeMono()` seed them on every subscription.

```java
// on ReactiveFlow
ReactiveFlow<I, O> propagate(Context.Key<?>... keys);

@Bean(destroyMethod = "close")
ReactiveFlow<String, Receipt> orders() {
    return Reactive.flow(DefaultNioFlow.from(String.class))
                   .propagate(TRACE, TENANT);      // ← once, in the config
}
```

and the controller writes nothing:

```java
@PostMapping("/orders/{id}/pay")
Mono<Receipt> pay(@PathVariable String id) {
    return orders.just(id)
            .handleContextual("charge", (order, ctx) -> psp.charge(order, ctx.get(TRACE)))
            .adapt(Receipt::of)
            .executeMono();          // TRACE and TENANT are already in the context
}
```

## Why it is cheap

`Context.Key` is **name-based** — `record Key<T>(String name)` (`Context.java:25`), and its javadoc already says the keys interoperate with the `Map<String, Object>` handed to `engine.call`. A Reactor subscriber context is keyed by arbitrary objects, in practice by `String`. The two line up because of a design decision taken two RFCs ago, and this RFC is mostly cashing it in.

The mechanics are the manual pattern, hoisted into the terminal:

```java
// DefaultReactiveStep.executeMono(), when the flow declared keys
if (config.keys().isEmpty()) {
    return Mono.fromFuture(delegate::executeAsync);      // declared nothing: unchanged
}
return Mono.deferContextual(view -> Mono.fromFuture(() -> delegate.executeAsync(seed(view))));

// seed(view): the declared keys the subscriber context actually carries, and only them
```

A key absent from the subscriber context is simply not seeded — no throw, no null in the map. The context map stays lazy, so a flow that declares no keys, or an execution whose keys are all absent, allocates nothing. That is the acceptance bar: **`propagate()` unused must cost exactly zero** — measured, see *Numbers*.

Note the laziness constraint: the seeding must happen **inside** the `deferContextual`, per subscription. Seeding at assembly time would share one value across every subscription of the same `Mono` — the same eager-vs-lazy trap `executeMono()` exists to avoid (`0002:441`).

## What the implementation changed

**The premise above — that `with()` can express one subscription's context — was false**, and it is the only thing this RFC got wrong. `NioStep.with()` is a *builder step*: it writes into the pipeline's state and returns `this` (`ExecutionNioFlow:214`). Seeding through it per subscription would have meant two subscriptions of the same `Mono` racing on one map, and a key that one carries and the other does not lingering into the second run — precisely the two properties this RFC set out to guarantee.

So core grew **one terminal**, and the reactive package is still where all the policy lives:

```java
// NioStep — next to executeAsync()
CompletableFuture<T> executeAsync(Map<String, Object> context);   // the context of THIS run
```

The run's entries are merged with the pipeline's own seed into a fresh map per run; `with()` wins on a name they share (it is the one the pipeline declared), and null/empty is exactly `executeAsync()` — no map is created. `Context.Key` being name-based means **no cast anywhere**: the subscriber context's `Object` value goes into a `Map<String, Object>` the engine already knows how to take.

The rest is the flow's state: the propagated keys ride along with the default budget in one `ReactiveConfig`, into `just()`'s pipeline, into a branch's lane and into a fork's segment — so what the flow declared once holds everywhere the chain goes.

## Numbers

`ReactiveBenchmark`, JDK 25, 2 forks × (5 warmup + 8 measured) iterations, `-prof gc`. Before = `HEAD` in a worktree, after = this branch:

| | throughput | allocation |
| --- | --- | --- |
| `monoOverhead` — the flow declares no keys — **before** | 55.4 ± 3.2 ops/ms | 1049.5 B/op |
| `monoOverhead` — the flow declares no keys — **after** | 54.5 ± 3.8 ops/ms | 1048.4 B/op |
| `contextPropagated` — one key, seeded per subscription | 52.9 ± 2.5 ops/ms | 1368.3 B/op |
| `contextSeededByHand` — the `deferContextual`/`with()` dance it replaces | 50.9 ± 10.4 ops/ms | 1436.4 B/op |

**The acceptance bar holds:** a flow that declares no keys allocates the same 1 048 B it allocated before `propagate()` existed (the byte-for-byte match is the honest metric here; the throughput scores of this suite overlap inside their error bars). The unused path is one branch on an empty list — no `deferContextual`, no map.

**And the bridge is not a tax:** declared and seeded, it runs at parity with the hand-written dance it replaces and allocates 68 B/op *less* than it. Against seeding nothing at all it costs ~320 B/op and ~3 % throughput — one seed map, one merged copy, one `deferContextual` — which is what a trace id crossing the boundary costs however you write it.

## What this deliberately does NOT do

**No `reactor.core.publisher.Hooks`. No Micrometer `ContextPropagation`.** Making the bridge fully automatic is possible and it is the wrong trade: implicit context propagation across a thread hop is how you get an MDC that is right 99 % of the time and silently wrong during the incident you bought it for.

The line this RFC draws: **declared-and-automatic, never discovered-and-automatic.** A reader of `NioFlowConfig` can see exactly what crosses the boundary, and nothing crosses that a person did not write down. RFC 0002 put it well (`0002:433`): *"explicit, cheap, and honest."* `propagate()` keeps all three — it just stops charging the honesty to every controller method.

**No write-back.** Nothing in the reactive package writes into the subscriber context on the way out, and this RFC adds nothing. A stage that wants to publish something publishes it in the value.

## Testing

`core/` (reactor is `testImplementation`), in `ReactiveFlowTest` or a new `ReactiveContextTest`:

- a declared key present in the subscriber context is seeded, and a `handleContextual` reads it;
- a declared key **absent** from the subscriber context leaves the context untouched (no null entry) and the stage sees `null` from `ctx.get`, not an exception;
- an **undeclared** key in the subscriber context does not cross — the bridge is a whitelist, and this test is what makes that a contract;
- **per subscription, not per assembly**: two subscriptions of the same `Mono` under two different subscriber contexts get two different values (the eager trap);
- `.retry()` on the Mono re-seeds from the context on each attempt;
- a flow that declares nothing allocates no context map (the existing lazy-init assertion, extended);
- `propagate()` composes with `with()` — an explicit `with()` on the pipeline wins over a propagated key of the same name (last write wins, and it is the caller's).

The example (`examples/springwebflux-with-nioflow`) drops the hand-threaded `traceId` parameter from `OrderService` and uses `propagate(TRACE)` — which is the real acceptance test: **the boilerplate the example was avoiding stops existing.**

## Risks

| Risk | Mitigation |
| --- | --- |
| **`propagate()` makes context seeding implicit** — the very thing RFC 0002 argued against. | Implicit *per subscription*, but the keys are **declared** at the flow. Declared-and-automatic is the line; `Hooks`/`ContextPropagation` stays out precisely because it crosses it. |
| A key is declared but the subscriber context uses a different name, so it silently never arrives. | The whitelist test above makes absence a defined behavior (no seed, `ctx.get` → null) rather than a mystery, and the javadoc names the `String` correspondence. A "strict" mode that throws on a missing declared key is future work if anyone wants it. |
| Type safety: `Context.Key<T>` is typed, the Reactor context is `Object`-keyed. | There is no cast: keys are name-based, so the value travels as an `Object` in the same `Map<String, Object>` the engine already accepts from `engine.call`. A type mismatch surfaces where a mis-seeded `engine.call` map would surface it — at the `ctx.get` that reads it — and the javadoc names the `String` correspondence that produces it. |
