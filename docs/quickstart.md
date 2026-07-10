# Quick start

## Requirements

- **Java 25 or later** — nio-flow relies on virtual threads and scoped values.
- No other dependencies. Resilience4j and OpenTelemetry are optional integrations you add yourself when you use their adapters.

## Installation

**Gradle (Groovy DSL)**

```groovy
implementation 'dev.nioflow:core:1.0.0'
```

**Gradle (Kotlin DSL)**

```kotlin
implementation("dev.nioflow:core:1.0.0")
```

**Maven**

```xml
<dependency>
    <groupId>dev.nioflow</groupId>
    <artifactId>core</artifactId>
    <version>1.0.0</version>
</dependency>
```

## Your first flow

A nio-flow is built in three steps: **declare** the chain, **inject** values, **join**.

```java
import dev.nioflow.application.facade.DefaultNioFlow;

try (NioFlow<Integer> flow = new NioFlow<>()) {

    // 1. Declare the chain — once, before injecting values
    flow.handle(x -> x * 2)              // sync stage: runs on a handle worker
        .submit(x -> slowLookup(x))      // async stage: runs on the executor, engine moves on
        .handle(x -> x + 1)
        .seal();                         // freeze the chain: no more stages

    // 2. Inject values — each one walks the chain independently
    flow.just(1);
    flow.just(2);
    flow.just(3);

    // 3. Wait until every value finished
    int last = flow.join();              // result of the newest injected value
}
```

Key points:

- **`handle` is sync, `submit` is async** — that is the heart of the API. Both take a `Function<T, T>` and both can do IO (JDBC, HTTP, ...). `handle` runs the stage to completion on a handle worker — a virtual thread by default, so blocking there ties up only that value. `submit` hands the stage to the flow's executor and the engine *does not wait* — the result is reaped asynchronously and the value resumes later. Reach for `submit` when a stage should run on your own executor or needs a timeout with real cancellation.
- **Order is per value, never across values** — a fast value may overtake a slower one injected earlier. If a value fails, only that value stops.
- **`join()`** waits until the flow is quiescent and returns the result of the newest injected value that finished.
- **`seal()`** freezes the chain: declaring more stages throws, and finished values are released instead of retained. Seal every long-running flow.
- **`close()`** (or try-with-resources) drains in-flight values for up to 10 seconds, then stops the engine. Use `close(Duration)` to tune the grace period.

## Handling errors

Errors short-circuit only the failing value. You choose what happens next:

```java
try (NioFlow<String> flow = new NioFlow<>()) {
    flow.handle("parse", s -> parse(s))          // named stage: failures say where
        .onErrorResume(error -> "fallback")      // recover: value resumes from here
        .submit(s -> store(s))
        .onError(error -> log.warn("lost value", error))  // terminal failures only
        .seal();

    flow.just("ok");
    flow.just("boom");
    flow.join();
}
```

- **`onErrorResume(fallback)`** is a *recovery link*: when a value fails at any upstream stage, the fallback turns the error into a replacement value and the flow resumes from that point. Values flowing normally skip it.
- **`onError(handler)`** observes *terminal* failures — values that exhausted every recovery. Recovered values never reach it.
- **Named stages** (`handle("parse", ...)`, `submit("store", ...)`) wrap their failures in a `StageException` carrying the stage name, so errors are self-describing.
- **`join()` after a failure** rethrows it once (wrapped in a `CompletionException`) and then clears it, so the flow stays usable.

Declare the whole chain — recoveries included — **before** injecting values: a recovery only catches failures of links declared before it.

## Choosing an executor

The default constructor runs everything on virtual threads. For finer control:

```java
// Your executor for submit stages — you keep its lifecycle, close() won't touch it
NioFlow<Order> flow = new NioFlow<>(executor);

// Bound sync parallelism with a fixed handle-worker pool (CPU-heavy chains)
NioFlow<Order> flow = new NioFlow<>(executor, 8);

// Fully tuned: executor + handle workers + backpressure
NioFlow<Order> flow = new NioFlow<>(executor, 8, Backpressure.blocking(10_000));
```

> With a **fixed** handle-worker pool, keep `handle` stages fast and non-blocking — a blocked handle ties up a shared worker. With the default virtual workers there is no such restriction.

## Next steps

- [Examples](examples.md) — forks, batching, fan-out, resilience, backpressure and observability.
- [Architecture](architecture.md) — how the engine works and what it guarantees.
- [API reference](reference.md) — the full operator catalogue.
