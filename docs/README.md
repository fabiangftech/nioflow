# What is nioflow

[![Maven Central](https://img.shields.io/maven-central/v/dev.nioflow/nioflow-core?label=Maven%20Central&color=blue)](https://central.sonatype.com/artifact/dev.nioflow/nioflow-core)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue)](https://www.apache.org/licenses/LICENSE-2.0)
[![Java](https://img.shields.io/badge/Java-21%2B-orange)](https://openjdk.org/)
[![Website](https://img.shields.io/badge/web-nioflow.dev-1a7f5a)](https://nioflow.dev)

**nioflow** is a Java library for building business pipelines as fluent, typed flows that run on an event-loop engine — and can be **edited while they run**.

```java
// The shared definition: what every request goes through first.
NioFlow<OrderRequest, Receipt> orders = DefaultNioFlow.from(OrderRequest.class)
        .handle("validate", validator::check)
        .background("audit", audit::record);

// Per request — thousands of these run concurrently on the same bean.
// The pipeline starts at the INPUT type and adapt is what re-types it:
Receipt receipt = orders.just(request)              // OrderRequest
        .adapt(pricing::price)                      // -> Order
        .when(order -> order.total() > 1_000)
        .then(lane -> lane.handle("review", risk::hold))
        .otherwise(lane -> lane.handle("approve", risk::fastPath))
        .adapt(Receipt::from)                       // -> Receipt
        .execute();
```

Every step is a **link** in an immutable chain. A pool of **boss threads** orchestrates each execution; your functions run on **virtual-thread workers**. The result is a pipeline that is:

- **Typed end to end** — `NioFlow<I, O>` is a promise: `just()` takes an `I` and the pipeline answers an `O`. The per-request builder starts at the input type and `adapt` is the only step that re-types it — so the compiler tells you the moment a pipeline does not deliver what the flow declares.
- **Editable at runtime** — `splice` single links or swap whole named **regions** atomically; in-flight requests keep their snapshot and never notice. [Runtime editing →](runtime-editing.md)
- **Resilient by composition** — rate limit → per-attempt timeout → retry → `recover()`, all native. [Resilience →](resilience.md)
- **Built for load** — stage fusion, batching, per-key ordering, backpressure, dedicated event loops. [Scaling →](scaling.md)
- **Zero required dependencies** — Resilience4j and OpenTelemetry are optional, compile-only integrations.

## Where it fits

nioflow sits between "a chain of service calls in a `@Service` method" and a full workflow engine. Reach for it when your logic has **shape** — branches, fan-outs, fallbacks, bulk steps — and that shape needs to **change without a redeploy**: pricing rules swapped by ops, a fraud gate tightened during an incident, a provider replaced behind a named stage.

```mermaid
flowchart LR
    A[Request] --> B[handle]
    B --> C{when}
    C -->|true| D[lane]
    C -->|false| E[lane]
    D --> F[batch]
    E --> F
    F --> G[Result]
```

## At a glance

| You need | nioflow gives you |
|---|---|
| Branching logic | `when()` / first-match-wins `match()` with nested forks |
| Bulk downstream calls | `batch(size, window, bulk)` — callers still get individual results |
| Per-entity ordering | `just(x).key(orderId)` — Kafka-partition style FIFO per key |
| Hot changes | `splice`, named regions, `replaceRegion` — atomic, validated |
| Protection | native `RateLimit`, `Retry`, timeouts, `recover()`, circuit breaker via Resilience4j |
| Visibility | `onComplete`/`onError` taps, metrics SPI, OpenTelemetry adapter |

## Install

Requires **Java 21+** (virtual threads and pattern matching over sealed types; developed and tested on JDK 25). No other runtime dependencies.

**Gradle**

```groovy
dependencies {
    implementation 'dev.nioflow:nioflow-core:1.0.0'
}
```

**Maven**

```xml
<dependency>
    <groupId>dev.nioflow</groupId>
    <artifactId>nioflow-core</artifactId>
    <version>1.0.0</version>
</dependency>
```

Ready? Head to the [Quickstart](quickstart.md).
