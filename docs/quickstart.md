# Quickstart

## Requirements

- **Java 21+** (nio-flow uses virtual threads and pattern matching over sealed types; developed and tested on JDK 25)
- No other runtime dependencies

## Install

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

## Your first flow

A flow is declared once — usually at startup — and executed many times, once per request:

```java
import dev.nioflow.application.facade.DefaultNioFlow;
import dev.nioflow.core.facade.NioFlow;

NioFlow<String, Integer> flow = DefaultNioFlow.from(String.class)
        .handle("trim", String::trim)          // named stages are editable later
        .filter(s -> !s.isEmpty())             // cut the flow: execute() returns null
        .adapt(String::length);                // re-types the pipeline: String -> Integer

Integer length = flow.just("  hello  ").execute();   // 5
```

Three things just happened:

1. `from(String.class)` anchored the **input type** `I = String`. From here, `adapt` is the only step that changes the value's type `T`.
2. Each builder call appended a **link** to the engine's chain. The chain stays open: you can keep appending, or edit it live — see [Runtime editing](runtime-editing.md).
3. `just(input)` opened an isolated, per-request **execution** over a snapshot of the chain, and `execute()` ran it.

## Blocking, async, or explicit

```java
Integer v  = flow.just(" a ").execute();               // blocks (executeAsync().join())
CompletableFuture<Integer> f = flow.just(" a ").executeAsync();  // never blocks

// Was that null a filter cut, or a real null? executeResult() can tell:
switch (flow.just("   ").executeResult()) {
    case FlowResult.Completed<Integer> c -> use(c.value());
    case FlowResult.Filtered<Integer> ignored -> log.info("filtered out");
}
```

Return the `CompletableFuture` from a controller and the endpoint is non-blocking — see [Spring Boot](spring-boot.md).

## Per-request steps

Executions can add their own links on top of the shared definition — they never touch it:

```java
flow.just("  hi  ")
    .handle(s -> s + "!")     // this link exists only for THIS execution
    .execute();
```

That is why one flow bean serves any number of concurrent requests: each `just()` is share-nothing.

## Where to go next

- [Pipeline API](pipeline-api.md) — every link type, with examples
- [Runtime editing](runtime-editing.md) — the feature the library is named for
- [Spring Boot](spring-boot.md) — beans, controllers, admin endpoints
