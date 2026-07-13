# Quickstart

## Requirements

- **Java 21+** (nioflow uses virtual threads and pattern matching over sealed types; developed and tested on JDK 25)
- No other runtime dependencies

## Install

**Gradle**

```groovy
dependencies {
    implementation 'dev.nioflow:nioflow-core:1.1.0'
}
```

**Maven**

```xml
<dependency>
    <groupId>dev.nioflow</groupId>
    <artifactId>nioflow-core</artifactId>
    <version>1.1.0</version>
</dependency>
```

## Your first flow

A flow is declared once — usually at startup — and executed many times, once per request:

```java
import dev.nioflow.application.facade.DefaultNioFlow;
import dev.nioflow.core.facade.NioFlow;

// Declared once: takes a String, answers an Integer.
NioFlow<String, Integer> flow = DefaultNioFlow.from(String.class)
        .handle("trim", String::trim)          // named stages are editable later
        .filter(s -> !s.isEmpty());            // cut the flow: execute() returns null

// Per request: the pipeline starts at the INPUT type.
Integer length = flow.just("  hello  ")        // String
        .adapt(String::length)                 // -> Integer
        .execute();                            // 5
```

Three things just happened:

1. `from(String.class)` anchored the **input type** `I = String`, and the `Integer` in the flow's type is the **output** its pipelines must reach.
2. The shared definition (`handle`, `filter`) is **type-preserving**: it takes a String and leaves a String. That is what lets `just()` hand you a builder that starts at the input type — `adapt` is the step that re-types the value, and the compiler follows it from there.
3. `just(input)` opened an isolated, per-request **execution** over a snapshot of the chain, and `execute()` ran it. Drop the `adapt` and this does not compile: `execute()` would give you a String, not the Integer the method wants.

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
