# Spring Boot

nioflow needs no starter: a flow is a bean, and every request runs an isolated execution over it.

## The base bean

One bean serves every flow in the application. `DefaultNioFlow.create()` opens a root flow without naming the type, so the wildcards let Spring hand it to whatever typed field asks for it; `prototype` gives each injection point its own flow (and therefore its own chain):

```java
@Configuration
public class NioFlowConfig {

    @Bean
    @Scope("prototype")
    public NioFlow<?, ?> nioFlow() {
        return DefaultNioFlow.create();
    }
}
```

Then declare the contract where you use it — the field's generics say what goes in and what comes out:

```java
@Service
@RequiredArgsConstructor
public class SampleService {

    private final NioFlow<String, String> greetingFlow;
    private final NioFlow<Integer, String> creditFlow;   // Integer in, String out
}
```

The pipeline **starts at the input type**, and `adapt` is what takes the value to the output the flow promises:

```java
public String credit(int cents) {
    return creditFlow.just(cents)                // Integer
            .handle("charge", item -> item * 2)  // still an Integer here
            .adapt(item -> "EUR " + item)        // -> String
            .execute();                          // String
}
```

Drop the `adapt` and this does not compile: `execute()` would hand back an Integer, and the method promised a String. That is the compiler telling you the pipeline has not reached the flow's output type yet.

> **The trade-off of the wildcard bean.** `create()` gives up the `Class` token, so `just()` cannot check that the value it receives really is an `I` — and Spring's generic matching is lenient enough to inject this bean into *any* `NioFlow<X, Y>` field. A mismatch surfaces later, inside the first stage that touches the value. When you can name the type, prefer a typed bean: `DefaultNioFlow.from(Integer.class)` keeps the token, rejects a wrong input at `just()` with a clear message, and lets you use `destroyMethod = "close"` to drain the engine on shutdown (Spring does not call destroy methods on prototypes).

## Lanes in a service

A service opens an execution with `just()` and routes it through its own lanes. These per-request forks never touch the shared definition — any number of them run concurrently:

```java
@Service
@RequiredArgsConstructor
public class OrderService {

    private final NioFlow<OrderRequest, Order> orderFlow;

    public CompletableFuture<Order> place(OrderRequest request) {
        return orderFlow.just(request)                 // OrderRequest
                .filter(validation::isValid)
                .adapt(pricing::price)                 // -> Order
                .handle("tax", pricing::withTax)
                .background("audit", audit::record)
                .match()
                .is(Order::isPriority, lane -> lane
                        .handle("expedite", shipping::expedite)
                        .background("notify-vip", shipping::notifyVip))
                .is(Order::isBulk, lane -> lane
                        .handle("split-shipments", shipping::split))
                .otherwise(lane -> lane
                        .handle("standard", shipping::standard))
                .executeAsync();     // return it from a controller: non-blocking endpoint
    }
}
```

`match()` is first-match-wins: a priority order never evaluates the bulk case. Note how the pipeline starts at the `OrderRequest` and the `adapt` is what turns it into the `Order` the lanes work with — and the `Order` the field promised.

## The shared definition

Steps declared on the **flow itself** (not on a `just()` execution) run for every request, before the per-request pipeline. They are type-preserving — they take an `I` and leave an `I` — which is exactly what lets `just()` start at the input type:

```java
@PostConstruct
void defineSharedPipeline() {
    // Coalescing needs a shared link: without it there is nothing for
    // concurrent callers to pool into.
    bulkFlow.batch("bulk-store", 16, Duration.ofMillis(10), values ->
            repository.insertAll(values));
}
```

This is also what makes [runtime editing](runtime-editing.md) possible: named stages on the shared chain are the anchors you splice and the regions you swap while traffic flows.

From here:

- [Runtime editing](runtime-editing.md) — splice stages and swap regions of the shared chain while it serves traffic
- [Resilience](resilience.md) — timeouts, retries, rate limits and recovery on any stage
- [Observability](observability.md) — metrics SPI and the OpenTelemetry adapter
