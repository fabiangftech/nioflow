# Spring Boot

nio-flow needs no starter: the flow is a singleton bean declared once, and every request runs an isolated execution over it.

## The base bean

```java
@Configuration
public class NioFlowConfig {

    @Bean(destroyMethod = "close")   // drains the engine on shutdown
    public NioFlow<OrderRequest, Order> orderFlow() {
        return DefaultNioFlow.from(OrderRequest.class);
    }
}
```

The bean's generic type documents the contract: it accepts `OrderRequest` and leaves the value as `Order`. That final type is what per-request steps see next.

## Lanes in a service

One bean serves every business case: a service opens an execution with `just()` and routes it through its own lanes. These per-request forks never touch the shared definition — any number of them run concurrently:

```java
@Service
public class OrderService {
    
    ...
    private final NioFlow<OrderRequest, Order> orderFlow;
    ...
    
    public CompletableFuture<Order> place(OrderRequest request) {
        return orderFlow.just(request)
                .filter(validation::isValid)
                .adapt(pricing::price)                 // OrderRequest -> Order
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

`match()` is first-match-wins: a priority order never evaluates the bulk case. The lanes operate on `Order` — the type where the shared chain left the value — and the shared steps (validation, pricing, tax, audit) always run first.

That is the whole integration. From here:

- [Runtime editing](runtime-editing.md) — splice stages and swap regions of the bean's chain while it serves traffic
- [Resilience](resilience.md) — timeouts, retries, rate limits and recovery on any stage
- [Observability](observability.md) — metrics SPI and the OpenTelemetry adapter
