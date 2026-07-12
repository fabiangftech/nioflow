# Spring Boot

nio-flow needs no starter: a flow is a singleton bean, and executions are per-request. This page mirrors the [example app](https://github.com/fabiangftech/nioflow/tree/main/examples/springboot-with-nioflow) in the repository.

## The flow bean

Declare the shared definition once. The bean's generic type documents input and output; `destroyMethod = "close"` drains the engine on shutdown:

```java
@Configuration
public class OrderFlowConfig {

    @Bean
    public NioEngine orderEngine() {
        return new DefaultNioEngine();
    }

    @Bean(destroyMethod = "close")
    public NioFlow<OrderRequest, OrderReceipt> orderFlow(NioEngine orderEngine,
                                                         PricingService pricing,
                                                         InventoryService inventory,
                                                         RiskService risk,
                                                         AuditService audit) {
        return DefaultNioFlow.from(OrderRequest.class, orderEngine)
                .filter(OrderRequest::isValid)
                .adapt(pricing::price)
                .handle("reserve", inventory::reserve,
                        Duration.ofSeconds(2), Retry.of(3, Duration.ofMillis(100)))
                .use("fraud-gate", fraudGate(risk))          // named region: swappable live
                .handle("tax", pricing::withTax)
                .background("audit", audit::record)
                .adapt(OrderReceipt::from);
    }

    private static Segment<Order, Order> fraudGate(RiskService risk) {
        return lane -> lane
                .handle("risk-score", risk::score)
                .when(order -> order.riskScore() > 80)
                .then(l -> l.handle("hold", risk::hold))
                .otherwise(l -> l.handle("clear", risk::clear));
    }
}
```

Exposing the engine as its own bean gives admin components a handle for [runtime edits](runtime-editing.md).

## Controllers

One bean, unlimited concurrent requests — each `just()` is an isolated execution:

```java
@RestController
public class OrderController {

    private final NioFlow<OrderRequest, OrderReceipt> orderFlow;

    OrderController(NioFlow<OrderRequest, OrderReceipt> orderFlow) {
        this.orderFlow = orderFlow;
    }

    @PostMapping("/orders")
    public CompletableFuture<OrderReceipt> create(@RequestBody OrderRequest request) {
        // Returning the future keeps the servlet thread free: non-blocking endpoint.
        return orderFlow.just(request).executeAsync();
    }

    @PostMapping("/accounts/{id}/movements")
    public CompletableFuture<OrderReceipt> move(@PathVariable String id,
                                                @RequestBody OrderRequest request) {
        // Same-account movements apply in arrival order.
        return orderFlow.just(request).key(id).executeAsync();
    }
}
```

## An admin endpoint for live edits

The operational superpower: change the running pipeline from an endpoint (secure it accordingly):

```java
@RestController
@RequestMapping("/admin/flow")
public class FlowAdminController {

    private final NioEngine orderEngine;
    private final DefaultNioFlow<OrderRequest, OrderReceipt> orderFlow;

    @PostMapping("/tax-rate")
    public String retax(@RequestParam double rate) {
        orderEngine.splice("tax", Splice.REPLACE, List.of(new Stage("tax",
                value -> ((Order) value).withTax(rate), false, null, null, List.of())));
        return "tax stage replaced: rate=" + rate;
    }

    @PostMapping("/fraud-gate/strict")
    public String tightenFraudGate() {
        orderFlow.replaceRegion("fraud-gate", lane -> lane
                .handle("risk-score", risk::score)
                .filter(order -> order.riskScore() < 50));   // whole region, one atomic swap
        return "fraud gate tightened";
    }

    @GetMapping("/chain")
    public List<String> chain() {
        return orderEngine.chain().stream()
                .map(link -> switch (link) {
                    case Stage s -> "stage:" + s.name();
                    case Batch b -> "batch:" + b.name();
                    case Background b -> "background:" + b.name();
                    case Recovery r -> "recovery:" + r.name();
                    case FanOut f -> "fanout:" + f.name();
                    case Decision d -> "decision:" + d.id();
                    case Filter f -> "filter";
                })
                .toList();
    }
}
```

In-flight requests are never affected by an edit — they finish on the chain snapshot they started with.

## Hardening for production

Optional, but recommended once the definition stabilizes: call `orderEngine.seal()` at the end of the config method. Sealing freezes appends, **validates every subsequent splice or region swap** (a broken edit is rejected and the running chain stays intact), and compiles a dispatch plan. Runtime editing keeps working — that is the point.

## Observability

Install the metrics SPI, or the OpenTelemetry adapter if `opentelemetry-api` is on your classpath:

```java
@Bean
public NioEngine orderEngine(Meter meter) {
    NioEngine engine = new DefaultNioEngine();
    engine.metrics(new OpenTelemetryMetrics(meter));
    return engine;
}
```

You get execution latency (completed/failed/filtered), per-stage latency, retries, recoveries, drops and queue depth. [Observability →](observability.md)
