# Spring Boot

nioflow needs no starter: a flow **is a bean**, and every request runs an isolated execution over it. This page is a tour of that integration, from the first endpoint to runtime edits, metrics and shutdown. Every example uses plain JDK types (`String`, `Integer`, `List`, `Map`) — swap them for your own domain types and nothing else changes.

A runnable version of most of this lives in [`examples/springboot-with-nioflow`](https://github.com/fabiangftech/nioflow/tree/main/examples/springboot-with-nioflow).

## The beans

A flow bean states a contract: `NioFlow<I, O>` takes an `I` in `just()` and its pipeline must reach an `O`. Declare **one bean per contract** and let Spring match them by field name:

```java
@Configuration
public class NioFlowConfig {

    @Bean(destroyMethod = "close")
    public NioFlow<String, String> textFlow() {
        return DefaultNioFlow.from(String.class);       // String in, String out
    }

    @Bean(destroyMethod = "close")
    public NioFlow<Integer, String> invoiceFlow() {
        return DefaultNioFlow.from(Integer.class);      // Integer in, String out
    }

    @Bean(destroyMethod = "close")
    public NioFlow<Integer, Integer> numberFlow() {
        return DefaultNioFlow.from(Integer.class);      // Integer in, Integer out
    }
}
```

Three things to notice:

- **Singletons on purpose.** A flow *is* the shared definition — that is what makes shared chains, batching and runtime edits possible — and it serves any number of concurrent requests, because every `just()` opens an isolated execution.
- **`from(Class)` keeps the type token**, so `just()` rejects a value that is not an `I` with a clear `IllegalArgumentException` instead of failing as a `ClassCastException` inside a worker.
- **`destroyMethod = "close"`** drains the engine when the context shuts down (see [Lifecycle](#lifecycle-and-shutdown)).

Then inject them by name:

```java
@Service
@RequiredArgsConstructor
public class SampleService {

    private final NioFlow<String, String> textFlow;
    private final NioFlow<Integer, String> invoiceFlow;
    private final NioFlow<Integer, Integer> numberFlow;
}
```

> **The wildcard shortcut, and what it costs.** `DefaultNioFlow.create()` opens a flow without naming the type, so a single `@Bean @Scope("prototype") NioFlow<?, ?>` can be injected into *any* typed field. It is convenient, and it is a loaded gun: Spring's generic matching is lenient enough to hand that bean to a `NioFlow<Integer, String>` field whose types are then fiction, and with no `Class` token `just()` cannot catch it — the mismatch surfaces later, inside the first stage that touches the value. Also, Spring does not call destroy methods on prototypes, so nothing drains the engine. Prefer a typed bean whenever you can name the type.

## Hello world

`just()` opens the per-request execution; `handle` transforms on a virtual-thread worker; `background` fires an effect nobody waits for:

```java
@Service
@RequiredArgsConstructor
public class SampleService {

    private final NioFlow<String, String> textFlow;

    public String greeting(String value) {
        return textFlow.just(value)
                .handle("greet", item -> item.concat(" World!"))
                .background("audit", item -> log.info("audited: {}", item))
                .execute();                       // blocks: executeAsync().join()
    }
}
```

```java
@RestController
@RequiredArgsConstructor
public class SampleController {

    private final SampleService sampleService;

    @GetMapping("/greeting")
    public String greeting(@RequestParam(defaultValue = "Hello") String value) {
        return sampleService.greeting(value);
    }
}
```

## Non-blocking endpoints

`executeAsync()` returns the `CompletableFuture`. Return it from the controller and the servlet thread is released immediately — the response is written when the pipeline finishes:

```java
public CompletableFuture<String> greetingAsync(String value) {
    return textFlow.just(value)
            .handleSync("trim", String::trim)       // pure CPU: inlined on the event loop
            .handle("greet", item -> "hello " + item)
            .onComplete(result -> log.info("completed: {}", result))
            .onError(error -> log.warn("failed: {}", error.getMessage()))
            .executeAsync();
}
```

```java
@GetMapping("/greeting-async")
public CompletableFuture<String> greetingAsync(@RequestParam String value) {
    return sampleService.greetingAsync(value);      // Spring MVC: async dispatch
}
```

On **WebFlux** the same future becomes a `Mono`:

```java
@GetMapping("/greeting-reactive")
public Mono<String> greetingReactive(@RequestParam String value) {
    return Mono.fromFuture(() -> sampleService.greetingAsync(value));
}
```

`onComplete` / `onError` on an execution scope to *that* execution and fire before the future completes. Declared on the **bean** instead, they observe every request — see [Observability](#observability).

## Input and output of different types

The per-request pipeline starts at the **input type**, and `adapt` is the only step that re-types it. The compiler follows it all the way to the method's return type:

```java
// NioFlow<Integer, String>: cents in, formatted invoice out.
public String invoice(int cents) {
    return invoiceFlow.just(cents)                                  // Integer
            .handle("apply-vat", amount -> amount * 121 / 100)      // still Integer
            .adapt(amount -> "EUR " + (amount / 100) + "." + String.format("%02d", amount % 100))
            .handle("stamp", text -> text + " (VAT included)")      // String from here on
            .execute();                                             // String
}
```

Drop the `adapt` and this does not compile: `execute()` would hand back an `Integer` and the method promised a `String`. That is the point of the two type parameters.

## Filtering, and telling a cut from a null

`filter` short-circuits the execution on purpose. `execute()` maps a cut to `null`, which is ambiguous; `executeResult()` gives you a sealed `FlowResult` that is not:

```java
public Optional<Integer> evenOnly(int value) {
    FlowResult<Integer> result = numberFlow.just(value)
            .filter(item -> item % 2 == 0)
            .handle("scale", item -> item * 10)
            .executeResult();

    return switch (result) {
        case FlowResult.Completed<Integer> completed -> Optional.of(completed.value());
        case FlowResult.Filtered<Integer> ignored -> Optional.empty();
    };
}
```

Which maps cleanly onto an HTTP status:

```java
@GetMapping("/even/{value}")
public ResponseEntity<Integer> evenOnly(@PathVariable int value) {
    return sampleService.evenOnly(value)
            .map(ResponseEntity::ok)
            .orElseGet(() -> ResponseEntity.noContent().build());   // 204: cut, not an error
}
```

## Branching: when and match

Forks are per-request and never touch the shared definition, so any number of them run concurrently. `when` is the two-lane form:

```java
public String review(int amount) {
    return numberFlow.just(amount)
            .when(item -> item > 1_000)
            .then(lane -> lane.handle("hold", item -> item - 50))       // large: fee
            .otherwise(lane -> lane.handle("fast-path", item -> item))
            .adapt(item -> "amount=" + item)
            .execute();
}
```

`match()` is **first-match-wins** — a value that hits the first case never evaluates the second — and whatever you chain after `otherwise` is the main line, running for every value:

```java
public String route(int amount) {
    return invoiceFlow.just(amount)
            .match()
            .is(item -> item > 1_000, lane -> lane.handle("manual-review", item -> item))
            .is(item -> item > 100,   lane -> lane.handle("auto-approve", item -> item))
            .otherwise(lane -> lane.handle("fast-path", item -> item))
            .adapt(item -> "order " + item)          // main line: every value
            .handle("stamp", order -> order + " [done]")
            .execute();
}
```

## Resilience per stage

The layers compose in this order: **rate limit → timeout per attempt → retry over attempts → `recover` as the final net.**

```java
private static final RateLimit PROVIDER_LIMIT = RateLimit.of(5, Duration.ofSeconds(1));

public String charge(String orderId) {
    return textFlow.just(orderId)
            .handle("charge", item -> gateway.charge(item),
                    Duration.ofMillis(300),                 // budget per attempt
                    Retry.of(3, Duration.ofMillis(50)))     // 3 attempts, 50ms backoff
            .handle("notify", item -> provider.send(item), PROVIDER_LIMIT)
            .recover("fallback", error -> orderId + " deferred: " + error.getMessage())
            .execute();
}
```

- The timeout is **per attempt**, not for the whole retry sequence.
- `recover` is **positional**: it catches failures from the links above it, and the execution continues below it with the recovered value.
- The rate limit parks a virtual thread, never the event loop. One `RateLimit` instance = one bucket: share the constant across every stage that hits the same dependency.

Recovery also lets a controller answer 200 with a degraded body instead of 500 — the failure never reaches the future.

## fanOut: several calls at once

The branches run concurrently on virtual workers and the join combines them **in declaration order**. Sequentially the three calls below would cost ~300ms; here they cost ~100ms:

```java
public String enrich(String customerId) {
    List<Function<String, String>> branches = List.of(
            item -> remoteCall("credit", item),
            item -> remoteCall("loyalty", item),
            item -> remoteCall("risk", item));

    return textFlow.just(customerId)
            .fanOut("enrich", branches, results -> String.join(" | ", results))
            .execute();
}
```

The join can also **re-type** the value, exactly like `adapt`. Here the three `String` results become a `Map`, which is what a `NioFlow<String, Map<String, String>>` bean promises to return:

```java
private final NioFlow<String, Map<String, String>> profileFlow;   // String in, Map out

public Map<String, String> profile(String customerId) {
    List<Function<String, String>> branches = List.of(
            item -> remoteCall("credit", item),
            item -> remoteCall("loyalty", item),
            item -> remoteCall("risk", item));

    return profileFlow.just(customerId)                    // String
            .fanOut("enrich", branches, results -> Map.of( // -> Map<String, String>
                    "credit", results.get(0),
                    "loyalty", results.get(1),
                    "risk", results.get(2)))
            .execute();                                    // Map<String, String>
}
```

> Because of Java's inference limits, always type the branches list explicitly (`List<Function<T, R>> branches = List.of(...)`) so the join lambda infers `List<R>`.

A branch failure fails the fan-out, and a downstream `recover` catches it like any other stage failure.

## Context: request-scoped scratch state

`handleContextual` hands a stage the typed per-execution `Context`, so trace ids, tenant ids and timings never have to be smuggled inside the value type:

```java
private static final Context.Key<String> TRACE_ID = Context.Key.of("traceId");

public String tracked(String value, String traceId) {
    return textFlow.just(value)
            .handleContextual("open-trace", (item, ctx) -> {
                ctx.put(TRACE_ID, traceId);
                return item;
            })
            .handle("call-provider", item -> provider.send(item))
            .handleContextual("close-trace", (item, ctx) -> item + " [trace=" + ctx.get(TRACE_ID) + "]")
            .execute();
}
```

The context is per execution, allocated lazily on the first `put`, and the engine serializes access across thread hops — no `ThreadLocal`, and nothing leaks between requests. A natural fit for the MDC value you pulled off the incoming request in a filter.

## The shared definition

Steps declared **on the bean** (not on a `just()` execution) run for every request, before the per-request pipeline. They are type-preserving — an `I` in, an `I` out — which is exactly what lets `just()` start at the input type. Declare them once at startup:

```java
@Service
@RequiredArgsConstructor
public class BulkService {

    private final NioFlow<String, String> bulkFlow;

    @PostConstruct
    void defineSharedPipeline() {
        bulkFlow.handle("normalize", String::trim)
                .background("audit", value -> log.info("received {}", value))
                // Coalescing NEEDS a shared link: without it there is nothing
                // for concurrent callers to pool into.
                .batch("bulk-store", 16, Duration.ofMillis(20),
                        values -> repository.insertAll(values));   // List<String> -> List<String>
    }

    public CompletableFuture<String> store(String value) {
        return bulkFlow.just(value).executeAsync();   // the batch is invisible from here
    }
}
```

Concurrent requests park at the batch link until 16 pile up (or 20ms pass), then **one** bulk call serves them all — and each caller still gets its own element back. The shutdown grace period must cover the window, since parked executions still count as in flight.

Named stages on the shared chain are also the anchors you [splice at runtime](#runtime-editing-from-an-admin-endpoint).

## Ordering by key

Executions sharing a key run **one at a time, in submission order**, even when the requests arrive concurrently. Distinct keys keep full parallelism:

```java
public CompletableFuture<Integer> ordered(String accountId, int delta) {
    return numberFlow.just(delta)
            .key(accountId)                         // per-key FIFO lane
            .handle("apply", item -> ledger.apply(accountId, item))
            .executeAsync();
}
```

This is head-of-line blocking **by design** — one slow execution delays the rest of its key, so bound the stages with timeouts. `key()` only exists on an execution, never on the shared definition.

## Segments: keeping a big config readable

A `Segment` is a reusable piece of pipeline. It is build-time only — the links are embedded as if you had written them inline, so it costs nothing at runtime:

```java
public static final Segment<String, String> NORMALIZE = lane -> lane
        .handleSync("trim", String::trim)
        .handle("lowercase", String::toLowerCase);

public String report(String value) {
    return textFlow.just(value)
            .use(NORMALIZE)                        // embedded inline
            .handle("stamp", item -> item + "!")
            .execute();
}
```

Segments compose (`lane.use(...)`) and work inside lanes, where they inherit the lane's guards. `use(name, segment)` on the shared definition names the embedded span as a **region** you can swap whole, atomically.

## Runtime editing from an admin endpoint

To edit a live chain you need the engine, so create it as its own bean and hand it to the flow:

```java
@Configuration
public class NioFlowConfig {

    @Bean(destroyMethod = "shutdown")
    public NioEngine pricingEngine() {
        return new DefaultNioEngine();
    }

    // Declared as DefaultNioFlow: replaceRegion lives on the implementation
    // (it is an operator's tool, not part of the per-request contract).
    @Bean(destroyMethod = "close")
    public DefaultNioFlow<Integer, Integer> pricingFlow(NioEngine pricingEngine) {
        DefaultNioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, pricingEngine);
        flow.handle("base-price", amount -> amount)
            .use("surcharge", lane -> lane.handle("fee", amount -> amount + 10))   // named region
            .handle("tax", amount -> amount * 121 / 100);
        return flow;
    }
}
```

Services keep injecting it as a `NioFlow<Integer, Integer>` — the implementation type is only what the admin endpoint needs.

Now an ops endpoint can change behaviour while traffic flows. In-flight requests keep the chain they snapshotted at submission; the next request sees the new one:

```java
@RestController
@RequiredArgsConstructor
public class AdminController {

    private final NioEngine pricingEngine;
    private final DefaultNioFlow<Integer, Integer> pricingFlow;

    /** Swap one stage: a new VAT rate, no redeploy. */
    @PostMapping("/admin/tax/{percent}")
    public void tax(@PathVariable int percent) {
        pricingEngine.splice("tax", Splice.REPLACE, List.of(
                new Stage("tax", value -> (Integer) value * (100 + percent) / 100,
                        false, null, null, List.of())));   // sync, timeout, retry, guards
    }

    /** Swap a whole named region — one atomic edit, one validation. */
    @PostMapping("/admin/surcharge/off")
    public void surchargeOff() {
        pricingFlow.replaceRegion("surcharge", lane -> lane.handle("no-fee", amount -> amount));
    }
}
```

Guard this endpoint like any other admin surface (Spring Security, a separate management port). Call `engine.seal()` after startup if you want every edit validated before it lands — a rejected edit leaves the running chain untouched. Full story in [Runtime editing](runtime-editing.md).

## Observability

Register the taps on the **bean** and they see every execution:

```java
@PostConstruct
void observe() {
    textFlow.onComplete(result -> log.debug("ok: {}", result))
            .onError(error -> log.warn("flow error", error));
}
```

For real metrics, install the SPI on the engine bean. Micrometer (already on the classpath in any Spring Boot app with Actuator) is a three-method adapter:

```java
@Configuration
@RequiredArgsConstructor
public class NioFlowMetricsConfig {

    private final NioEngine pricingEngine;
    private final MeterRegistry registry;

    @PostConstruct
    void install() {
        pricingEngine.metrics(new NioFlowMetrics() {
            @Override public void executionCompleted(long nanos) {
                registry.timer("nioflow.execution", "outcome", "completed")
                        .record(nanos, TimeUnit.NANOSECONDS);
            }
            @Override public void executionFailed(long nanos) {
                registry.timer("nioflow.execution", "outcome", "failed")
                        .record(nanos, TimeUnit.NANOSECONDS);
            }
            @Override public void stageCompleted(String stage, long nanos) {
                registry.timer("nioflow.stage", "stage", stage).record(nanos, TimeUnit.NANOSECONDS);
            }
            @Override public void stageRetried(String stage) {
                registry.counter("nioflow.stage.retried", "stage", stage).increment();
            }
        });
    }
}
```

Everything shows up in `/actuator/metrics` and Prometheus with no further wiring. If you export to OpenTelemetry instead, bring `opentelemetry-api` and use the ready-made adapter: `engine.metrics(new OpenTelemetryMetrics(meter))`. See [Observability](observability.md).

## Dedicated engines and backpressure

By default every flow created with `DefaultNioFlow.from(type)` shares one JVM-wide boss pool and one virtual-thread worker pool — the `commonPool()` model. For a latency-critical flow that should not queue behind a noisy neighbour, give it its own event loops; and bound the in-flight queue if a burst must be rejected rather than swallowed:

```java
@Bean(destroyMethod = "shutdown")
public NioEngine checkoutEngine() {
    return DefaultNioEngine.dedicated(4, 10_000, OverflowPolicy.FAIL);
}

@Bean(destroyMethod = "close")
public NioFlow<String, String> checkoutFlow(NioEngine checkoutEngine) {
    return DefaultNioFlow.from(String.class, checkoutEngine);
}
```

`OverflowPolicy` is `BLOCK` (park the producer), `DROP` (discard, reported to the error handlers) or `FAIL` (throw `RejectedExecutionException` — turn it into a 429 in a `@ControllerAdvice`). Admission happens *before* the execution starts. More in [Scaling & ordering](scaling.md).

## Lifecycle and shutdown

`close()` on the flow releases the engine, which is why the beans above declare `destroyMethod = "close"` (and the engine beans `destroyMethod = "shutdown"`). Engines built with the default constructor share the JVM-wide executors, so closing one flow never starves the others.

When you need a **graceful drain** — stop accepting work, let in-flight executions finish — ask the engine for it and check what was left:

```java
@PreDestroy
void drain() {
    int stillRunning = checkoutEngine.shutdown(Duration.ofSeconds(10));
    if (stillRunning > 0) {
        log.warn("{} executions did not finish within the grace period", stillRunning);
    }
}
```

New `call`/`inject` are rejected immediately; the grace period must cover the slowest stage — and any `batch` window, since parked executions still count as in flight.

## Testing

The flow bean is a normal bean, so the usual Spring test slices work unchanged:

```java
@SpringBootTest
@AutoConfigureMockMvc
class SampleControllerTest {

    @Autowired MockMvc mockMvc;

    @Test
    void greetingRunsThePipeline() throws Exception {
        mockMvc.perform(get("/greeting").param("value", "Hello"))
                .andExpect(status().isOk())
                .andExpect(content().string("Hello World!"));
    }

    @Test
    void anOddNumberIsFilteredOut() throws Exception {
        mockMvc.perform(get("/even/3")).andExpect(status().isNoContent());
    }
}
```

And a service test needs no container at all — build the flow the same way the config does:

```java
@Test
void invoiceReachesTheOutputType() {
    try (DefaultNioFlow<Integer, String> flow = DefaultNioFlow.from(Integer.class)) {
        SampleService service = new SampleService(flow);
        assertThat(service.invoice(1_000)).isEqualTo("EUR 12.10 (VAT included)");
    }
}
```

## Gotchas

| Symptom | Cause |
|---|---|
| `execute()` / `key()` do not exist | They live on the per-request pipeline, not on the bean. Open one with `just(input)`. |
| The pipeline does not compile at `return` | It never reached the flow's output type `O`. That is the contract working — add the `adapt`. |
| `IllegalArgumentException` in `just()` | The value is not an `I`. Usually a raw or wildcard flow injected into a typed field. |
| Wrong-type value blows up inside a stage | The bean came from `create()` (wildcards, no type token). Use `from(Class)`. |
| `batch` never coalesces | The batch was declared on a `just()` execution — it must live on the shared definition. |
| Engine still alive after shutdown | The bean is a prototype: Spring does not call destroy methods on those. |
| A `handleSync` stage stalls the app | It runs on the event loop. Keep it pure CPU and sub-microsecond, or use `handle`. |

## From here

- [Pipeline API](pipeline-api.md) — every step, in order
- [Runtime editing](runtime-editing.md) — splice stages and swap regions while traffic flows
- [Resilience](resilience.md) — timeouts, retries, rate limits and recovery
- [Scaling & ordering](scaling.md) — fusion, batching, keys, backpressure, dedicated loops
- [Observability](observability.md) — the metrics SPI and the OpenTelemetry adapter
