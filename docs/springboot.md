# Spring Boot

nio-flow needs **no Spring adapter and no extra dependency**: a flow is a plain `AutoCloseable` bean, `call` returns a `CompletableFuture` that WebMVC resolves natively (and WebFlux wraps in one line), and scopes and runtime edits let a single shared instance serve every controller. Just add the core dependency and wire beans.

The one rule that matters: **controllers never declare stages on the shared flow** — a `handle(...)` inside a request handler appends a permanent link on every call, so the chain grows with each request. Either declare the chain once in configuration, or use a [scope](#one-empty-flow-stages-at-the-call-site) so the stages you declare are private to that call.

## One shared flow, chain declared once

The classic setup: the chain is declared (and sealed) in configuration, controllers only inject.

```java
@Configuration
class NioFlowConfig {

    @Bean(destroyMethod = "close")
    NioFlow<Order> orderFlow() {
        var flow = new DefaultNioFlow<Order>();
        flow.handle("validate", o -> validate(o))
            .submit("price", o -> price(o))
            .onErrorResume(error -> Order.rejected(error))
            .seal();                            // accidental appends now throw
        return flow;
    }
}
```

```java
@RestController
class OrderController {

    private final NioFlow<Order> flow;          // the same bean in every controller

    OrderController(NioFlow<Order> flow) {
        this.flow = flow;
    }

    @PostMapping("/orders")
    CompletableFuture<Order> create(@RequestBody Order order) {
        return flow.call(order, Duration.ofSeconds(2));
    }
}
```

- **`call` resolves this request's own value** — many requests are in flight concurrently and each future completes with its own result. Returning the `CompletableFuture` keeps the servlet thread free; Spring MVC handles it natively.
- **Never `join()` in a controller** — it waits for the *whole* flow to go quiescent and returns the newest value's result, so under concurrent traffic it blocks the thread and can hand you another request's result.
- **Prefer the `timeout` variants** — they bound the caller's wait whatever happens to the value (slow stage, dropped by a `filter`).
- A request whose value is dropped by a `filter` gets a **cancelled** future; a value that fails past every recovery gets a **failed** one — map both in an `@ExceptionHandler` if you want custom status codes.

### WebFlux

Same bean, wrapped cold so the call fires per subscription:

```java
@GetMapping("/orders/{id}")
Mono<Order> find(@PathVariable String id) {
    return Mono.defer(() -> Mono.fromFuture(flow.call(new Order(id), Duration.ofSeconds(2))));
}
```

## Editable routes at runtime

To re-declare parts of the chain while the application runs — feature routes, tenant pipelines, plugins — use `release()` instead of `seal()` (finished values are still released, but the chain stays editable) and anchor the editable part on a **named stage**:

```java
@Bean(destroyMethod = "close")
NioFlow<Event> eventFlow() {
    var flow = new DefaultNioFlow<Event>();
    flow.handle("routes", e -> e)               // the editable anchor
        .submit("store", e -> store(e))
        .release();                             // flat memory, chain stays open
    return flow;
}
```

Any component — an admin endpoint, a config listener — can then swap the whole routing block atomically; values already in flight finish on the version they entered:

```java
@PostMapping("/admin/routes")
void reroute() {
    flow.replace("routes", f -> f.match()
            .is(e -> e.isBilling(), lane -> lane.submit(e -> billing(e)))
            .is(e -> e.isAudit(),   lane -> lane.handle(e -> audit(e))));
}
```

A later `replace("routes", ...)` swaps the whole previous segment (not just one link), and `remove("routes")` takes all of it out. Edits are engine-locked — safe from any thread. See [runtime editing](reference.md#runtime-editing).

## One empty flow, stages at the call site

When each endpoint wants its *own* mini-pipeline, keep the bean empty and open a **scope** per call: the stages you declare are private to that scope, concurrent requests never see each other, and nothing accumulates on the shared chain.

```java
@Bean(destroyMethod = "close")
NioFlow<String> flow() {
    return new DefaultNioFlow<>();              // empty: no handlers, no chain
}
```

```java
@GetMapping("/greeting")
public ResponseEntity<?> greeting() {
    return ResponseEntity.ok(flow.scoped()
            .just("Hello")
            .handle("greeting", s -> s + ", World!")
            .join());                           // waits for this scope's values only
}
```

Inside a scope, `join()` is fine: it flushes the scope's buffered values through its private chain and waits for those values only. Scopes ride the shared engine — threads, executor, backpressure — and their values are released on finish, so memory stays flat with no `seal()` needed. The async style works too:

```java
@PostMapping("/quotes")
CompletableFuture<Quote> quote(@RequestBody Request request) {
    return flow.scoped()
            .handle(r -> normalize(r))
            .submit(r -> priceRemote(r))
            .adapt(r -> toQuote(r))
            .call(request, Duration.ofSeconds(2));
}
```

## Lifecycle and tips

- **Shutdown**: `@Bean(destroyMethod = "close")` drains in-flight values for up to 10 s, then stops the engine — and never touches an executor you supplied. (Spring also infers `close` for `AutoCloseable` beans; being explicit documents the intent.)
- **Which mode for the shared bean?** `seal()` when the chain is fixed — loud failure on accidental mutation. `release()` when you need runtime edits. Neither, plus `scoped()`, when the bean is just a shared engine.
- **Observability is global**: register `metrics(...)`/`trace(...)` on the bean at configuration time; they throw on a scope by design.
- Closing a scope (or try-with-resources around one) never stops the shared engine.
- One bean per distinctly-typed flow is the natural layout; scopes cover the ad-hoc cases.
