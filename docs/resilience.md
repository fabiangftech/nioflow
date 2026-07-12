# Resilience

nio-flow composes protection in **layers**, from admission to last resort:

```mermaid
flowchart LR
    RL[RateLimit<br/>gates admission] --> TO[Timeout<br/>per attempt] --> RT[Retry<br/>over attempts] --> RC["recover()"<br/>final net]
```

All of it is native — zero dependencies. Circuit breaker and bulkhead stay external, through the optional [Resilience4j adapter](#resilience4j-circuit-breaker-and-bulkhead).

## Rate limiting

A token bucket refilled lazily from timestamps — no timer thread anywhere. The wait parks the virtual worker, never the event loop, and shows up in the stage's latency metric:

```java
RateLimit providerLimit = RateLimit.perSecond(100);   // or RateLimit.of(50, Duration.ofSeconds(2))

flow.handle("geocode", geo::lookup, providerLimit);
```

**One instance = one bucket.** Pass the same instance to several stages to protect a single downstream dependency behind all of them. An idle bucket holds one full period of burst.

## Timeouts

A per-attempt budget. The attempt is cut with a `TimeoutException` that flows to recovery like any failure:

```java
flow.handle("charge", payments::charge, Duration.ofSeconds(2))
    .recover("charge-fallback", error -> Payment.deferred());
```

Budgets are guards against hung calls, not precise timers: the engine schedules them on a shared timer wheel, and a cut may land a few milliseconds after the deadline — never before.

## Retries

```java
flow.handle("notify", mailer::send, Retry.of(3, Duration.ofMillis(100)));          // fixed backoff
flow.handle("sync",   erp::push,    Retry.exponential(4, Duration.ofMillis(50)));  // 50, 100, 200ms
```

Backoff parks the worker between attempts. Combined with a timeout, **the budget applies to each attempt** — a hung attempt is cut, then retried:

```java
flow.handle("charge", payments::charge, Duration.ofSeconds(2), Retry.of(3, Duration.ofMillis(100)));
```

Exhausted retries surface the last failure to the recovery path. Retries are observable via the `stageRetried` metric.

## Recovery

`recover` is **positional**: it catches failures from links upstream of it — exceptions, timeouts, exhausted retries, failed fan-outs and batches — and the flow continues after it with the recovered value:

```java
flow.handle("inventory", stock::reserve)
    .handle("charge", payments::charge)
    .recover("degrade", error -> Order.acceptedForLater())   // catches BOTH stages above
    .background("notify-ops", ops::flag);
```

- Several recoveries create tiers: the closest one downstream of the failure wins; if it throws too, the search continues.
- Inside a fork lane, `recover` only catches failures of values routed through that branch.
- With no recovery left, the failure completes the caller's future exceptionally and reports to `onError`.

## Failure semantics per link

| Link | On failure |
|---|---|
| `handle` | Flows to the nearest downstream `recover`, else fails the execution |
| `filter` / `when` / `match` predicates | A throwing predicate fails the value — never the engine |
| `background` | Never fails the flow; reports to error handlers only |
| `fanOut` | Any branch failure fails the fan-out — recoverable |
| `batch` | A bulk failure fails every batched execution — each recovers on its own chain |

## Resilience4j: circuit breaker and bulkhead

If you bring `io.github.resilience4j:resilience4j-all` to your classpath, decorate stage functions with the stateful pair:

```java
CircuitBreaker breaker = CircuitBreaker.ofDefaults("payments");
Bulkhead bulkhead = Bulkhead.ofDefaults("payments");

flow.handle("charge", Resilience4jStages.guarded(breaker, bulkhead, payments::charge))
    .recover("charge-fallback", error -> Payment.deferred());
```

Decorated functions are plain functions: an open breaker (`CallNotPermittedException`) or a full bulkhead (`BulkheadFullException`) short-circuits **into the recovery path** like any stage failure. With a native `Retry` on the same stage, each attempt passes through the decorators — the breaker counts every attempt.
