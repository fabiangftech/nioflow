# springwebflux-with-nioflow

The executable version of [RFC 0002](../../docs/rfc/0002-webflux.md): a WebFlux app whose handlers **are** nioflow pipelines.

```bash
./gradlew bootRun     # :8080
./gradlew test        # the endpoints, end to end over real HTTP
```

The build consumes `core/` through a composite build (`includeBuild('../../core')` with an explicit `dependencySubstitution` — without it Gradle silently resolves the *published* jar from Maven Central and the example stops testing your changes).

## The point

WebFlux gives you the non-blocking edge; nioflow gives you the blocking middle. Neither has to pretend to be the other:

```java
public Mono<Receipt> pay(String id, String traceId) {
    return orders.just(id)
            .with(TRACE, traceId)                              // seed the context the caller knew
            .adapt(repository::findById)                       // BLOCKING JDBC — on a virtual worker
            .filter(Objects::nonNull)                          // unknown id -> empty Mono -> 404
            .handleMono("fraud", remote::score,                // a WebClient call — an ordinary stage,
                    ofMillis(500), Retry.of(2, ofMillis(50)))  //   with a budget and a retry on top
            .adaptMono(remote::charge, ofSeconds(2))           // reactive, and it re-types: Order -> Receipt
            .fork("notify", sub -> Reactive.lane(sub)          // DETACHED: the response does not wait
                    .adaptMono(remote::notify)
                    .recover(error -> "notification failed"))
            .executeMono();                                    // Netty's event loop was never touched
}
```

No `subscribeOn`, no `publishOn`, no `boundedElastic`. The blocking repository call and the reactive `WebClient` calls sit in the same chain because both run on virtual workers — `executeMono()` only hands a task to a boss and returns.

## Endpoints

| | What it shows |
| --- | --- |
| `GET /orders/{id}` | `filter()` → empty `Mono` → **404**. A deliberate cut and "no value" are the same thing, so `switchIfEmpty` is all it takes. |
| `POST /orders/{id}/pay` | The headline: blocking repo + two `WebClient` stages + a **detached fork**, behind one `Mono`. The notification takes 300 ms and the response does not wait for it. |
| `POST /orders/pay-all?ids=1,2,3` | A `Flux` through the flow. Backpressure IS the `concurrency` argument; `key()` gives per-key FIFO in the **engine** while different keys stay parallel. One failing element fails the stream — right for a request/response batch. |
| `POST /orders/ingest?ids=1,nope,3` | The ingestion loop (`pipeResilient`): the poison element is reported to a handler and **dropped**, and the stream carries on. One bad message must not stop a consumer. |
| `GET /threads` | Which thread each part of the request ran on. Read on. |
| `POST /stub/**` | The "remote service" the `WebClient` calls — it is this same app, so the calls are real HTTP round trips, not mocks. |

## The trap `/threads` exists to show

```
controller=webflux-http-nio-4          ← Netty's event loop, released immediately
stage=VIRTUAL:                         ← your code: a virtual worker
insideMonoOperator=reactor-http-nio-2  ← a .map() CHAINED ON THE MONO runs on Netty!
afterReactiveStage=VIRTUAL:            ← the chain resumes on a worker
```

Read the third line twice. Operators you chain onto the `WebClient`'s Mono (`.map`, `.doOnNext`, `.filter`) run on the thread that **completes** it — Netty's event loop. That is Reactor behaving normally, and it is exactly where a blocking call takes the server down.

**Do the work in a stage, not in a `map()` on the Mono.** A stage body always runs on a virtual worker; that is the whole point.

## BlockHound: the proof, not the prose

The suite runs with [BlockHound](https://github.com/reactor/BlockHound) installed over the whole JVM (test scope only — it never reaches `core` or a consumer's runtime), and `NioFlowBlockHoundIntegration` tells it two things:

- **the virtual workers may block** — they are not `NonBlocking` threads, so nothing needs allowlisting. That IS the design: a stage parks a thread that unmounts;
- **the bosses may not** — they are marked non-blocking, so the engine's own event-loop rule ("the boss never runs user code") is now checked by the JVM. `BlockHoundTest` plants a `Thread.sleep` inside a boss-inlined `handleSync` and asserts it trips.

`/orders/{id}/pay` — blocking repository, `WebClient` calls parked on with `block()`, a detached fork — runs green with all of that armed. That is the central promise, held mechanically instead of by a thread-name assertion.

## What it does not show

Cancellation. If the client disconnects, the pipeline keeps running to completion and its result is dropped — the engine has no cancellation, and per-stage budgets are what bound it today. The RFC says so out loud, and so does this line.
