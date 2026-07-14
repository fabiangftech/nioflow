package dev.nioflow.springwebfluxwithnioflow.service;

import dev.nioflow.core.facade.Context;
import dev.nioflow.core.model.Retry;
import dev.nioflow.infrastructure.reactive.Reactive;
import dev.nioflow.infrastructure.reactive.ReactiveFlow;
import dev.nioflow.springwebfluxwithnioflow.client.RemoteClient;
import dev.nioflow.springwebfluxwithnioflow.model.Order;
import dev.nioflow.springwebfluxwithnioflow.model.Receipt;
import dev.nioflow.springwebfluxwithnioflow.repository.OrderRepository;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Objects;

/**
 * Every pipeline here returns a Mono, and none of them ever touches the Netty
 * event loop: executeMono() only hands a task to a boss, and the stages run on
 * virtual workers. That is why a BLOCKING repository call and a REACTIVE
 * WebClient call sit side by side in the same chain without ceremony — no
 * subscribeOn, no publishOn, no boundedElastic.
 */
@Service
public class OrderService {

    private static final Context.Key<String> TRACE = Context.Key.of("traceId");

    private final ReactiveFlow<String, Receipt> orders;
    private final OrderRepository repository;
    private final RemoteClient remote;

    public OrderService(ReactiveFlow<String, Receipt> orders, OrderRepository repository, RemoteClient remote) {
        this.orders = orders;
        this.repository = repository;
        this.remote = remote;
    }

    /**
     * A filter() cut and a Mono's "no value" are the same thing, so the
     * idiomatic 404 needs no special case: an unknown id produces an empty Mono
     * and switchIfEmpty (in the controller) turns it into the 404.
     */
    public Mono<Order> find(String id) {
        return orders.just(id)
                .adapt(repository::findById)       // BLOCKING JDBC — on a virtual worker
                .filter(Objects::nonNull)          // unknown id -> cut -> empty Mono
                .executeMono();
    }

    /**
     * The headline: a blocking repository call, two reactive WebClient calls and
     * a detached fork, in ONE chain, behind a Mono.
     *
     * <p>The notification takes 300 ms and the response does not wait for it —
     * that is what fork() means. Response latency is repo (40) + fraud (60) +
     * charge (80), not that plus 300.
     */
    public Mono<Receipt> pay(String id, String traceId) {
        return orders.just(id)
                .with(TRACE, traceId)                                   // seed the context the caller knew
                .adapt(repository::findById)                            // blocking
                .filter(Objects::nonNull)
                .handleMono("fraud", remote::score,                     // reactive — and an ORDINARY stage:
                        Duration.ofMillis(500),                         //   the budget CANCELS the HTTP call,
                        Retry.of(2, Duration.ofMillis(50)))             //   and retry composes on top of it
                .adaptMono(remote::charge, Duration.ofSeconds(2))       // reactive, and it re-types: Order -> Receipt
                .fork("notify", sub -> Reactive.lane(sub)               // DETACHED: nobody waits for this
                        .adaptMono(remote::notify)                      // Receipt -> Mono<String>: adaptMono re-types,
                        .recover(error -> "notification failed"))       // handleMono would have to give a Receipt back
                .handleContextual("audit", (receipt, context) -> {
                    System.out.printf("[%s] order %s -> %s%n",
                            context.get(TRACE), receipt.orderId(), receipt.status());
                    return receipt;
                })
                .executeMono();
    }

    /**
     * A Flux through the flow. Backpressure IS the concurrency argument — it is
     * the number of executions in flight — and Reactor's own operator does the
     * request(n) accounting (we do not implement a Publisher).
     *
     * <p>The ordering lives in the ENGINE, not in the Flux: key() pins same-key
     * executions to one boss and runs them one at a time, in submission order,
     * while different keys keep full parallelism. Kafka-partition semantics over
     * a Flux, without serializing the stream.
     */
    public Flux<Receipt> payAll(Flux<String> ids, int concurrency) {
        return ids.transform(orders.pipe(concurrency, (id, step) -> step
                .key(id)                                            // per-order FIFO, cross-order parallelism
                .adapt(repository::findById)
                .filter(Objects::nonNull)
                .adaptMono(remote::charge, Duration.ofSeconds(2))));
    }

    /**
     * Proof, not prose — and the one trap worth knowing. Reports the thread each
     * part of the request ran on:
     *
     * <pre>
     * controller=webflux-http-nio-4      ← Netty's event loop, released immediately
     * stage=VIRTUAL:                     ← where YOUR code runs: a virtual worker
     * insideMonoOperator=reactor-http-nio ← a .map() CHAINED ON THE MONO runs on Netty!
     * afterReactiveStage=VIRTUAL:        ← the chain resumes on a worker
     * </pre>
     *
     * <p>Read the third line twice. Operators you chain onto the WebClient's Mono
     * ({@code .map}, {@code .doOnNext}, {@code .filter}) execute on the thread
     * that completes it — Netty's event loop. That is Reactor behaving normally,
     * and it is where a blocking call would take the server down. The rule is
     * simple: <b>do the work in a stage, not in a {@code map()} on the Mono.</b>
     * A stage body always runs on a virtual worker, which is the whole point.
     */
    public Mono<String> threads(String callerThread) {
        return orders.just("1")
                .adapt(id -> "controller=" + callerThread)
                .adapt(report -> report + " | stage=" + describe())
                .adaptMono(report -> remote.score(new Order("1", "alice", 1, false))
                        // Chained on the Mono: this runs on NETTY, not on the worker.
                        .map(ignored -> report + " | insideMonoOperator=" + describe()))
                .adapt(report -> report + " | afterReactiveStage=" + describe())   // a stage again: worker
                .executeMono();
    }

    private static String describe() {
        Thread thread = Thread.currentThread();
        return (thread.isVirtual() ? "VIRTUAL:" : "platform:") + thread.getName();
    }
}
