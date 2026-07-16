package dev.nioflow.infrastructure.reactive;

import dev.nioflow.application.facade.DefaultNioEngine;
import dev.nioflow.application.facade.DefaultNioFlow;
import dev.nioflow.core.facade.Context.Key;
import dev.nioflow.core.facade.NioEngine;
import dev.nioflow.core.model.Retry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The reactive facade: a ReactiveStep IS a NioStep, and a reactive stage IS a
 * Stage. Everything asserted here is a row of the RFC's semantics table.
 */
class ReactiveFlowTest {

    private NioEngine engine;
    private ReactiveFlow<Integer, Integer> flow;

    @BeforeEach
    void setUp() {
        engine = new DefaultNioEngine();
        flow = Reactive.<Integer, Integer>flow(DefaultNioFlow.from(Integer.class, engine)).allowUnbudgeted();
    }

    @AfterEach
    void tearDown() {
        engine.shutdown(Duration.ofSeconds(1));
    }

    @Test
    void nothingRunsUntilSomebodySubscribes() throws InterruptedException {
        AtomicInteger ran = new AtomicInteger();

        Mono<Integer> mono = flow.just(1)
                .handle("side-effect", value -> {
                    ran.incrementAndGet();
                    return value;
                })
                .executeMono();

        // Assembled, not subscribed: a Mono promises exactly this, and a naive
        // Mono.fromFuture(step.executeAsync()) would already have run it.
        Thread.sleep(100);
        assertEquals(0, ran.get());

        assertEquals(1, mono.block());
        assertEquals(1, ran.get());
    }

    @Test
    void eachSubscriptionIsAFreshExecution() {
        AtomicInteger runs = new AtomicInteger();
        Mono<Integer> mono = flow.just(1)
                .handle("count", value -> value + runs.incrementAndGet())
                .executeMono();

        assertEquals(2, mono.block());   // 1 + 1
        assertEquals(3, mono.block());   // 1 + 2: re-subscription re-ran the pipeline
        assertEquals(2, runs.get());
    }

    @Test
    void retryOnTheMonoReRunsTheWholePipeline() {
        AtomicInteger attempts = new AtomicInteger();

        Integer result = flow.just(1)
                .handle("flaky", value -> {
                    if (attempts.incrementAndGet() < 3) {
                        throw new IllegalStateException("not yet");
                    }
                    return value * 10;
                })
                .executeMono()
                .retry(2)          // Reactor-level retry, on top of the pipeline
                .block();

        assertEquals(10, result);
        assertEquals(3, attempts.get());
    }

    @Test
    void aFilterCutArrivesAsAnEmptyMono() {
        StepVerifier.create(flow.just(3)
                        .filter(value -> value > 100)
                        .handle("never", value -> value * 1000)
                        .executeMono())
                .verifyComplete();   // empty: no onNext

        // ...which is what makes the idiomatic 404 work.
        StepVerifier.create(flow.just(3)
                        .filter(value -> value > 100)
                        .executeMono()
                        .switchIfEmpty(Mono.error(new IllegalStateException("not found"))))
                .verifyErrorMessage("not found");
    }

    @Test
    void aTerminalFailureArrivesAsOnError() {
        StepVerifier.create(flow.just(1)
                        .handle("boom", value -> {
                            throw new IllegalStateException("kaboom");
                        })
                        .executeMono())
                .verifyErrorMessage("kaboom");
    }

    @Test
    void aRecoveredFailureIsNotAFailure() {
        StepVerifier.create(flow.just(1)
                        .handle("boom", value -> {
                            throw new IllegalStateException("kaboom");
                        })
                        .recover(error -> -1)
                        .executeMono())
                .expectNext(-1)
                .verifyComplete();
    }

    @Test
    void subscribingFromANonBlockingThreadNeverBlocksIt() {
        // Schedulers.parallel() threads are Reactor NonBlocking: if executeAsync()
        // (or anything under it) ever parked the caller, Reactor itself would
        // throw. This is the WebFlux contract, pinned.
        StepVerifier.create(flow.just(2)
                        .handle("work", value -> value * 21)
                        .executeMono()
                        .subscribeOn(Schedulers.parallel()))
                .expectNext(42)
                .verifyComplete();
    }

    @Test
    void aReactiveStageIsAnOrdinaryStage() {
        // Retry, recover and a lane all apply to a handleMono exactly as they do
        // to a handle — because it appends the same Stage link.
        AtomicInteger attempts = new AtomicInteger();

        Integer result = flow.just(5)
                .handleMono("remote", value -> Mono.fromSupplier(() -> {
                    if (attempts.incrementAndGet() < 2) {
                        throw new IllegalStateException("flaky downstream");
                    }
                    return value * 2;
                }), Retry.of(3, Duration.ofMillis(5)))
                .executeMono()
                .block();

        assertEquals(10, result);
        assertEquals(2, attempts.get());
    }

    @Test
    void aFailingMonoIsCaughtByRecover() {
        StepVerifier.create(flow.just(1)
                        .handleMono("remote", value -> Mono.error(new IllegalStateException("downstream is down")))
                        .recover(error -> -99)
                        .executeMono())
                .expectNext(-99)
                .verifyComplete();
    }

    @Test
    void theBudgetOnTheMonoCancelsTheCall() {
        AtomicInteger cancelled = new AtomicInteger();

        StepVerifier.create(flow.just(1)
                        .handleMono("slow",
                                value -> Mono.delay(Duration.ofSeconds(10)).map(ignored -> value)
                                        .doOnCancel(cancelled::incrementAndGet),
                                Duration.ofMillis(50))          // budget on the MONO
                        .executeMono())
                .verifyError(java.util.concurrent.TimeoutException.class);

        // The point of putting the budget on the Mono: the call is CANCELLED,
        // not merely abandoned (which is all a stage timeout could do).
        assertEquals(1, cancelled.get());
    }

    @Test
    void adaptMonoRetypesThroughTheMono() {
        StepVerifier.create(flow.just(7)
                        .adaptMono(value -> Mono.just("value=" + value))
                        .handle("shout", text -> text + "!")
                        .executeMono())
                .expectNext("value=7!")
                .verifyComplete();
    }

    @Test
    void adaptFluxCollectsTheStream() {
        StepVerifier.create(flow.just(3)
                        .adaptFlux(value -> Flux.range(1, value))
                        .handleSync("sum", numbers -> numbers)
                        .adapt(List::size)
                        .executeMono())
                .expectNext(3)
                .verifyComplete();
    }

    @Test
    void fanOutMonoRunsTheBranchesConcurrently() {
        List<java.util.function.Function<Integer, Mono<Integer>>> branches = List.of(
                value -> Mono.delay(Duration.ofMillis(100)).map(ignored -> value * 2),
                value -> Mono.delay(Duration.ofMillis(100)).map(ignored -> value * 3),
                value -> Mono.delay(Duration.ofMillis(100)).map(ignored -> value * 4));

        long start = System.nanoTime();
        Integer result = flow.just(1)
                .fanOutMono("enrich", branches, results -> results.stream().mapToInt(Integer::intValue).sum())
                .executeMono()
                .block();
        long elapsedMillis = (System.nanoTime() - start) / 1_000_000;

        assertEquals(9, result);   // 2 + 3 + 4
        assertTrue(elapsedMillis < 250, "the three 100ms branches ran concurrently, took " + elapsedMillis + "ms");
    }

    @Test
    void chainingStaysReactiveAfterAFork() {
        // The covariant overrides earn their keep here: without them, when()
        // would hand back a plain NioStep and handleMono would not compile.
        Integer result = flow.just(42)
                .when(value -> value > 10)
                    .then(lane -> Reactive.lane(lane)                     // ← the one unwrap
                            .handleMono("big", value -> Mono.just(value * 2)))
                    .otherwise(lane -> lane.handle("small", value -> value))
                .handleMono("after", value -> Mono.just(value + 1))       // ← still reactive
                .executeMono()
                .block();

        assertEquals(85, result);   // (42 * 2) + 1
    }

    @Test
    void reactiveStagesWorkInsideAMatchAndAFork() throws InterruptedException {
        CountDownLatch forked = new CountDownLatch(1);
        ConcurrentLinkedQueue<Integer> audited = new ConcurrentLinkedQueue<>();

        Integer result = flow.just(4)
                .match()
                    .is(value -> value % 2 == 0, lane -> Reactive.lane(lane)
                            .handleMono("even", value -> Mono.just(value * 10)))
                    .otherwise(lane -> lane.handle("odd", value -> -value))
                .fork("audit", sub -> Reactive.lane(sub)
                        .handleMono("report", value -> Mono.just(value).doOnNext(audited::add))
                        .handle("done", value -> {
                            forked.countDown();
                            return value;
                        }))
                .executeMono()
                .block();

        assertEquals(40, result);
        assertTrue(forked.await(2, TimeUnit.SECONDS));
        assertEquals(40, audited.poll());
    }

    @Test
    void withSeedsTheContextTheCallerKnew() {
        Key<String> trace = Key.of("traceId");

        StepVerifier.create(flow.just(1)
                        .with(trace, "abc-123")
                        .handleContextual("read", (value, ctx) -> value + ctx.get(trace).length())
                        .executeMono())
                .expectNext(8)   // 1 + "abc-123".length()
                .verifyComplete();
    }

    @Test
    void pipeRunsAFluxThroughTheFlowUnordered() {
        StepVerifier.create(Flux.range(1, 5)
                        .transform(flow.pipe(4, (input, step) -> step
                                .handleMono("remote", value -> Mono.just(value * 10))
                                .adapt(value -> value + 1)))
                        .sort())
                .expectNext(11, 21, 31, 41, 51)
                .verifyComplete();
    }

    @Test
    void pipeOrderedKeepsTheInputOrder() {
        // Slower for smaller values: an unordered flatMap would emit 1 last.
        StepVerifier.create(Flux.just(3, 2, 1)
                        .transform(flow.<Integer>pipeOrdered(4, (input, step) -> step
                                .adaptMono(value -> Mono.delay(Duration.ofMillis(value * 40L))
                                        .map(ignored -> value))))
                        .collectList())
                .assertNext(values -> assertEquals(List.of(3, 2, 1), values))
                .verifyComplete();
    }

    @Test
    void pipeWithAKeyGivesPerKeyFifoAndCrossKeyParallelism() {
        ConcurrentLinkedQueue<String> order = new ConcurrentLinkedQueue<>();

        Flux.range(0, 6)
                .transform(flow.pipe(6, (input, step) -> step
                        .key(input % 2)                     // two keys: evens and odds
                        .handle("work", value -> {
                            order.add(value % 2 + ":" + value);
                            return value;
                        })))
                .blockLast(Duration.ofSeconds(5));

        // Within a key, submission order is preserved (0,2,4 and 1,3,5).
        List<String> evens = order.stream().filter(entry -> entry.startsWith("0:")).toList();
        List<String> odds = order.stream().filter(entry -> entry.startsWith("1:")).toList();
        assertEquals(List.of("0:0", "0:2", "0:4"), evens);
        assertEquals(List.of("1:1", "1:3", "1:5"), odds);
    }
}
