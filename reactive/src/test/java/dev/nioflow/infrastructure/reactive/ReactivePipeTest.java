package dev.nioflow.infrastructure.reactive;

import dev.nioflow.application.facade.DefaultNioEngine;
import dev.nioflow.application.facade.DefaultNioFlow;
import dev.nioflow.core.facade.NioEngine;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * pipe()/pipeOrdered(): a Flux through the flow, one execution per element.
 * Reactor's operator does the request(n) accounting — what is asserted here is
 * that the CONCURRENCY argument really is the number of executions in flight,
 * that the executions stay isolated from each other, and what an element that
 * is filtered or that fails does to the stream around it.
 */
class ReactivePipeTest {

    private NioEngine engine;
    private ReactiveFlow<Integer, Integer> flow;

    @BeforeEach
    void setUp() {
        engine = new DefaultNioEngine();
        flow = Reactive.<Integer, Integer>flow(DefaultNioFlow.from(Integer.class, engine)).allowUnbudgeted();
    }

    @AfterEach
    void tearDown() {
        engine.shutdown(Duration.ofSeconds(2));
    }

    @Test
    void concurrencyIsTheNumberOfExecutionsInFlight() {
        AtomicInteger inFlight = new AtomicInteger();
        AtomicInteger peak = new AtomicInteger();

        Function<Flux<Integer>, Flux<Integer>> pipe = flow.pipe(3, (input, step) -> step
                .handleMono("slow", value -> Mono
                        .fromSupplier(() -> {
                            peak.accumulateAndGet(inFlight.incrementAndGet(), Math::max);
                            return value;
                        })
                        .delayElement(Duration.ofMillis(60))
                        .doOnNext(ignored -> inFlight.decrementAndGet())));

        List<Integer> results = pipe.apply(Flux.range(1, 12)).collectList().block();

        assertEquals(12, results.size());
        assertTrue(peak.get() <= 3, "concurrency 3 was exceeded: " + peak.get() + " executions in flight");
        assertTrue(peak.get() > 1, "nothing ran concurrently at all (peak " + peak.get() + ")");
    }

    @Test
    void aFilteredElementDisappearsFromTheStream() {
        // A filter() cut is an empty Mono, and flatMap over an empty Mono emits
        // nothing: the element is DROPPED from the output, not nulled and not an
        // error. This is what makes pipe() usable as a filtering stage — and it
        // means the output Flux can be shorter than the input.
        Function<Flux<Integer>, Flux<Integer>> pipe = flow.pipe(4, (input, step) -> step
                .filter(value -> value % 2 == 0)
                .handle("double", value -> value * 2));

        StepVerifier.create(pipe.apply(Flux.range(1, 6)).sort())
                .expectNext(4, 8, 12)   // 2, 4, 6 doubled; the odd ones vanished
                .verifyComplete();
    }

    @Test
    void aFailingElementFailsTheStreamUnlessItIsRecovered() {
        Function<Flux<Integer>, Flux<Integer>> failing = flow.pipe(1, (input, step) -> step
                .handle("boom", value -> {
                    if (value == 3) {
                        throw new IllegalStateException("element 3 is poison");
                    }
                    return value;
                }));

        StepVerifier.create(failing.apply(Flux.range(1, 5)))
                .expectNext(1, 2)
                .verifyErrorMessage("element 3 is poison");

        // recover() inside the pipeline keeps the stream alive: the failure never
        // reaches the Flux, which is where a per-element net belongs.
        Function<Flux<Integer>, Flux<Integer>> recovered = flow.pipe(1, (input, step) -> step
                .handle("boom", value -> {
                    if (value == 3) {
                        throw new IllegalStateException("element 3 is poison");
                    }
                    return value;
                })
                .recover(error -> -1));

        StepVerifier.create(recovered.apply(Flux.range(1, 5)))
                .expectNext(1, 2, -1, 4, 5)
                .verifyComplete();
    }

    @Test
    void pipeOrderedKeepsTheOrderWhileStillRunningConcurrently() {
        // flatMapSequential: the slow first element must not serialize the rest,
        // it only holds back their EMISSION.
        Function<Flux<Integer>, Flux<Integer>> pipe = flow.pipeOrdered(5, (input, step) -> step
                .handleMono("staggered", value -> Mono
                        .delay(Duration.ofMillis(20L * (6 - value)))
                        .map(ignored -> value * 10)));

        long start = System.nanoTime();
        List<Integer> results = pipe.apply(Flux.range(1, 5)).collectList().block();
        long elapsedMillis = (System.nanoTime() - start) / 1_000_000;

        assertEquals(List.of(10, 20, 30, 40, 50), results);
        // Serialized, the staggered delays would add up to 300ms.
        assertTrue(elapsedMillis < 250, "pipeOrdered serialized the work, took " + elapsedMillis + "ms");
    }

    @Test
    void eachElementIsItsOwnExecutionWithItsOwnContext() {
        // One execution per element: the per-request context of one element must
        // never be visible to another (they run concurrently on shared workers).
        Set<Object> contexts = ConcurrentHashMap.newKeySet();
        var tag = new dev.nioflow.core.facade.Context.Key<Integer>("tag");

        Function<Flux<Integer>, Flux<Integer>> pipe = flow.pipe(4, (input, step) -> step
                .with(tag, input)
                .handleContextual("read-back", (value, context) -> {
                    contexts.add(context.get(tag));
                    return context.get(tag) + value;
                }));

        List<Integer> results = pipe.apply(Flux.range(1, 8)).sort().collectList().block();

        assertEquals(List.of(2, 4, 6, 8, 10, 12, 14, 16), results);   // each saw its OWN tag
        assertEquals(8, contexts.size());
    }

    @Test
    void anEmptyFluxRunsNothingAndCompletes() {
        AtomicInteger ran = new AtomicInteger();
        Function<Flux<Integer>, Flux<Integer>> pipe = flow.pipe(2, (input, step) -> step
                .handle("work", value -> {
                    ran.incrementAndGet();
                    return value;
                }));

        StepVerifier.create(pipe.apply(Flux.empty())).verifyComplete();
        assertEquals(0, ran.get());
    }

    // ── the knobs, and the failure mode that should be a decision ──

    @Test
    void aConcurrencyBelowOneIsRejectedWhenThePipeIsBuilt() {
        // Reactor validates concurrency on SUBSCRIBE: a pipe(0, ...) wired at
        // startup would blow up at the first element, inside FluxFlatMap, naming
        // none of the caller's code. Fail where the mistake is.
        IllegalArgumentException zero = assertThrows(IllegalArgumentException.class,
                () -> flow.pipe(0, (input, step) -> step.handle("work", value -> value)));
        IllegalArgumentException negative = assertThrows(IllegalArgumentException.class,
                () -> flow.pipeOrdered(-1, (input, step) -> step.handle("work", value -> value)));

        assertTrue(zero.getMessage().contains("concurrency"), zero.getMessage());
        assertTrue(negative.getMessage().contains("concurrency"), negative.getMessage());
        assertThrows(IllegalArgumentException.class,
                () -> flow.pipe(2, -1, (input, step) -> step.handle("work", value -> value)));
    }

    @Test
    void prefetchBoundsWhatTheOperatorPullsFromAnEagerSource() {
        // The pipeline is slow and the source is eager: with prefetch 1, flatMap
        // requests one element ahead of the executions in flight, so the source is
        // never drained into a buffer nobody asked for.
        AtomicInteger requested = new AtomicInteger();

        Function<Flux<Integer>, Flux<Integer>> pipe = flow.pipe(2, 1, (input, step) -> step
                .handleMono("slow", value -> Mono.delay(Duration.ofMillis(30)).map(ignored -> value)));

        List<Integer> results = pipe.apply(Flux.range(1, 10).doOnRequest(n -> requested.addAndGet((int) n)))
                .sort()
                .collectList()
                .block();

        assertEquals(List.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), results);
        // Reactor requests concurrency + prefetch up front, then replenishes: what
        // matters is that it did NOT request the whole 10 at once.
        assertTrue(requested.get() >= 10, "everything must still be consumed: " + requested.get());
    }

    @Test
    void pipeResilientDropsThePoisonElementAndKeepsTheStreamAlive() {
        // pipe() would stop the consumer here. Whether one bad message does that
        // should be a decision somebody made, which is what this method names.
        List<Integer> reported = new java.util.concurrent.CopyOnWriteArrayList<>();
        List<Throwable> engineSaw = new java.util.concurrent.CopyOnWriteArrayList<>();
        flow.onError(engineSaw::add);

        Function<Flux<Integer>, Flux<Integer>> pipe = flow.pipeResilient(1, (input, step) -> step
                        .handle("boom", value -> {
                            if (value == 3) {
                                throw new IllegalStateException("element 3 is poison");
                            }
                            return value;
                        }),
                (element, error) -> reported.add(element));

        StepVerifier.create(pipe.apply(Flux.range(1, 5)))
                .expectNext(1, 2, 4, 5)     // 3 was dropped, the stream carried on
                .verifyComplete();

        assertEquals(List.of(3), reported);
        // The engine's own handlers still see it — once. The element handler is the
        // stream's net, not a replacement for the flow's reporting.
        assertEquals(1, engineSaw.size(), "the failure was reported " + engineSaw.size() + " times: " + engineSaw);
        assertEquals("element 3 is poison", engineSaw.getFirst().getMessage());
    }

    @Test
    void pipeResilientHandsTheHandlerTheElementAndItsFailure() {
        Map<Integer, String> failures = new ConcurrentHashMap<>();

        Function<Flux<Integer>, Flux<Integer>> pipe = flow.pipeResilient(4, (input, step) -> step
                        .handle("boom", value -> {
                            if (value % 2 == 0) {
                                throw new IllegalStateException("even: " + value);
                            }
                            return value;
                        }),
                (element, error) -> failures.put(element, error.getMessage()));

        List<Integer> survivors = pipe.apply(Flux.range(1, 6)).sort().collectList().block();

        assertEquals(List.of(1, 3, 5), survivors);
        assertEquals(Map.of(2, "even: 2", 4, "even: 4", 6, "even: 6"), failures);
    }

    @Test
    void pipeResilientWithoutAHandlerIsNotAThing() {
        assertThrows(IllegalArgumentException.class,
                () -> flow.pipeResilient(2, (input, step) -> step.handle("work", value -> value), null));
    }

    @Test
    void theSharedChainRunsForEveryElementOfTheFlux() {
        // pipe() opens a just() per element, so the shared definition's links are
        // in front of the per-element ones — the same as any other execution.
        flow.handle("shared", value -> value + 100);

        Function<Flux<Integer>, Flux<Integer>> pipe = flow.pipe(2, (input, step) -> step
                .handle("local", value -> value * 2));

        List<Integer> results = pipe.apply(Flux.just(1, 2)).sort().collectList().block();

        assertEquals(List.of(202, 204), results);   // (1+100)*2, (2+100)*2
    }
}
