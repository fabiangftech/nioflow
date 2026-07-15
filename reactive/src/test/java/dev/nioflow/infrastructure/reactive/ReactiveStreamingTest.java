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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Streaming OUT: the bounded adaptFlux and the executeFlux terminal.
 *
 * <p>The rule under test is the one the model has always had — a nioflow value is
 * ONE object — and the two ways of respecting it: if you collect a stream, name a
 * bound; if you cannot name one, do not collect it at all (stream it past the
 * pipeline instead). The un-capped adaptFlux is the way to break the rule, and it
 * stays; what changes is that the correct answer now has a name at the same call
 * site.
 */
class ReactiveStreamingTest {

    private NioEngine engine;
    private ReactiveFlow<Integer, Integer> flow;

    @BeforeEach
    void setUp() {
        engine = new DefaultNioEngine();
        flow = Reactive.flow(DefaultNioFlow.from(Integer.class, engine));
    }

    @AfterEach
    void tearDown() {
        engine.shutdown(Duration.ofSeconds(1));
    }

    // ── the bounded adaptFlux ────────────────────────────────────────────

    @Test
    void aStreamUnderTheCapCollectsNormally() {
        List<Integer> rows = flow.just(3)
                .adaptFlux(value -> Flux.range(1, value), 10)
                .executeMono()
                .block();

        assertEquals(List.of(1, 2, 3), rows);
    }

    @Test
    void aStreamExactlyAtTheCapCollectsNormally() {
        // The cap is inclusive: maxItems items are what the caller agreed to hold.
        List<Integer> rows = flow.just(3)
                .adaptFlux(value -> Flux.range(1, 3), 3)
                .executeMono()
                .block();

        assertEquals(List.of(1, 2, 3), rows);
    }

    @Test
    void aStreamOverTheCapFailsWithFlowOverflow() {
        StepVerifier.create(flow.just(1)
                        .adaptFlux(value -> Flux.range(1, 1_000), 10)
                        .executeMono())
                .verifyErrorMatches(error -> error instanceof FlowOverflowException
                        && error.getMessage().contains("more than 10 items"));
    }

    @Test
    void theSourceIsCancelledAtTheCapPlusOne() {
        // The whole point of take(maxItems + 1): an overrun costs ONE extra
        // element, not the ten million the stream still had to give. Asserted,
        // not assumed — a stub counts what it was actually asked to emit.
        AtomicInteger emitted = new AtomicInteger();

        StepVerifier.create(flow.just(1)
                        .adaptFlux(value -> Flux.range(1, 100_000).doOnNext(item -> emitted.incrementAndGet()), 10)
                        .executeMono())
                .verifyError(FlowOverflowException.class);

        assertEquals(11, emitted.get(), "the source must be cancelled at maxItems + 1, not drained");
    }

    @Test
    void theOverflowIsCaughtByRecoverLikeAnyStageFailure() {
        AtomicReference<Throwable> seen = new AtomicReference<>();

        List<Integer> rows = flow.just(1)
                .adaptFlux(value -> Flux.range(1, 1_000), 10)
                .recover(error -> {
                    seen.set(error);
                    return List.of(-1);
                })
                .executeMono()
                .block();

        assertEquals(List.of(-1), rows);
        assertInstanceOf(FlowOverflowException.class, seen.get());
    }

    @Test
    void aCapOfZeroIsRejectedAtBuildTime() {
        // Not a bound, a mistake — and rejected where the caller's line still is.
        ReactiveStep<Integer, Integer> step = flow.just(1);
        Function<Integer, Flux<Integer>> source = value -> Flux.range(1, 3);

        IllegalArgumentException rejected = assertThrows(IllegalArgumentException.class,
                () -> step.adaptFlux(source, 0));
        assertTrue(rejected.getMessage().contains("at least 1"));
    }

    @Test
    @SuppressWarnings("deprecation")   // the whole point of this test is the uncapped overload
    void theUnCappedOverloadStillBehavesExactlyAsBefore() {
        // It has to survive — removing it breaks every existing call site.
        List<Integer> rows = flow.just(4)
                .adaptFlux(value -> Flux.range(1, value))
                .executeMono()
                .block();

        assertEquals(List.of(1, 2, 3, 4), rows);
    }

    @Test
    void theBoundedCollectWorksInsideABranch() {
        // A fork body or a branch is no safer a place to buffer an unbounded
        // stream than the main line is, so the cap must be reachable there too.
        Integer result = flow.just(20)
                .when(value -> value > 10)
                    .then(lane -> Reactive.lane(lane)
                            .adaptFlux(value -> Flux.range(1, 1_000), 10)
                            .recover(error -> List.of(-1))
                            .adapt(List::size))
                    .otherwise(lane -> lane.handle("small", value -> value))
                .executeMono()
                .block();

        assertEquals(1, result);   // the recovery's single element: the cap tripped
    }

    // ── executeFlux: the streaming terminal ──────────────────────────────

    @Test
    void executeFluxIsLazy() {
        // Assembling runs nothing: executeMono() is a supplier, and flatMapMany
        // subscribes it only when somebody subscribes the Flux.
        AtomicInteger executions = new AtomicInteger();
        AtomicBoolean tailSubscribed = new AtomicBoolean();

        Flux<Integer> stream = flow.just(1)
                .handle("count", value -> {
                    executions.incrementAndGet();
                    return value;
                })
                .executeFlux(value -> Flux.range(1, 3)
                        .doOnSubscribe(subscription -> tailSubscribed.set(true)));

        assertEquals(0, executions.get());
        assertFalse(tailSubscribed.get());

        StepVerifier.create(stream).expectNext(1, 2, 3).verifyComplete();
        assertEquals(1, executions.get());
        assertTrue(tailSubscribed.get());
    }

    @Test
    void everySubscriptionIsItsOwnExecution() {
        AtomicInteger executions = new AtomicInteger();

        Flux<Integer> stream = flow.just(1)
                .handle("count", value -> {
                    executions.incrementAndGet();
                    return value;
                })
                .executeFlux(Flux::just);

        StepVerifier.create(stream).expectNext(1).verifyComplete();
        StepVerifier.create(stream).expectNext(1).verifyComplete();

        assertEquals(2, executions.get());
    }

    @Test
    void retryOnTheFluxReRunsTheWholePipeline() {
        // The Mono's semantics, inherited: .retry(2) after a failing stage means
        // three executions — not three subscriptions of one result.
        AtomicInteger attempts = new AtomicInteger();

        StepVerifier.create(flow.just(1)
                        .handleMono("remote", value -> {
                            attempts.incrementAndGet();
                            return Mono.error(new IllegalStateException("down"));
                        })
                        .executeFlux(Flux::just)
                        .retry(2))
                .verifyError(IllegalStateException.class);

        assertEquals(3, attempts.get());
    }

    @Test
    void aFilterCutIsAnEmptyFlux() {
        // The three notions of "nothing" line up: the cut completes the execution
        // with no value, executeMono() is empty, flatMapMany over an empty Mono is
        // an empty Flux — so switchIfEmpty is still your 404.
        AtomicBoolean tailSubscribed = new AtomicBoolean();

        Flux<Integer> stream = flow.just(1)
                .filter(value -> false)
                .executeFlux(value -> Flux.range(1, 3)
                        .doOnSubscribe(subscription -> tailSubscribed.set(true)));

        StepVerifier.create(stream).verifyComplete();
        assertFalse(tailSubscribed.get(), "there was no value: the tail has nothing to stream");

        StepVerifier.create(flow.just(1)
                        .filter(value -> false)
                        .executeFlux(value -> Flux.range(1, 3))
                        .switchIfEmpty(Flux.just(-404)))
                .expectNext(-404)
                .verifyComplete();
    }

    @Test
    void aPipelineFailureReachesOnErrorAndTheTailIsNeverSubscribed() {
        AtomicBoolean tailSubscribed = new AtomicBoolean();

        StepVerifier.create(flow.just(1)
                        .handle("boom", value -> {
                            throw new IllegalStateException("stage failed");
                        })
                        .executeFlux(value -> Flux.range(1, 3)
                                .doOnSubscribe(subscription -> tailSubscribed.set(true))))
                .verifyErrorMatches(error -> error instanceof IllegalStateException
                        && "stage failed".equals(error.getMessage()));

        assertFalse(tailSubscribed.get());
    }

    @Test
    void theTailStreamsInsteadOfBuffering() {
        // The one thing adaptFlux can never do: 100 000 elements pass through and
        // none of them is ever held in a List. Backpressure proves it — a
        // subscriber that asks for one element gets the source to emit a prefetch
        // window, not the whole stream.
        AtomicInteger emitted = new AtomicInteger();

        Flux<Integer> stream = flow.just(1)
                .handle("load", value -> value)
                .executeFlux(value -> Flux.range(1, 100_000).doOnNext(item -> emitted.incrementAndGet()));

        StepVerifier.create(stream, 1)
                .expectNext(1)
                .thenCancel()
                .verify(Duration.ofSeconds(5));

        assertTrue(emitted.get() < 100_000,
                "the tail must honour backpressure, not drain 100 000 elements: emitted " + emitted.get());

        emitted.set(0);
        StepVerifier.create(stream)
                .expectNextCount(100_000)
                .verifyComplete();
        assertEquals(100_000, emitted.get());
    }
}
