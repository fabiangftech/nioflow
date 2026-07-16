package dev.nioflow.infrastructure.reactive;

import dev.nioflow.application.facade.DefaultNioEngine;
import dev.nioflow.application.facade.DefaultNioFlow;
import dev.nioflow.core.facade.NioEngine;
import dev.nioflow.core.facade.NioFlowMetrics;
import dev.nioflow.core.model.Retry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The Mono/chain BOUNDARY, where the facade's promise is either kept or
 * quietly broken: what a Mono's failure looks like once it reaches recover(),
 * how many times a Mono is subscribed, which thread subscribes it, and what an
 * EMPTY Mono means to a chain whose links all expect a value.
 *
 * <p>ReactiveFlowTest asserts the happy rows of the RFC's semantics table;
 * this one goes after the edges that would make a reactive stage stop behaving
 * like an ordinary stage.
 */
class ReactiveMonoSemanticsTest {

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

    // ── error identity: a Mono's failure must reach recover() AS ITSELF ──

    @Test
    void aCheckedFailureReachesRecoverUnwrapped() {
        // Blocking.await puts a checked cause in a CompletionException (Mono.block
        // would otherwise hand over a Reactor wrapper). The engine unwraps that on
        // the recovery path, so the stage's own exception is what recover() sees —
        // the same as it would for a plain handle() that threw it.
        AtomicReference<Throwable> seen = new AtomicReference<>();

        Integer result = flow.just(1)
                .handleMono("remote", value -> Mono.error(new IOException("connection reset")))
                .recover(error -> {
                    seen.set(error);
                    return -1;
                })
                .executeMono()
                .block();

        assertEquals(-1, result);
        assertInstanceOf(IOException.class, seen.get());
        assertEquals("connection reset", seen.get().getMessage());
    }

    @Test
    void anUnrecoveredCheckedFailureSurfacesAsItselfOnTheMono() {
        // Same unwrapping on the terminal: a subscriber's onError gets the
        // IOException, not a CompletionException wrapping it.
        StepVerifier.create(flow.just(1)
                        .handleMono("remote", value -> Mono.error(new IOException("connection reset")))
                        .executeMono())
                .verifyErrorMatches(error -> error instanceof IOException
                        && "connection reset".equals(error.getMessage()));
    }

    @Test
    void aMonoTimeoutIsIndistinguishableFromAStageTimeout() {
        // The promise the facade makes: mono.timeout(d) and handle(name, fn, timeout)
        // hand recover() the same exception type. If Blocking stopped unwrapping,
        // this one would arrive as a Reactor ReactiveException.
        AtomicReference<Throwable> fromMono = new AtomicReference<>();
        AtomicReference<Throwable> fromStage = new AtomicReference<>();

        flow.just(1)
                .handleMono("slow-remote", value -> Mono.never(), Duration.ofMillis(50))
                .recover(error -> {
                    fromMono.set(error);
                    return -1;
                })
                .executeMono()
                .block();

        CountDownLatch hung = new CountDownLatch(1);   // a call that never answers
        flow.just(1)
                .handle("slow-stage", value -> {
                    try {
                        hung.await(10, TimeUnit.SECONDS);
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                    }
                    return value;
                }, Duration.ofMillis(50))
                .recover(error -> {
                    fromStage.set(error);
                    return -1;
                })
                .executeMono()
                .block();

        assertInstanceOf(TimeoutException.class, fromMono.get());
        assertEquals(fromStage.get().getClass(), fromMono.get().getClass());
    }

    @Test
    void aFailingFluxReachesRecoverUnwrapped() {
        AtomicReference<Throwable> seen = new AtomicReference<>();

        List<Integer> result = flow.just(1)
                .adaptFlux(value -> Flux.concat(Flux.just(1, 2), Flux.error(new IllegalStateException("stream broke"))))
                .recover(error -> {
                    seen.set(error);
                    return List.of(-1);
                })
                .executeMono()
                .block();

        assertEquals(List.of(-1), result);
        assertInstanceOf(IllegalStateException.class, seen.get());
        assertEquals("stream broke", seen.get().getMessage());
    }

    // ── the empty Mono: the one value a chain cannot carry ──

    @Test
    void anEmptyMonoFailsWithEmptyMonoExceptionInsteadOfInjectingNull() {
        // A value-carrying handleMono that returns Mono.empty() no longer injects a
        // silent null into the next stage (RFC 0027). It fails with an
        // EmptyMonoException naming the step — the next stage never runs, and
        // recover() catches it like any other stage failure. A remote call that
        // legitimately returns "nothing" (a 404 mapped to Mono.empty()) must be
        // modelled explicitly (an Optional/sentinel inside the Mono) instead.
        AtomicBoolean nextRan = new AtomicBoolean(false);
        AtomicReference<Throwable> recovered = new AtomicReference<>();

        Integer result = flow.just(1)
                .handleMono("lookup", value -> Mono.empty())
                .handle("next", value -> {
                    nextRan.set(true);
                    return value;
                })
                .recover(error -> {
                    recovered.set(error);
                    return -1;
                })
                .executeMono()
                .block();

        assertEquals(-1, result);
        assertFalse(nextRan.get(), "the next stage must not run on an empty value");
        assertInstanceOf(EmptyMonoException.class, recovered.get());
        assertTrue(recovered.get().getMessage().contains("lookup"), recovered.get()::getMessage);
    }

    @Test
    void aMidChainEmptyMonoErrorsWhileAFilterCutStillCompletesEmpty() {
        // These used to be indistinguishable — both an empty terminal Mono. Now a
        // value-carrying handleMono that goes empty is a stage FAILURE (RFC 0027),
        // while a filter() cut is still the empty terminal it always was: "no
        // value" that a person chose, not one a remote call smuggled in.
        StepVerifier.create(flow.just(1).handleMono("lookup", value -> Mono.<Integer>empty()).executeMono())
                .verifyError(EmptyMonoException.class);

        StepVerifier.create(flow.just(1).filter(value -> false).executeMono())
                .verifyComplete();
    }

    @Test
    void anEmptyAdaptMonoFailsWithEmptyMonoExceptionInsteadOfNull() {
        // adaptMono is value-carrying too, so an empty one fails identically —
        // never a null of the new type handed to the next stage.
        AtomicBoolean nextRan = new AtomicBoolean(false);
        AtomicReference<Throwable> recovered = new AtomicReference<>();

        String result = flow.just(1)
                .adaptMono(value -> Mono.<String>empty())
                .handle("next", text -> {
                    nextRan.set(true);
                    return text;
                })
                .recover(error -> {
                    recovered.set(error);
                    return "recovered";
                })
                .executeMono()
                .block();

        assertEquals("recovered", result);
        assertFalse(nextRan.get(), "the next stage must not run on an empty value");
        assertInstanceOf(EmptyMonoException.class, recovered.get());
    }

    @Test
    void anEmptyFluxCollectsToAnEmptyListNotToNull() {
        // collectList() never emits null, so adaptFlux is the one reactive step
        // that cannot smuggle a null into the chain.
        List<Integer> result = flow.just(1)
                .adaptFlux(value -> Flux.<Integer>empty())
                .executeMono()
                .block();

        assertEquals(List.of(), result);
    }

    // ── subscription discipline ──

    @Test
    void theMonoIsSubscribedExactlyOncePerExecution() {
        AtomicInteger subscriptions = new AtomicInteger();
        Mono<Integer> mono = flow.just(1)
                .handleMono("remote", value -> Mono.just(value * 2).doOnSubscribe(s -> subscriptions.incrementAndGet()))
                .executeMono();

        assertEquals(2, mono.block());
        assertEquals(1, subscriptions.get());

        assertEquals(2, mono.block());
        assertEquals(2, subscriptions.get(), "a second subscription is a second execution, so a second call");
    }

    @Test
    void aRetriedMonoIsReSubscribedOncePerAttempt() {
        // The retry is the ENGINE's (Retry on the stage), not Reactor's: each
        // attempt re-applies the function, which builds and subscribes a new Mono.
        // A Mono cached at build time would subscribe once and retry nothing.
        AtomicInteger subscriptions = new AtomicInteger();

        Integer result = flow.just(1)
                .handleMono("flaky", value -> Mono.defer(() -> {
                    if (subscriptions.incrementAndGet() < 3) {
                        return Mono.error(new IllegalStateException("not yet"));
                    }
                    return Mono.just(value * 10);
                }), Retry.of(3, Duration.ofMillis(5)))
                .executeMono()
                .block();

        assertEquals(10, result);
        assertEquals(3, subscriptions.get());
    }

    @Test
    void aBudgetedRetryGivesEachAttemptItsOwnBudget() {
        // The layers, composed: the budget bounds ONE attempt, the retry spans the
        // attempts. Two attempts time out at 50ms each and the third answers — a
        // budget shared across attempts would have failed the whole stage instead.
        AtomicInteger attempts = new AtomicInteger();

        Integer result = flow.just(1)
                .handleMono("flaky-slow", value -> Mono.defer(() -> attempts.incrementAndGet() < 3
                                ? Mono.never()
                                : Mono.just(value * 7)),
                        Duration.ofMillis(50), Retry.of(3, Duration.ofMillis(5)))
                .executeMono()
                .block();

        assertEquals(7, result);
        assertEquals(3, attempts.get());
    }

    @Test
    void theMonoIsSubscribedOnAVirtualWorkerNeverOnABoss() {
        // The whole point of the facade: the park is a virtual thread unmounting,
        // never a boss thread (a blocked boss stalls every execution pinned to it).
        AtomicReference<Thread> subscriber = new AtomicReference<>();

        flow.just(1)
                .handleMono("remote", value -> Mono.fromSupplier(() -> {
                    subscriber.set(Thread.currentThread());
                    return value;
                }))
                .executeMono()
                .block();

        assertTrue(subscriber.get().isVirtual(),
                "subscribed on " + subscriber.get() + ", which is not a virtual worker");
    }

    @Test
    void aLaneThatDoesNotRouteNeverSubscribesItsMono() {
        // A reactive stage inside a lane is a guarded Stage: the value that is not
        // routed there must not even build the Mono, let alone call the remote.
        AtomicInteger subscriptions = new AtomicInteger();

        Integer result = flow.just(4)
                .when(value -> value > 100)
                .then(lane -> Reactive.lane(lane)
                        .handleMono("remote", value -> Mono.fromSupplier(() -> {
                            subscriptions.incrementAndGet();
                            return value * 1000;
                        })))
                .executeMono()
                .block();

        assertEquals(4, result);
        assertEquals(0, subscriptions.get());
    }

    // ── fan-out over Monos ──

    @Test
    void fanOutMonoJoinsInDeclarationOrderNotCompletionOrder() {
        List<Function<Integer, Mono<String>>> branches = List.of(
                value -> Mono.delay(Duration.ofMillis(120)).map(ignored -> "slow"),
                value -> Mono.just("fast"),
                value -> Mono.delay(Duration.ofMillis(60)).map(ignored -> "middle"));

        List<String> joined = flow.just(1)
                .fanOutMono("enrich", branches, results -> results)
                .executeMono()
                .block();

        assertEquals(List.of("slow", "fast", "middle"), joined);
    }

    @Test
    void aFailingBranchFailsTheFanOutAndIsRecoverable() {
        AtomicReference<Throwable> seen = new AtomicReference<>();
        List<Function<Integer, Mono<Integer>>> branches = List.of(
                value -> Mono.just(value * 2),
                value -> Mono.error(new IllegalStateException("branch down")));

        Integer result = flow.just(1)
                .fanOutMono("enrich", branches, results -> results.stream().mapToInt(Integer::intValue).sum())
                .recover(error -> {
                    seen.set(error);
                    return -1;
                })
                .executeMono()
                .block();

        assertEquals(-1, result);
        // Unwrapped here too: the branch's own exception, not a Reactor wrapper.
        assertInstanceOf(IllegalStateException.class, rootOf(seen.get()));
        assertEquals("branch down", rootOf(seen.get()).getMessage());
    }

    // ── it reports like an ordinary stage, because it IS one ──

    @Test
    void aReactiveStageReportsItsLatencyUnderItsOwnName() {
        Map<String, Long> stageNanos = new ConcurrentHashMap<>();
        ConcurrentLinkedQueue<String> retried = new ConcurrentLinkedQueue<>();
        engine.metrics(new NioFlowMetrics() {
            @Override
            public void stageCompleted(String stage, long nanos) {
                stageNanos.put(stage, nanos);
            }

            @Override
            public void stageRetried(String stage) {
                retried.add(stage);
            }
        });
        AtomicInteger attempts = new AtomicInteger();

        flow.just(1)
                .handleMono("remote", value -> Mono.defer(() -> attempts.incrementAndGet() < 2
                                ? Mono.error(new IllegalStateException("flaky"))
                                : Mono.delay(Duration.ofMillis(60)).map(ignored -> value)),
                        Retry.of(3, Duration.ofMillis(5)))
                .executeMono()
                .block();

        assertTrue(stageNanos.containsKey("remote"), "the reactive stage did not report: " + stageNanos.keySet());
        // The time parked on the Mono IS the stage's latency — that is what makes
        // a slow downstream visible in the metric instead of invisible.
        assertTrue(stageNanos.get("remote") >= Duration.ofMillis(50).toNanos(),
                "latency did not include the wait on the Mono: " + stageNanos.get("remote") + "ns");
        assertEquals(List.of("remote"), List.copyOf(retried));
    }

    private static Throwable rootOf(Throwable error) {
        Throwable cause = error;
        while (cause.getCause() != null && cause.getCause() != cause) {
            cause = cause.getCause();
        }
        return cause;
    }
}
