package dev.nioflow.infrastructure.reactive;

import dev.nioflow.application.facade.DefaultNioEngine;
import dev.nioflow.application.facade.DefaultNioFlow;
import dev.nioflow.core.facade.NioEngine;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * RFC 0016 — {@code fanOutMono} decorates core's {@code fanOutAsync} instead of
 * blocking N branches, so N concurrent remote calls park no worker. The results,
 * ordering, failure→recover and lane semantics are unchanged; what is genuinely
 * different is that a per-branch budget CANCELS the branch's subscription (it
 * rides on the Mono as {@code mono.timeout}), which these tests pin along with
 * the behaviour that must NOT have changed.
 */
class ReactiveFanOutMonoAsyncTest {

    private NioEngine engine;

    @BeforeEach
    void setUp() {
        engine = new DefaultNioEngine();
    }

    @AfterEach
    void tearDown() {
        engine.shutdown(Duration.ofSeconds(2));
    }

    private ReactiveFlow<Integer, Integer> flow() {
        return Reactive.flow(DefaultNioFlow.from(Integer.class, engine));
    }

    @Test
    void branchesRunConcurrentlyAndJoinInDeclarationOrder() {
        // A barrier of 3: unless all three branches are INVOKED concurrently (on
        // three workers), none passes it and the fan-out fails on the timeout.
        // Deterministic where a wall-clock threshold would be flaky.
        var barrier = new CyclicBarrier(3);
        Function<Integer, Mono<Integer>> meet = value -> {
            awaitBarrier(barrier);
            return Mono.just(value);
        };
        List<Function<Integer, Mono<Integer>>> branches = List.of(
                value -> meet.apply(value).map(v -> v + 1),
                value -> meet.apply(value).map(v -> v + 2),
                value -> meet.apply(value).map(v -> v + 3));

        String result = flow().just(10)
                .fanOutMono("enrich", branches,
                        results -> results.get(0) + "-" + results.get(1) + "-" + results.get(2))
                .executeMono()
                .block();

        assertEquals("11-12-13", result);   // declaration order, regardless of finish
    }

    @Test
    void aFailingBranchFailsTheFanOutAndIsRecoverable() {
        List<Function<Integer, Mono<Integer>>> branches = List.of(
                value -> Mono.just(value + 1),
                value -> Mono.error(new IllegalStateException("branch down")));

        Integer recovered = flow().just(1)
                .fanOutMono("enrich", branches, results -> results.get(0) + results.get(1))
                .recover("net", error -> -1)
                .executeMono()
                .block();

        assertEquals(-1, recovered);
    }

    @Test
    void theBudgetCancelsAHungBranchAndReachesRecoverAsTimeout() {
        var cancelled = new AtomicBoolean();
        ReactiveFlow<Integer, Integer> budgeted = flow().defaultBudget(Duration.ofMillis(80));
        List<Function<Integer, Mono<Integer>>> branches = List.of(
                value -> Mono.just(value + 1),
                value -> Mono.<Integer>never().doOnCancel(() -> cancelled.set(true)));

        Integer result = budgeted.just(1)
                .fanOutMono("enrich", branches, results -> results.get(0) + results.get(1))
                .recover(error -> error instanceof TimeoutException ? -1 : -2)
                .executeMono()
                .block();

        assertEquals(-1, result);   // the budget's TimeoutException reached recover
        assertTrue(cancelled.get(), "the budget must cancel the hung branch's subscription");
    }

    @Test
    void fanOutMonoInsideALaneStillRunsConcurrently() {
        // Same barrier proof, but the fan-out is inside a when() branch: the lane
        // mirror uses the same async path, so it parks nothing either.
        var barrier = new CyclicBarrier(2);
        Function<Integer, Mono<Integer>> meet = value -> {
            awaitBarrier(barrier);
            return Mono.just(value);
        };
        List<Function<Integer, Mono<Integer>>> branches = List.of(
                value -> meet.apply(value).map(v -> v + 1),
                value -> meet.apply(value).map(v -> v + 2));

        Integer result = flow().just(4)
                .when(value -> value % 2 == 0)
                .then(lane -> Reactive.lane(lane)
                        .fanOutMono("even-enrich", branches, results -> results.get(0) * results.get(1)))
                .otherwise(lane -> lane.handle(value -> -value))
                .executeMono()
                .block();

        assertEquals(30, result);   // (4+1) * (4+2), both branches met the barrier
    }

    private static void awaitBarrier(CyclicBarrier barrier) {
        try {
            barrier.await(5, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new IllegalStateException("a branch did not run concurrently", e);
        }
    }
}
