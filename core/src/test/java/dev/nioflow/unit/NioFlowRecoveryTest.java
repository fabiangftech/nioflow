package dev.nioflow.unit;

import dev.nioflow.application.facade.NioFlow;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class NioFlowRecoveryTest {

    @Test
    void aFailingValueRecoversWithTheFallbackInsteadOfDropping() {
        try (NioFlow<Integer> pipeline = new NioFlow<>()) {
            List<Integer> completed = new CopyOnWriteArrayList<>();
            List<Throwable> errors = new CopyOnWriteArrayList<>();
            pipeline.submit(x -> {
                        if (x == 2) {
                            throw new IllegalStateException("value 2 boom");
                        }
                        return x * 10;
                    })
                    .onErrorResume(error -> -1)
                    .onComplete(completed::add)
                    .onError(errors::add);

            pipeline.just(1);
            pipeline.just(2);
            pipeline.just(3);
            pipeline.join(); // no throw: the failure was recovered

            assertEquals(3, completed.size());
            assertTrue(completed.containsAll(List.of(10, -1, 30)));
            assertTrue(errors.isEmpty(), "a recovered value must not reach onError");
        }
    }

    @Test
    void aRecoveredValueContinuesDownTheChain() {
        try (NioFlow<Integer> pipeline = new NioFlow<>()) {
            // the recovery must be declared before the value fails, so build first
            pipeline.handle(x -> {
                        if (true) {
                            throw new IllegalStateException("boom");
                        }
                        return x;
                    })
                    .onErrorResume(error -> 0)
                    .handle(x -> x + 5);

            pipeline.just(1);
            assertEquals(5, pipeline.join());
        }
    }

    @Test
    void recoveryOnlyCatchesUpstreamFailures() throws InterruptedException {
        try (NioFlow<Integer> pipeline = new NioFlow<>()) {
            CountDownLatch failed = new CountDownLatch(1);
            pipeline.onErrorResume(error -> -1) // upstream of the failure: must not catch it
                    .handle(x -> {
                        throw new IllegalStateException("late boom");
                    })
                    .onError(error -> failed.countDown());
            pipeline.just(1);

            assertTrue(failed.await(1, TimeUnit.SECONDS));
            assertThrows(CompletionException.class, pipeline::join);
        }
    }

    @Test
    void aThrowingFallbackHandsTheErrorToTheNextRecovery() {
        try (NioFlow<Integer> pipeline = new NioFlow<>()) {
            pipeline.handle(x -> {
                        if (true) {
                            throw new IllegalStateException("boom");
                        }
                        return x;
                    })
                    .onErrorResume(error -> {
                        throw new IllegalStateException("fallback boom");
                    })
                    .onErrorResume(error -> -2);

            pipeline.just(1);
            assertEquals(-2, pipeline.join());
        }
    }

    @Test
    void recoveryInsideALaneOnlyCatchesItsOwnLane() {
        try (NioFlow<Integer> pipeline = new NioFlow<>()) {
            List<Integer> completed = new CopyOnWriteArrayList<>();
            pipeline.when(x -> x > 10)
                    .then(lane -> lane
                            .submit(x -> {
                                throw new IllegalStateException("lane boom");
                            })
                            .onErrorResume(error -> -1))
                    .otherwise(lane -> lane
                            .handle(x -> x))
                    .onComplete(completed::add);

            pipeline.just(20); // fails in the true lane, recovers to -1
            pipeline.just(5);  // false lane, untouched by the recovery
            pipeline.join();

            assertEquals(2, completed.size());
            assertTrue(completed.containsAll(List.of(-1, 5)));
        }
    }

    @Test
    void aTimedOutValueCanRecover() {
        try (NioFlow<Integer> pipeline = new NioFlow<>()) {
            int result = pipeline.just(1)
                    .submit(x -> {
                        sleep(5_000);
                        return x;
                    }, Duration.ofMillis(200))
                    .onErrorResume(error -> error instanceof TimeoutException ? -1 : -2)
                    .join();

            assertEquals(-1, result);
        }
    }

    @Test
    void theFallbackReceivesTheExactThrownException() {
        try (NioFlow<Integer> pipeline = new NioFlow<>()) {
            IllegalStateException boom = new IllegalStateException("original");
            AtomicReference<Throwable> seen = new AtomicReference<>();
            pipeline.handle(x -> {
                        if (true) {
                            throw boom;
                        }
                        return x;
                    })
                    .onErrorResume(error -> {
                        seen.set(error);
                        return -1;
                    });

            pipeline.just(1);

            assertEquals(-1, pipeline.join());
            assertSame(boom, seen.get());
        }
    }

    @Test
    void aMainLineRecoveryCatchesLaneFailures() {
        try (NioFlow<Integer> pipeline = new NioFlow<>()) {
            List<Integer> completed = new CopyOnWriteArrayList<>();
            pipeline.when(x -> x > 10)
                    .then(lane -> lane
                            .submit(x -> {
                                throw new IllegalStateException("lane boom");
                            }))
                    .otherwise(lane -> lane
                            .handle(x -> x))
                    .onErrorResume(error -> -1)
                    .onComplete(completed::add);

            pipeline.just(20); // fails in the true lane, recovered on the main line
            pipeline.just(5);  // false lane, flows through untouched
            pipeline.join();

            assertEquals(2, completed.size());
            assertTrue(completed.containsAll(List.of(-1, 5)));
        }
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
