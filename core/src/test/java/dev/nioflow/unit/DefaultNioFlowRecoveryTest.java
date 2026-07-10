package dev.nioflow.unit;

import dev.nioflow.application.facade.DefaultNioFlow;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class DefaultNioFlowRecoveryTest {

    @Test
    void aFailingValueRecoversWithTheFallbackInsteadOfDropping() {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            List<Integer> completed = new CopyOnWriteArrayList<>();
            List<Throwable> errors = new CopyOnWriteArrayList<>();
            defaultNioFlow.submit(x -> {
                        if (x == 2) {
                            throw new IllegalStateException("value 2 boom");
                        }
                        return x * 10;
                    })
                    .onErrorResume(error -> -1)
                    .onComplete(completed::add)
                    .onError(errors::add);

            defaultNioFlow.just(1);
            defaultNioFlow.just(2);
            defaultNioFlow.just(3);
            defaultNioFlow.join(); // no throw: the failure was recovered

            assertEquals(3, completed.size());
            assertTrue(completed.containsAll(List.of(10, -1, 30)));
            assertTrue(errors.isEmpty(), "a recovered value must not reach onError");
        }
    }

    @Test
    void aRecoveredValueContinuesDownTheChain() {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            // the recovery must be declared before the value fails, so build first
            defaultNioFlow.handle(x -> {
                        if (true) {
                            throw new IllegalStateException("boom");
                        }
                        return x;
                    })
                    .onErrorResume(error -> 0)
                    .handle(x -> x + 5);

            defaultNioFlow.just(1);
            assertEquals(5, defaultNioFlow.join());
        }
    }

    @Test
    void recoveryOnlyCatchesUpstreamFailures() throws InterruptedException {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            CountDownLatch failed = new CountDownLatch(1);
            defaultNioFlow.onErrorResume(error -> -1) // upstream of the failure: must not catch it
                    .handle(x -> {
                        throw new IllegalStateException("late boom");
                    })
                    .onError(error -> failed.countDown());
            defaultNioFlow.just(1);

            assertTrue(failed.await(1, TimeUnit.SECONDS));
            assertThrows(CompletionException.class, defaultNioFlow::join);
        }
    }

    @Test
    void aThrowingFallbackHandsTheErrorToTheNextRecovery() {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            defaultNioFlow.handle(x -> {
                        if (true) {
                            throw new IllegalStateException("boom");
                        }
                        return x;
                    })
                    .onErrorResume(error -> {
                        throw new IllegalStateException("fallback boom");
                    })
                    .onErrorResume(error -> -2);

            defaultNioFlow.just(1);
            assertEquals(-2, defaultNioFlow.join());
        }
    }

    @Test
    void recoveryInsideALaneOnlyCatchesItsOwnLane() {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            List<Integer> completed = new CopyOnWriteArrayList<>();
            defaultNioFlow.when(x -> x > 10)
                    .then(lane -> lane
                            .submit(x -> {
                                throw new IllegalStateException("lane boom");
                            })
                            .onErrorResume(error -> -1))
                    .otherwise(lane -> lane
                            .handle(x -> x))
                    .onComplete(completed::add);

            defaultNioFlow.just(20); // fails in the true lane, recovers to -1
            defaultNioFlow.just(5);  // false lane, untouched by the recovery
            defaultNioFlow.join();

            assertEquals(2, completed.size());
            assertTrue(completed.containsAll(List.of(-1, 5)));
        }
    }

    @Test
    void aTimedOutValueCanRecover() {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            int result = defaultNioFlow.just(1)
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
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            IllegalStateException boom = new IllegalStateException("original");
            AtomicReference<Throwable> seen = new AtomicReference<>();
            defaultNioFlow.handle(x -> {
                        if (true) {
                            throw boom;
                        }
                        return x;
                    })
                    .onErrorResume(error -> {
                        seen.set(error);
                        return -1;
                    });

            defaultNioFlow.just(1);

            assertEquals(-1, defaultNioFlow.join());
            assertSame(boom, seen.get());
        }
    }

    @Test
    void aMainLineRecoveryCatchesLaneFailures() {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            List<Integer> completed = new CopyOnWriteArrayList<>();
            defaultNioFlow.when(x -> x > 10)
                    .then(lane -> lane
                            .submit(x -> {
                                throw new IllegalStateException("lane boom");
                            }))
                    .otherwise(lane -> lane
                            .handle(x -> x))
                    .onErrorResume(error -> -1)
                    .onComplete(completed::add);

            defaultNioFlow.just(20); // fails in the true lane, recovered on the main line
            defaultNioFlow.just(5);  // false lane, flows through untouched
            defaultNioFlow.join();

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
