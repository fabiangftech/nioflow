package dev.nioflow.unit;

import dev.nioflow.application.facade.DefaultNioFlow;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

class DefaultNioFlowTimeoutTest {

    @Test
    void aSlowSubmitTimesOutAndFailsOnlyThatValue() throws InterruptedException {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            List<Integer> completed = new CopyOnWriteArrayList<>();
            List<Throwable> errors = new CopyOnWriteArrayList<>();
            CountDownLatch failed = new CountDownLatch(1);
            defaultNioFlow.submit(x -> {
                        if (x == 1) {
                            sleep(5_000);
                        }
                        return x * 10;
                    }, Duration.ofMillis(200))
                    .onComplete(completed::add)
                    .onError(error -> {
                        errors.add(error);
                        failed.countDown();
                    });

            defaultNioFlow.just(1);
            defaultNioFlow.just(2);

            assertTrue(failed.await(2, TimeUnit.SECONDS));
            assertInstanceOf(TimeoutException.class, errors.getFirst());

            assertThrows(CompletionException.class, defaultNioFlow::join);
            assertEquals(List.of(20), completed);
        }
    }

    @Test
    void aSubmitWithinItsTimeoutSucceeds() {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            int result = defaultNioFlow.just(2)
                    .submit(x -> x * 10, Duration.ofSeconds(5))
                    .join();

            assertEquals(20, result);
        }
    }

    @Test
    void joinWithTimeoutThrowsWhileValuesAreStillRunning() {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            CountDownLatch release = new CountDownLatch(1);
            defaultNioFlow.submit(x -> {
                try {
                    release.await(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return x;
            });
            defaultNioFlow.just(7);

            CompletionException thrown = assertThrows(CompletionException.class,
                    () -> defaultNioFlow.join(Duration.ofMillis(200)));
            assertInstanceOf(TimeoutException.class, thrown.getCause());

            // the value kept running: releasing it lets a later join finish normally
            release.countDown();
            assertEquals(7, defaultNioFlow.join());
        }
    }

    @Test
    void joinWithTimeoutReturnsNormallyWhenWorkFinishesInTime() {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            int result = defaultNioFlow.just(3)
                    .submit(x -> x + 39)
                    .join(Duration.ofSeconds(5));

            assertEquals(42, result);
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
