package dev.nioflow.unit;

import dev.nioflow.application.facade.DefaultNioFlow;
import dev.nioflow.core.model.Backpressure;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class DefaultNioFlowShutdownTest {

    @Test
    void closeDrainsInFlightValues() {
        DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>();
        List<Integer> results = new CopyOnWriteArrayList<>();
        defaultNioFlow.submit(x -> {
                    sleep(200);
                    return x * 10;
                })
                .handle(x -> {
                    results.add(x);
                    return x;
                });

        defaultNioFlow.just(1);
        defaultNioFlow.close(); // no join: close itself must wait for the in-flight value

        assertEquals(List.of(10), results);
    }

    @Test
    void closeGivesUpAfterTheGracePeriod() {
        DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>();
        CountDownLatch never = new CountDownLatch(1);
        List<Integer> results = new CopyOnWriteArrayList<>();
        defaultNioFlow.submit(x -> {
                    try {
                        never.await(5, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    return x;
                })
                .handle(x -> {
                    results.add(x);
                    return x;
                });

        defaultNioFlow.just(1);
        long start = System.nanoTime();
        defaultNioFlow.close(Duration.ofMillis(200));
        long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

        assertTrue(elapsedMillis >= 200, "close returned before the grace period");
        assertTrue(elapsedMillis < 3000, "close waited past the grace period: " + elapsedMillis + "ms");
        assertTrue(results.isEmpty(), "the stuck value should have been dropped");
        never.countDown();
    }

    @Test
    void closeIsIdempotent() {
        DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>();
        assertEquals(2, defaultNioFlow.just(1).handle(x -> x + 1).join());

        defaultNioFlow.close();
        defaultNioFlow.close(); // second close returns immediately, no exception
    }

    @Test
    void justAfterCloseIsRejected() {
        DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>();
        assertEquals(2, defaultNioFlow.just(1).handle(x -> x + 1).join());
        defaultNioFlow.close();

        assertThrows(RejectedExecutionException.class, () -> defaultNioFlow.just(2));
    }

    @Test
    void aProducerBlockedByBackpressureIsReleasedWhenThePipelineCloses() throws InterruptedException {
        DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>(Backpressure.blocking(1));
        CountDownLatch never = new CountDownLatch(1);
        defaultNioFlow.submit(x -> {
            try {
                never.await(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return x;
        });
        defaultNioFlow.just(1); // occupies the only slot forever

        AtomicReference<Throwable> producerFailure = new AtomicReference<>();
        CountDownLatch producerDone = new CountDownLatch(1);
        Thread producer = new Thread(() -> {
            try {
                defaultNioFlow.just(2);
            } catch (Throwable t) {
                producerFailure.set(t);
            }
            producerDone.countDown();
        });
        producer.start();

        defaultNioFlow.close(Duration.ofMillis(200));

        assertTrue(producerDone.await(2, TimeUnit.SECONDS), "the producer stayed blocked after close");
        assertInstanceOf(RejectedExecutionException.class, producerFailure.get());
        never.countDown();
    }

    @Test
    void joinAfterAForcedCloseThrowsInsteadOfHanging() {
        DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>();
        CountDownLatch never = new CountDownLatch(1);
        defaultNioFlow.submit(x -> {
            try {
                never.await(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return x;
        });
        defaultNioFlow.just(1);
        defaultNioFlow.close(Duration.ofMillis(100)); // gives up on the stuck value

        CompletionException thrown = assertThrows(CompletionException.class, defaultNioFlow::join);
        assertInstanceOf(IllegalStateException.class, thrown.getCause());
        never.countDown();
    }

    @Test
    void closingOnePipelineDoesNotAffectAnotherSharingTheExecutor() {
        ExecutorService shared = Executors.newVirtualThreadPerTaskExecutor();
        try {
            DefaultNioFlow<Integer> first = new DefaultNioFlow<>(shared);
            assertEquals(2, first.just(1).handle(x -> x + 1).join());
            first.close();

            try (DefaultNioFlow<Integer> second = new DefaultNioFlow<>(shared)) {
                assertEquals(42, second.just(21).submit(x -> x * 2).join());
            }
        } finally {
            shared.shutdown();
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
