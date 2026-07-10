package dev.nioflow.unit;

import dev.nioflow.application.facade.NioFlow;
import dev.nioflow.core.model.Backpressure;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class NioFlowShutdownTest {

    @Test
    void closeDrainsInFlightValues() {
        NioFlow<Integer> pipeline = new NioFlow<>();
        List<Integer> results = new CopyOnWriteArrayList<>();
        pipeline.submit(x -> {
                    sleep(200);
                    return x * 10;
                })
                .handle(x -> {
                    results.add(x);
                    return x;
                });

        pipeline.just(1);
        pipeline.close(); // no join: close itself must wait for the in-flight value

        assertEquals(List.of(10), results);
    }

    @Test
    void closeGivesUpAfterTheGracePeriod() {
        NioFlow<Integer> pipeline = new NioFlow<>();
        CountDownLatch never = new CountDownLatch(1);
        List<Integer> results = new CopyOnWriteArrayList<>();
        pipeline.submit(x -> {
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

        pipeline.just(1);
        long start = System.nanoTime();
        pipeline.close(Duration.ofMillis(200));
        long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

        assertTrue(elapsedMillis >= 200, "close returned before the grace period");
        assertTrue(elapsedMillis < 3000, "close waited past the grace period: " + elapsedMillis + "ms");
        assertTrue(results.isEmpty(), "the stuck value should have been dropped");
        never.countDown();
    }

    @Test
    void closeIsIdempotent() {
        NioFlow<Integer> pipeline = new NioFlow<>();
        assertEquals(2, pipeline.just(1).handle(x -> x + 1).join());

        pipeline.close();
        pipeline.close(); // second close returns immediately, no exception
    }

    @Test
    void justAfterCloseIsRejected() {
        NioFlow<Integer> pipeline = new NioFlow<>();
        assertEquals(2, pipeline.just(1).handle(x -> x + 1).join());
        pipeline.close();

        assertThrows(RejectedExecutionException.class, () -> pipeline.just(2));
    }

    @Test
    void aProducerBlockedByBackpressureIsReleasedWhenThePipelineCloses() throws InterruptedException {
        NioFlow<Integer> pipeline = new NioFlow<>(Backpressure.blocking(1));
        CountDownLatch never = new CountDownLatch(1);
        pipeline.submit(x -> {
            try {
                never.await(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return x;
        });
        pipeline.just(1); // occupies the only slot forever

        AtomicReference<Throwable> producerFailure = new AtomicReference<>();
        CountDownLatch producerDone = new CountDownLatch(1);
        Thread producer = new Thread(() -> {
            try {
                pipeline.just(2);
            } catch (Throwable t) {
                producerFailure.set(t);
            }
            producerDone.countDown();
        });
        producer.start();

        pipeline.close(Duration.ofMillis(200));

        assertTrue(producerDone.await(2, TimeUnit.SECONDS), "the producer stayed blocked after close");
        assertInstanceOf(RejectedExecutionException.class, producerFailure.get());
        never.countDown();
    }

    @Test
    void joinAfterAForcedCloseThrowsInsteadOfHanging() {
        NioFlow<Integer> pipeline = new NioFlow<>();
        CountDownLatch never = new CountDownLatch(1);
        pipeline.submit(x -> {
            try {
                never.await(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return x;
        });
        pipeline.just(1);
        pipeline.close(Duration.ofMillis(100)); // gives up on the stuck value

        CompletionException thrown = assertThrows(CompletionException.class, pipeline::join);
        assertInstanceOf(IllegalStateException.class, thrown.getCause());
        never.countDown();
    }

    @Test
    void closingOnePipelineDoesNotAffectAnotherSharingTheExecutor() {
        ExecutorService shared = Executors.newVirtualThreadPerTaskExecutor();
        try {
            NioFlow<Integer> first = new NioFlow<>(shared);
            assertEquals(2, first.just(1).handle(x -> x + 1).join());
            first.close();

            try (NioFlow<Integer> second = new NioFlow<>(shared)) {
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
