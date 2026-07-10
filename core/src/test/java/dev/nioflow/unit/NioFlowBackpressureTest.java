package dev.nioflow.unit;

import dev.nioflow.application.facade.NioFlow;
import dev.nioflow.core.model.Backpressure;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class NioFlowBackpressureTest {

    @Test
    void failPolicyRejectsInjectionsAtCapacity() {
        try (NioFlow<Integer> pipeline = new NioFlow<>(Backpressure.failing(2))) {
            CountDownLatch release = new CountDownLatch(1);
            List<Integer> completed = new CopyOnWriteArrayList<>();
            pipeline.submit(x -> {
                        await(release);
                        return x;
                    })
                    .onComplete(completed::add);

            pipeline.just(1);
            pipeline.just(2);
            assertThrows(RejectedExecutionException.class, () -> pipeline.just(3));

            release.countDown();
            pipeline.join();
            assertEquals(2, completed.size());
            assertTrue(completed.containsAll(List.of(1, 2)));
        }
    }

    @Test
    void dropPolicyDiscardsInjectionsAtCapacity() {
        try (NioFlow<Integer> pipeline = new NioFlow<>(Backpressure.dropping(2))) {
            CountDownLatch release = new CountDownLatch(1);
            List<Integer> completed = new CopyOnWriteArrayList<>();
            pipeline.submit(x -> {
                        await(release);
                        return x;
                    })
                    .onComplete(completed::add);

            pipeline.just(1);
            pipeline.just(2);
            pipeline.just(3); // silently dropped

            release.countDown();
            pipeline.join();
            assertEquals(2, completed.size());
            assertTrue(completed.containsAll(List.of(1, 2)));
        }
    }

    @Test
    void blockPolicyMakesTheProducerWaitForAFreeSlot() throws InterruptedException {
        try (NioFlow<Integer> pipeline = new NioFlow<>(Backpressure.blocking(1))) {
            CountDownLatch releaseFirst = new CountDownLatch(1);
            CountDownLatch secondInjected = new CountDownLatch(1);
            List<Integer> completed = new CopyOnWriteArrayList<>();
            pipeline.submit(x -> {
                        if (x == 1) {
                            await(releaseFirst);
                        }
                        return x;
                    })
                    .onComplete(completed::add);

            pipeline.just(1);
            Thread producer = new Thread(() -> {
                pipeline.just(2);
                secondInjected.countDown();
            });
            producer.start();

            // the producer is held back while value 1 occupies the only slot
            assertFalse(secondInjected.await(150, TimeUnit.MILLISECONDS));

            releaseFirst.countDown(); // value 1 finishes and frees the slot
            assertTrue(secondInjected.await(2, TimeUnit.SECONDS));

            pipeline.join();
            assertEquals(2, completed.size());
            assertTrue(completed.containsAll(List.of(1, 2)));
        }
    }

    @Test
    void capacityFreesAsValuesFinishNotJustAtTheEnd() {
        try (NioFlow<Integer> pipeline = new NioFlow<>(Backpressure.failing(1))) {
            List<Integer> completed = new CopyOnWriteArrayList<>();
            pipeline.handle(x -> x * 10)
                    .onComplete(completed::add);

            for (int i = 1; i <= 5; i++) {
                pipeline.just(i);
                pipeline.join(); // one at a time: the slot is free again for the next
            }

            assertEquals(5, completed.size());
            assertTrue(completed.containsAll(List.of(10, 20, 30, 40, 50)));
        }
    }

    @Test
    void aFilteredValueFreesItsSlot() {
        try (NioFlow<Integer> pipeline = new NioFlow<>(Backpressure.failing(1))) {
            pipeline.filter(x -> false);

            pipeline.just(1);
            assertNull(pipeline.join()); // discarded

            pipeline.just(2); // must not throw: the slot was freed by the drop
            assertNull(pipeline.join());
        }
    }

    @Test
    void blockPolicyPacesABulkInjection() {
        try (NioFlow<Integer> pipeline = new NioFlow<>(Backpressure.blocking(2))) {
            List<Integer> completed = new CopyOnWriteArrayList<>();
            pipeline.handle(x -> x * 2)
                    .onComplete(completed::add);

            for (int i = 1; i <= 50; i++) {
                pipeline.just(i); // blocks whenever 2 values are in flight
            }
            pipeline.join();

            assertEquals(50, completed.size());
        }
    }

    @Test
    void capacityMustBePositive() {
        assertThrows(IllegalArgumentException.class, () -> Backpressure.failing(0));
        assertThrows(IllegalArgumentException.class, () -> Backpressure.blocking(-1));
    }

    private static void await(CountDownLatch latch) {
        try {
            if (!latch.await(5, TimeUnit.SECONDS)) {
                throw new IllegalStateException("latch was never released");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }
}
