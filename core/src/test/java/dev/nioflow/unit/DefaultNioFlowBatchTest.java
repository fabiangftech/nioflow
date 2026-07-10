package dev.nioflow.unit;

import dev.nioflow.application.facade.DefaultNioFlow;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class DefaultNioFlowBatchTest {

    @Test
    void valuesAreGroupedIntoBatchesOfN() {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            ConcurrentLinkedQueue<Integer> batchSizes = new ConcurrentLinkedQueue<>();
            List<Integer> completed = new CopyOnWriteArrayList<>();
            defaultNioFlow.batch(3, Duration.ofSeconds(5), group -> {
                        batchSizes.add(group.size());
                        return group.stream().map(x -> x * 10).toList();
                    })
                    .onComplete(completed::add);

            defaultNioFlow.justAll(List.of(1, 2, 3, 4, 5, 6));
            defaultNioFlow.join();

            assertEquals(List.of(3, 3), List.copyOf(batchSizes));
            assertEquals(6, completed.size());
            assertTrue(completed.containsAll(List.of(10, 20, 30, 40, 50, 60)));
        }
    }

    @Test
    void aPartialBatchFlushesAfterMaxWait() {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            ConcurrentLinkedQueue<Integer> batchSizes = new ConcurrentLinkedQueue<>();
            List<Integer> completed = new CopyOnWriteArrayList<>();
            defaultNioFlow.batch(10, Duration.ofMillis(250), group -> {
                        batchSizes.add(group.size());
                        return group;
                    })
                    .onComplete(completed::add);

            defaultNioFlow.just(1);
            defaultNioFlow.just(2);
            defaultNioFlow.join(); // waits until the time-based flush completes the values

            assertEquals(List.of(2), List.copyOf(batchSizes));
            assertEquals(2, completed.size());
        }
    }

    @Test
    void resultsMatchInputsByIndex() {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            List<Integer> completed = new CopyOnWriteArrayList<>();
            defaultNioFlow.batch(4, Duration.ofSeconds(5), group ->
                            group.stream().map(x -> x + 100).toList())
                    .onComplete(completed::add);

            defaultNioFlow.justAll(List.of(1, 2, 3, 4));
            defaultNioFlow.join();

            assertEquals(4, completed.size());
            assertTrue(completed.containsAll(List.of(101, 102, 103, 104)));
        }
    }

    @Test
    void aFailingBatchFailsEveryValueInIt() throws InterruptedException {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            CountDownLatch allFailed = new CountDownLatch(3);
            defaultNioFlow.batch(3, Duration.ofSeconds(5), group -> {
                        throw new IllegalStateException("bulk insert failed");
                    })
                    .onError(error -> allFailed.countDown());

            defaultNioFlow.justAll(List.of(1, 2, 3));

            assertTrue(allFailed.await(2, TimeUnit.SECONDS));
            assertThrows(RuntimeException.class, defaultNioFlow::join);
        }
    }

    @Test
    void batchFailuresAreRecoverablePerValue() {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            List<Integer> completed = new CopyOnWriteArrayList<>();
            defaultNioFlow.batch(3, Duration.ofSeconds(5), group -> {
                        throw new IllegalStateException("bulk insert failed");
                    })
                    .onErrorResume(error -> -1)
                    .onComplete(completed::add);

            defaultNioFlow.justAll(List.of(1, 2, 3));
            defaultNioFlow.join(); // no throw: every value recovered

            assertEquals(List.of(-1, -1, -1), completed);
        }
    }

    @Test
    void aResultCountMismatchFailsTheWholeGroup() throws InterruptedException {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            CountDownLatch allFailed = new CountDownLatch(2);
            defaultNioFlow.batch(2, Duration.ofSeconds(5), group -> List.of(42)) // 1 result for 2 inputs
                    .onError(error -> allFailed.countDown());

            defaultNioFlow.justAll(List.of(1, 2));

            assertTrue(allFailed.await(2, TimeUnit.SECONDS));
            assertThrows(RuntimeException.class, defaultNioFlow::join);
        }
    }

    @Test
    void batchParametersAreValidated() {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            assertThrows(IllegalArgumentException.class,
                    () -> defaultNioFlow.batch(0, Duration.ofSeconds(1), group -> group));
            assertThrows(IllegalArgumentException.class,
                    () -> defaultNioFlow.batch(10, Duration.ZERO, group -> group));
        }
    }
}
