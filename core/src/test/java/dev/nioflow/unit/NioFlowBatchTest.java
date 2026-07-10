package dev.nioflow.unit;

import dev.nioflow.application.facade.NioFlow;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class NioFlowBatchTest {

    @Test
    void valuesAreGroupedIntoBatchesOfN() {
        try (NioFlow<Integer> nioFlow = new NioFlow<>()) {
            ConcurrentLinkedQueue<Integer> batchSizes = new ConcurrentLinkedQueue<>();
            List<Integer> completed = new CopyOnWriteArrayList<>();
            nioFlow.batch(3, Duration.ofSeconds(5), group -> {
                        batchSizes.add(group.size());
                        return group.stream().map(x -> x * 10).toList();
                    })
                    .onComplete(completed::add);

            nioFlow.justAll(List.of(1, 2, 3, 4, 5, 6));
            nioFlow.join();

            assertEquals(List.of(3, 3), List.copyOf(batchSizes));
            assertEquals(6, completed.size());
            assertTrue(completed.containsAll(List.of(10, 20, 30, 40, 50, 60)));
        }
    }

    @Test
    void aPartialBatchFlushesAfterMaxWait() {
        try (NioFlow<Integer> nioFlow = new NioFlow<>()) {
            ConcurrentLinkedQueue<Integer> batchSizes = new ConcurrentLinkedQueue<>();
            List<Integer> completed = new CopyOnWriteArrayList<>();
            nioFlow.batch(10, Duration.ofMillis(250), group -> {
                        batchSizes.add(group.size());
                        return group;
                    })
                    .onComplete(completed::add);

            nioFlow.just(1);
            nioFlow.just(2);
            nioFlow.join(); // waits until the time-based flush completes the values

            assertEquals(List.of(2), List.copyOf(batchSizes));
            assertEquals(2, completed.size());
        }
    }

    @Test
    void resultsMatchInputsByIndex() {
        try (NioFlow<Integer> nioFlow = new NioFlow<>()) {
            List<Integer> completed = new CopyOnWriteArrayList<>();
            nioFlow.batch(4, Duration.ofSeconds(5), group ->
                            group.stream().map(x -> x + 100).toList())
                    .onComplete(completed::add);

            nioFlow.justAll(List.of(1, 2, 3, 4));
            nioFlow.join();

            assertEquals(4, completed.size());
            assertTrue(completed.containsAll(List.of(101, 102, 103, 104)));
        }
    }

    @Test
    void aFailingBatchFailsEveryValueInIt() throws InterruptedException {
        try (NioFlow<Integer> nioFlow = new NioFlow<>()) {
            CountDownLatch allFailed = new CountDownLatch(3);
            nioFlow.batch(3, Duration.ofSeconds(5), group -> {
                        throw new IllegalStateException("bulk insert failed");
                    })
                    .onError(error -> allFailed.countDown());

            nioFlow.justAll(List.of(1, 2, 3));

            assertTrue(allFailed.await(2, TimeUnit.SECONDS));
            assertThrows(RuntimeException.class, nioFlow::join);
        }
    }

    @Test
    void batchFailuresAreRecoverablePerValue() {
        try (NioFlow<Integer> nioFlow = new NioFlow<>()) {
            List<Integer> completed = new CopyOnWriteArrayList<>();
            nioFlow.batch(3, Duration.ofSeconds(5), group -> {
                        throw new IllegalStateException("bulk insert failed");
                    })
                    .onErrorResume(error -> -1)
                    .onComplete(completed::add);

            nioFlow.justAll(List.of(1, 2, 3));
            nioFlow.join(); // no throw: every value recovered

            assertEquals(List.of(-1, -1, -1), completed);
        }
    }

    @Test
    void aResultCountMismatchFailsTheWholeGroup() throws InterruptedException {
        try (NioFlow<Integer> nioFlow = new NioFlow<>()) {
            CountDownLatch allFailed = new CountDownLatch(2);
            nioFlow.batch(2, Duration.ofSeconds(5), group -> List.of(42)) // 1 result for 2 inputs
                    .onError(error -> allFailed.countDown());

            nioFlow.justAll(List.of(1, 2));

            assertTrue(allFailed.await(2, TimeUnit.SECONDS));
            assertThrows(RuntimeException.class, nioFlow::join);
        }
    }

    @Test
    void batchParametersAreValidated() {
        try (NioFlow<Integer> nioFlow = new NioFlow<>()) {
            assertThrows(IllegalArgumentException.class,
                    () -> nioFlow.batch(0, Duration.ofSeconds(1), group -> group));
            assertThrows(IllegalArgumentException.class,
                    () -> nioFlow.batch(10, Duration.ZERO, group -> group));
        }
    }
}
