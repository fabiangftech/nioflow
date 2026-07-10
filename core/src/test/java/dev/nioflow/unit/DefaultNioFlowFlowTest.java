package dev.nioflow.unit;

import dev.nioflow.application.facade.DefaultNioFlow;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class DefaultNioFlowFlowTest {

    @Test
    void aSlowValueDoesNotBlockTheValuesBehindIt() throws InterruptedException {
        try (DefaultNioFlow<String> defaultNioFlow = new DefaultNioFlow<>()) {
            CountDownLatch slowRelease = new CountDownLatch(1);
            CountDownLatch fastFinished = new CountDownLatch(1);
            List<String> finished = new CopyOnWriteArrayList<>();
            defaultNioFlow.submit(x -> {
                        if (x.equals("slow")) {
                            await(slowRelease);
                        }
                        return x;
                    })
                    .handle(x -> {
                        finished.add(x);
                        if (x.equals("fast")) {
                            fastFinished.countDown();
                        }
                        return x;
                    });

            defaultNioFlow.just("slow");
            defaultNioFlow.just("fast");

            // fast overtakes slow, which is still parked inside its submit stage
            assertTrue(fastFinished.await(2, TimeUnit.SECONDS));
            assertEquals(List.of("fast"), finished);

            slowRelease.countDown();
            defaultNioFlow.join();
            assertEquals(List.of("fast", "slow"), finished);
        }
    }

    @Test
    void everyInjectedValueWalksTheWholeChain() {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            List<Integer> results = new CopyOnWriteArrayList<>();
            defaultNioFlow.handle(x -> x * 10)
                    .submit(x -> x + 1)
                    .handle(x -> {
                        results.add(x);
                        return x;
                    });

            for (int i = 1; i <= 5; i++) {
                defaultNioFlow.just(i);
            }
            defaultNioFlow.join();

            assertEquals(5, results.size());
            assertTrue(results.containsAll(List.of(11, 21, 31, 41, 51)));
        }
    }

    @Test
    void aFailingValueDoesNotStopTheOthers() throws InterruptedException {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            CountDownLatch failed = new CountDownLatch(1);
            List<Integer> results = new CopyOnWriteArrayList<>();
            defaultNioFlow.submit(x -> {
                        if (x == 2) {
                            throw new IllegalStateException("value 2 boom");
                        }
                        return x * 10;
                    })
                    .handle(x -> {
                        results.add(x);
                        return x;
                    })
                    .onError(error -> failed.countDown());

            defaultNioFlow.just(1);
            defaultNioFlow.just(2);
            defaultNioFlow.just(3);

            assertTrue(failed.await(2, TimeUnit.SECONDS));
            awaitSize(results, 2);
            assertTrue(results.containsAll(List.of(10, 30)));
        }
    }

    @Test
    void manyValuesAllCompleteThroughSlowStages() {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            List<Integer> results = new CopyOnWriteArrayList<>();
            defaultNioFlow.submit(x -> {
                        sleep(x % 5);
                        return x * 2;
                    })
                    .handle(x -> {
                        results.add(x);
                        return x;
                    });

            for (int i = 1; i <= 50; i++) {
                defaultNioFlow.just(i);
            }
            defaultNioFlow.join();

            assertEquals(50, results.size());
            assertEquals(2550, results.stream().mapToInt(Integer::intValue).sum());
        }
    }

    @Test
    void aStageCanInjectNewValuesWithoutDeadlocking() {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            List<Integer> results = new CopyOnWriteArrayList<>();
            defaultNioFlow.handle(x -> {
                        if (x < 3) {
                            defaultNioFlow.just(x + 1);
                        }
                        return x;
                    })
                    .handle(x -> {
                        results.add(x);
                        return x;
                    });

            defaultNioFlow.just(1);
            defaultNioFlow.join();

            assertEquals(3, results.size());
            assertTrue(results.containsAll(List.of(1, 2, 3)));
        }
    }

    @Test
    void aSlowHandleDoesNotBlockTheValuesBehindIt() throws InterruptedException {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            CountDownLatch slowEntered = new CountDownLatch(1);
            CountDownLatch slowRelease = new CountDownLatch(1);
            CountDownLatch fastFinished = new CountDownLatch(1);
            List<Integer> finished = new CopyOnWriteArrayList<>();
            defaultNioFlow.handle(x -> {
                        if (x == 1) {
                            slowEntered.countDown();
                            await(slowRelease);
                        }
                        return x;
                    })
                    .handle(x -> {
                        finished.add(x);
                        if (x == 2) {
                            fastFinished.countDown();
                        }
                        return x;
                    });

            defaultNioFlow.just(1);
            assertTrue(slowEntered.await(2, TimeUnit.SECONDS));
            defaultNioFlow.just(2);

            // value 2 finishes while value 1 is still stuck inside its handle
            assertTrue(fastFinished.await(2, TimeUnit.SECONDS));
            assertEquals(List.of(2), finished);

            slowRelease.countDown();
            defaultNioFlow.join();
            assertEquals(List.of(2, 1), finished);
        }
    }

    @Test
    void justAllInjectsEveryValueInIterationOrder() {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            List<Integer> completed = new CopyOnWriteArrayList<>();
            defaultNioFlow.handle(x -> x * 10)
                    .onComplete(completed::add);

            defaultNioFlow.justAll(List.of(1, 2, 3, 4, 5));

            // join returns the newest injected value's result: the last of the iterable
            assertEquals(50, defaultNioFlow.join());
            assertEquals(5, completed.size());
            assertTrue(completed.containsAll(List.of(10, 20, 30, 40, 50)));
        }
    }

    @Test
    void justAllWithAnEmptyIterableDoesNothing() {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            defaultNioFlow.handle(x -> x * 10);

            defaultNioFlow.justAll(List.of());

            assertNull(defaultNioFlow.join());
        }
    }

    @Test
    void joinReturnsTheResultOfTheNewestInjectedValue() {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            defaultNioFlow.submit(x -> {
                if (x == 1) {
                    sleep(150); // the older value finishes last
                }
                return x * 10;
            });

            defaultNioFlow.just(1);
            defaultNioFlow.just(2);

            // deterministic: injection order decides, not completion order
            assertEquals(20, defaultNioFlow.join());
        }
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
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

    private static void awaitSize(List<?> list, int size) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        while (list.size() < size && System.nanoTime() < deadline) {
            Thread.onSpinWait();
        }
        assertEquals(size, list.size());
    }
}
