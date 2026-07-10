package dev.nioflow.unit;

import dev.nioflow.application.facade.NioFlow;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

class NioFlowExecutorTest {

    @Test
    void blockingHandlesScaleBeyondCpuCoresByDefault() throws InterruptedException {
        int concurrency = 32; // well above any CI core count
        try (NioFlow<Integer> nioFlow = new NioFlow<>()) {
            CountDownLatch allEntered = new CountDownLatch(concurrency);
            CountDownLatch release = new CountDownLatch(1);
            nioFlow.handle(x -> {
                allEntered.countDown();
                try {
                    release.await(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return x;
            });

            for (int i = 0; i < concurrency; i++) {
                nioFlow.just(i);
            }

            // every value blocks inside its handle at the same time: business IO in
            // handles must not be capped by a core-sized pool
            assertTrue(allEntered.await(3, TimeUnit.SECONDS),
                    "blocking handles must run on virtual workers, not a core-bound pool");
            release.countDown();
            nioFlow.join();
        }
    }

    @Test
    void aFixedHandlePoolBoundsSyncParallelism() throws InterruptedException {
        ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
        try (NioFlow<Integer> nioFlow = new NioFlow<>(executor, 2)) {
            CountDownLatch firstTwoEntered = new CountDownLatch(2);
            CountDownLatch thirdEntered = new CountDownLatch(3);
            CountDownLatch release = new CountDownLatch(1);
            nioFlow.handle(x -> {
                firstTwoEntered.countDown();
                thirdEntered.countDown();
                try {
                    release.await(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return x;
            });

            nioFlow.justAll(List.of(1, 2, 3, 4));

            assertTrue(firstTwoEntered.await(2, TimeUnit.SECONDS));
            // both workers are occupied: a third handle cannot start
            assertFalse(thirdEntered.await(200, TimeUnit.MILLISECONDS),
                    "a fixed pool of 2 must not run a third handle concurrently");

            release.countDown();
            nioFlow.join();
        } finally {
            executor.shutdown();
        }
    }

    @Test
    void defaultConstructorRunsAsyncStagesOnVirtualThreads() {
        try (NioFlow<Integer> nioFlow = new NioFlow<>()) {
            AtomicBoolean virtual = new AtomicBoolean();
            int result = nioFlow.just(20)
                    .submit(x -> {
                        virtual.set(Thread.currentThread().isVirtual());
                        return x + 22;
                    })
                    .join();

            assertEquals(42, result);
            assertTrue(virtual.get());
        }
    }

    @Test
    void acceptsAVirtualThreadPerTaskExecutor() {
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
             NioFlow<Integer> nioFlow = new NioFlow<>(executor)) {
            AtomicBoolean virtual = new AtomicBoolean();
            int result = nioFlow.just(1)
                    .submit(x -> {
                        virtual.set(Thread.currentThread().isVirtual());
                        return x * 2;
                    })
                    .join();

            assertEquals(2, result);
            assertTrue(virtual.get());
        }
    }

    @Test
    void singleThreadExecutorStillPreservesStageOrder() {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try (NioFlow<Integer> nioFlow = new NioFlow<>(executor)) {
            List<String> order = new CopyOnWriteArrayList<>();
            int result = nioFlow.just(1)
                    .submit(x -> {
                        order.add("submit-1");
                        return x + 1;
                    })
                    .handle(x -> {
                        order.add("handle-1");
                        return x * 10;
                    })
                    .submit(x -> {
                        order.add("submit-2");
                        return x + 1;
                    })
                    .join();

            assertEquals(21, result);
            assertEquals(List.of("submit-1", "handle-1", "submit-2"), order);
        } finally {
            executor.shutdown();
        }
    }

    @Test
    void closeDoesNotShutDownAnExternallySuppliedExecutor() {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            try (NioFlow<Integer> nioFlow = new NioFlow<>(executor)) {
                assertEquals(2, nioFlow.just(1).submit(x -> x + 1).join());
            }
            assertFalse(executor.isShutdown());
        } finally {
            executor.shutdown();
        }
    }
}
