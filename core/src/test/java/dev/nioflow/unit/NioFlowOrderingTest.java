package dev.nioflow.unit;

import dev.nioflow.application.facade.NioFlow;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class NioFlowOrderingTest {

    @Test
    void runsStagesInDeclarationOrder() {
        try (NioFlow<Integer> nioFlow = new NioFlow<>()) {
            List<String> order = new CopyOnWriteArrayList<>();
            int result = nioFlow.just(1)
                    .handle(x -> {
                        order.add("handle-1");
                        return x + 1;
                    })
                    .handle(x -> {
                        order.add("handle-2");
                        return x * 5;
                    })
                    .submit(x -> {
                        order.add("submit-1");
                        sleep(50);
                        return x + 10;
                    })
                    .handle(x -> {
                        order.add("handle-3");
                        return x * 2;
                    })
                    .submit(x -> {
                        order.add("submit-2");
                        return x + 2;
                    })
                    .join();

            assertEquals(42, result);
            assertEquals(List.of("handle-1", "handle-2", "submit-1", "handle-3", "submit-2"), order);
        }
    }

    @Test
    void submitDoesNotBlockTheCallerThread() {
        try (NioFlow<Integer> nioFlow = new NioFlow<>()) {
            AtomicReference<String> submitThread = new AtomicReference<>();
            long start = System.nanoTime();
            nioFlow.just(1).submit(x -> {
                submitThread.set(Thread.currentThread().getName());
                sleep(200);
                return x;
            });
            long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

            assertTrue(elapsedMillis < 150, "submit blocked the caller for " + elapsedMillis + "ms");
            nioFlow.join();
            assertNotEquals(Thread.currentThread().getName(), submitThread.get());
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
