package dev.nioflow.stress;

import dev.nioflow.application.facade.DefaultNioEngine;
import dev.nioflow.application.facade.DefaultNioFlow;
import dev.nioflow.core.facade.NioFlow;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Hammers one batch link from many threads so size flushes and window
 * flushes race constantly (size 8, window 5ms, bursty arrival). Every
 * caller must get ITS OWN result exactly once — a lost continuation shows
 * up as a hung future (cut by orTimeout), a cross-mapped one as a wrong
 * value, and a double flush as too many bulk invocations.
 */
class BatchStressTest {

    @Test
    void concurrentCallersAlwaysGetTheirOwnResult() throws Exception {
        var engine = new DefaultNioEngine();
        var bulkCalls = new AtomicInteger();
        var batchedValues = new AtomicInteger();
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        flow.batch("bulk", 8, Duration.ofMillis(5), values -> {
            bulkCalls.incrementAndGet();
            batchedValues.addAndGet(values.size());
            return values.stream().map(value -> value * 10).toList();
        });
        engine.seal();

        int producers = 8;
        int perProducer = 250;
        var start = new CountDownLatch(1);
        List<CompletableFuture<Void>> verifiers = new ArrayList<>();
        for (int p = 0; p < producers; p++) {
            int base = p * perProducer;
            verifiers.add(CompletableFuture.runAsync(() -> {
                try {
                    start.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                List<CompletableFuture<Integer>> calls = new ArrayList<>(perProducer);
                for (int i = 0; i < perProducer; i++) {
                    int input = base + i;
                    calls.add(flow.just(input).executeAsync());
                    if (i % 40 == 0) {
                        LockSupportSleep.sleepMillis(3); // let some windows fire on partial batches
                    }
                }
                for (int i = 0; i < perProducer; i++) {
                    int input = base + i;
                    assertEquals(input * 10, calls.get(i).orTimeout(30, TimeUnit.SECONDS).join(),
                            "caller " + input + " got someone else's result");
                }
            }));
        }
        start.countDown();
        for (CompletableFuture<Void> verifier : verifiers) {
            verifier.orTimeout(60, TimeUnit.SECONDS).join();
        }

        int total = producers * perProducer;
        assertEquals(total, batchedValues.get(), "every value must be batched exactly once");
        assertTrue(bulkCalls.get() < total, "batching must coalesce (got " + bulkCalls.get()
                + " bulk calls for " + total + " values)");
        engine.shutdown(Duration.ofSeconds(1));
    }

    private static final class LockSupportSleep {
        static void sleepMillis(long millis) {
            java.util.concurrent.locks.LockSupport.parkNanos(millis * 1_000_000);
        }
    }
}
