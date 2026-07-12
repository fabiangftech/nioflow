package dev.nioflow.stress;

import dev.nioflow.application.facade.DefaultNioEngine;
import dev.nioflow.application.facade.DefaultNioFlow;
import dev.nioflow.core.facade.NioFlow;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Keyed ordering under chaos: each producer thread owns ONE key and fires
 * its values in sequence while 7 other producers and jittery stage
 * latencies try to shuffle everything. Per key, the processed sequence
 * must equal the submitted sequence exactly — an out-of-order pair means
 * the FIFO lane broke, a missing value means a lost handoff (orTimeout
 * turns the hang into a failure).
 */
class KeyedExecutionStressTest {

    @Test
    void perKeyOrderSurvivesConcurrentProducersAndJitter() {
        var engine = new DefaultNioEngine();
        Map<String, List<Integer>> processedByKey = new ConcurrentHashMap<>();
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        flow.handle("jittery", value -> {
            LockSupport.parkNanos(((value * 31) % 7) * 500_000L);
            return value;
        }).handle("record", value -> {
            processedByKey.computeIfAbsent("key-" + value / 1000, ignored -> new ArrayList<>())
                    .add(value);
            return value;
        });
        engine.seal();

        int producers = 8;
        int perProducer = 200;
        var start = new CountDownLatch(1);
        List<CompletableFuture<Void>> drivers = new ArrayList<>();
        for (int p = 0; p < producers; p++) {
            String key = "key-" + p;
            int base = p * 1000;
            drivers.add(CompletableFuture.runAsync(() -> {
                try {
                    start.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                List<CompletableFuture<Integer>> calls = new ArrayList<>(perProducer);
                for (int i = 0; i < perProducer; i++) {
                    calls.add(flow.just(base + i).key(key).executeAsync());
                }
                calls.forEach(call -> call.orTimeout(60, TimeUnit.SECONDS).join());
            }));
        }
        start.countDown();
        drivers.forEach(driver -> driver.orTimeout(120, TimeUnit.SECONDS).join());

        for (int p = 0; p < producers; p++) {
            int base = p * 1000;
            List<Integer> expected = new ArrayList<>(perProducer);
            for (int i = 0; i < perProducer; i++) {
                expected.add(base + i);
            }
            assertEquals(expected, processedByKey.get("key-" + p),
                    "key-" + p + " processed out of submission order");
        }
        engine.shutdown(Duration.ofSeconds(1));
    }
}
