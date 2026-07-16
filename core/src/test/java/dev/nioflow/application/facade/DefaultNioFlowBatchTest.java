package dev.nioflow.application.facade;

import dev.nioflow.core.facade.NioFlow;
import dev.nioflow.core.model.Batch;
import dev.nioflow.core.model.Splice;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * batch(size, window, bulk): executions park at the batch link until size
 * or window, ONE bulk call maps all their values positionally, and each
 * execution continues its own chain with its own element — callers never
 * see the batch. Timing assertions only use bounds loose enough to hold
 * under load.
 */
class DefaultNioFlowBatchTest extends EngineTestSupport {

    @Test
    void sizeTriggerFlushesWithoutWaitingForTheWindow() {
        var bulkCalls = new AtomicInteger();
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        flow.batch("bulk", 3, Duration.ofSeconds(30), values -> {
            bulkCalls.incrementAndGet();
            return values.stream().map(value -> value * 10).toList();
        });
        engine.seal();

        long start = System.nanoTime();
        List<CompletableFuture<Integer>> calls = new ArrayList<>();
        for (int i = 1; i <= 3; i++) {
            calls.add(flow.just(i).executeAsync());
        }
        assertEquals(10, calls.get(0).orTimeout(5, TimeUnit.SECONDS).join());
        assertEquals(20, calls.get(1).join());
        assertEquals(30, calls.get(2).join());

        long elapsedMillis = (System.nanoTime() - start) / 1_000_000;
        assertTrue(elapsedMillis < 5_000, "size flush must not wait for the 30s window, took " + elapsedMillis + "ms");
        assertEquals(1, bulkCalls.get(), "three executions must coalesce into ONE bulk call");
    }

    @Test
    void windowTriggerFlushesAPartialBatch() {
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        flow.batch("bulk", 100, Duration.ofMillis(150), values ->
                values.stream().map(value -> value + 1000).toList());

        long start = System.nanoTime();
        CompletableFuture<Integer> first = flow.just(1).executeAsync();
        CompletableFuture<Integer> second = flow.just(2).executeAsync();

        assertEquals(1001, first.orTimeout(5, TimeUnit.SECONDS).join());
        assertEquals(1002, second.join());
        long elapsedMillis = (System.nanoTime() - start) / 1_000_000;
        assertTrue(elapsedMillis >= 100, "a partial batch waits for its window, took only " + elapsedMillis + "ms");
    }

    @Test
    void aWindowFlushRunsOffTheSharedTimerThread() {
        // RFC 0041: the window flush is STAGED to a worker, so neither the bulk
        // NOR the group-lock swap runs on the one thread that ticks every timeout
        // and window in the JVM — a boss adding to a batch never contends it.
        var bulkThread = new java.util.concurrent.atomic.AtomicReference<String>();
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        flow.batch("bulk", 100, Duration.ofMillis(100), values -> {
            bulkThread.set(Thread.currentThread().getName());
            return values.stream().map(value -> value + 1).toList();
        });

        assertEquals(2, flow.just(1).executeAsync().orTimeout(5, TimeUnit.SECONDS).join());
        assertNotEquals("nio-flow-timer", bulkThread.get(),
                "a window flush must not run on the shared TimerWheel thread");
    }

    @Test
    void downstreamContinuesPerExecutionWithItsOwnElement() {
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        flow.batch("bulk", 2, Duration.ofSeconds(30), values ->
                        values.stream().map(value -> value * 10).toList())
                .handle("own-tail", value -> value + 1);
        engine.seal();

        CompletableFuture<Integer> first = flow.just(1).executeAsync();
        CompletableFuture<Integer> second = flow.just(2).executeAsync();

        assertEquals(11, first.orTimeout(5, TimeUnit.SECONDS).join());
        assertEquals(21, second.join());
    }

    @Test
    void bulkFailureFailsEveryBatchedExecutionRecoverably() {
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        flow.batch("bulk", 2, Duration.ofSeconds(30), values -> {
                    throw new IllegalStateException("bulk boom");
                })
                .recover("net", error -> -1);

        CompletableFuture<Integer> first = flow.just(1).executeAsync();
        CompletableFuture<Integer> second = flow.just(2).executeAsync();

        assertEquals(-1, first.orTimeout(5, TimeUnit.SECONDS).join());
        assertEquals(-1, second.join());
    }

    @Test
    void wrongSizedBulkResultFailsTheWholeBatch() {
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        flow.batch("bulk", 2, Duration.ofSeconds(30), values -> List.of(99));

        CompletableFuture<Integer> first = flow.just(1).executeAsync().orTimeout(5, TimeUnit.SECONDS);
        CompletableFuture<Integer> second = flow.just(2).executeAsync().orTimeout(5, TimeUnit.SECONDS);

        var failure = assertThrows(CompletionException.class, first::join);
        assertInstanceOf(IllegalStateException.class, failure.getCause());
        assertThrows(CompletionException.class, second::join);
    }

    @Test
    void batchInsideALaneOnlyPoolsRoutedValues() {
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        flow.when(value -> value % 2 == 0)
                .then(lane -> lane.batch("even-bulk", 2, Duration.ofSeconds(30), values ->
                        values.stream().map(value -> value * 10).toList()))
                .otherwise(lane -> lane.handle(value -> -value));
        engine.seal();

        // The odd value never parks: it takes the otherwise lane immediately
        // even while the even batch is still accumulating.
        CompletableFuture<Integer> evenA = flow.just(2).executeAsync();
        assertEquals(-3, flow.just(3).execute());
        CompletableFuture<Integer> evenB = flow.just(4).executeAsync();

        assertEquals(20, evenA.orTimeout(5, TimeUnit.SECONDS).join());
        assertEquals(40, evenB.join());
    }

    @Test
    void consecutiveBatchesReuseTheGroupCleanly() {
        var bulkCalls = new AtomicInteger();
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        flow.batch("bulk", 2, Duration.ofSeconds(30), values -> {
            bulkCalls.incrementAndGet();
            return values.stream().map(value -> value * 10).toList();
        });
        engine.seal();

        for (int round = 0; round < 3; round++) {
            CompletableFuture<Integer> a = flow.just(1).executeAsync();
            CompletableFuture<Integer> b = flow.just(2).executeAsync();
            assertEquals(10, a.orTimeout(5, TimeUnit.SECONDS).join());
            assertEquals(20, b.join());
        }
        assertEquals(3, bulkCalls.get());
    }

    @Test
    void batchIsASpliceAnchor() {
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        flow.batch("bulk", 1, Duration.ofSeconds(1), values ->
                values.stream().map(value -> value * 10).toList());
        engine.seal();
        assertEquals(70, flow.just(7).execute());

        engine.splice("bulk", Splice.REPLACE, List.of(new Batch("bulk", 1, Duration.ofSeconds(1),
                values -> values.stream().map(value -> ((int) value) * 100).map(value -> (Object) value).toList(),
                List.of())));

        assertEquals(700, flow.just(7).execute());
    }

    @Test
    void invalidBatchConfigurationsAreRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> new Batch("bad", 0, Duration.ofSeconds(1), values -> values, List.of()));
        assertThrows(IllegalArgumentException.class,
                () -> new Batch("bad", 2, Duration.ZERO, values -> values, List.of()));
    }
}
