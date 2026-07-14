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
 * Fork storm: many concurrent requests, each detaching several sub-flows that
 * branch, fail and recover. Two things must hold no matter the interleaving —
 * every REQUEST future completes with the main line's value (a fork must never
 * fail it, and never hang it), and every fork eventually runs.
 *
 * <p>The futures carry an orTimeout: a fork bug that kills a boss task would
 * otherwise leave them hanging forever instead of failing the test.
 */
class ForkStormStressTest {

    private static final int PRODUCERS = 8;
    private static final int PER_PRODUCER = 200;
    private static final int FORKS_PER_REQUEST = 3;

    @Test
    void forksNeverFailNorHangTheRequestsThatSpawnedThem() throws Exception {
        var engine = new DefaultNioEngine();
        var forksRun = new AtomicInteger();
        var forkFailures = new AtomicInteger();
        var recoveredInFork = new AtomicInteger();
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);

        flow.onError(error -> forkFailures.incrementAndGet());
        flow.handle("pre", value -> value + 1);
        for (int f = 0; f < FORKS_PER_REQUEST; f++) {
            int id = f;
            flow.fork("fork-" + f, sub -> sub
                    .handleSync("inline", value -> value * 2)
                    .match()
                        // Every third fork blows up — and recovers inside itself,
                        // so nothing must reach the error handler.
                        .is(value -> id == 2, lane -> lane.handle("boom", value -> {
                            throw new IllegalStateException("fork " + id);
                        }))
                        .otherwise(lane -> lane.handle("work", value -> value + id))
                    .recover(error -> {
                        recoveredInFork.incrementAndGet();
                        return -1;
                    })
                    // A stage, not a background: the fork's completion is what
                    // the drain waits for, and a background effect is not part
                    // of it (it is fire-and-forget by definition).
                    .handle("count", value -> {
                        forksRun.incrementAndGet();
                        return value;
                    }));
        }
        flow.handle("post", value -> value * 10);
        engine.seal();

        var start = new CountDownLatch(1);
        List<CompletableFuture<Void>> producers = new ArrayList<>();
        for (int p = 0; p < PRODUCERS; p++) {
            producers.add(CompletableFuture.runAsync(() -> {
                awaitQuietly(start);
                for (int i = 0; i < PER_PRODUCER; i++) {
                    int input = i;
                    // The request must see ONLY the main line: (input + 1) * 10.
                    Integer result = flow.just(input)
                            .executeAsync()
                            .orTimeout(10, TimeUnit.SECONDS)
                            .join();
                    assertEquals((input + 1) * 10, result);
                }
            }));
        }
        start.countDown();
        CompletableFuture.allOf(producers.toArray(CompletableFuture[]::new))
                .orTimeout(60, TimeUnit.SECONDS)
                .join();

        int requests = PRODUCERS * PER_PRODUCER;
        // The drain is what guarantees the forks finished: they are in-flight
        // work, so a clean shutdown means none was left behind.
        assertEquals(0, engine.shutdown(Duration.ofSeconds(20)), "shutdown must drain every in-flight fork");
        assertEquals(requests * FORKS_PER_REQUEST, forksRun.get(), "every fork ran exactly once");
        assertEquals(requests, recoveredInFork.get(), "the failing fork recovered inside itself, per request");
        assertEquals(0, forkFailures.get(), "a recovered fork failure must not reach the error handlers");
    }

    @Test
    void aForkThatAlwaysFailsNeverBreaksTheMainLine() throws Exception {
        var engine = new DefaultNioEngine();
        var reported = new AtomicInteger();
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        flow.onError(error -> reported.incrementAndGet());
        flow.fork("doomed", sub -> sub.handle("boom", value -> {
            throw new IllegalStateException("always");
        }));
        flow.handle("main", value -> value * 2);
        engine.seal();

        int requests = 500;
        List<CompletableFuture<Integer>> results = new ArrayList<>();
        for (int i = 0; i < requests; i++) {
            results.add(flow.just(i).executeAsync().orTimeout(10, TimeUnit.SECONDS));
        }
        for (int i = 0; i < requests; i++) {
            assertEquals(i * 2, results.get(i).join(), "the fork's failure never reaches the caller");
        }

        assertEquals(0, engine.shutdown(Duration.ofSeconds(20)));
        assertTrue(reported.get() == requests,
                "each unrecovered fork failure reports once to onError, got " + reported.get());
    }

    private static void awaitQuietly(CountDownLatch latch) {
        try {
            if (!latch.await(30, TimeUnit.SECONDS)) {
                throw new IllegalStateException("start latch never released");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }
}
