package dev.nioflow.stress;

import dev.nioflow.application.facade.DefaultNioEngine;
import dev.nioflow.application.facade.DefaultNioFlow;
import dev.nioflow.core.facade.Cancellable;
import dev.nioflow.core.facade.NioFlow;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * The acceptance test for RFC 0013's worker-side async driver: a fused run of
 * async stages, cancelled at a uniformly random point, over and over and under
 * concurrency. What must hold no matter the interleaving: every execution's
 * future SETTLES — a cancelled one with a CancellationException, an untouched
 * one with the right full-run value — and never hangs. The futures carry an
 * orTimeout, so a driver bug that strands an execution fails the test instead
 * of blocking it forever.
 *
 * <p>(That no stage runs after the cut is pinned deterministically in core's
 * DefaultNioFlowAsyncStageFusionTest; this test hunts hangs and races under
 * load.) Stages resolve after a tiny delay so the run is genuinely in flight
 * when the cut lands, exercising the pending-callback path, not just the inline
 * fast-path.
 */
class AsyncRunCancellationStressTest {

    private static final int PRODUCERS = 8;
    private static final int PER_PRODUCER = 400;
    private static final int STAGES = 6;

    @Test
    void aFusedAsyncRunCancelledAtRandomPointsNeverHangsAndNeverRunsAStageOutOfOrder() throws Exception {
        var engine = new DefaultNioEngine();
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        // Each stage adds its index+1; a full run of 6 leaves 0 + (1+2+..+6) = 21.
        NioFlow<Integer, Integer> building = flow;
        for (int i = 0; i < STAGES; i++) {
            int step = i + 1;
            building = building.handleAsync("s" + i, value -> resolveSoon(value + step));
        }
        engine.seal();

        var errors = new ConcurrentLinkedQueue<String>();
        var cancelled = new AtomicInteger();
        var completed = new AtomicInteger();
        var start = new CountDownLatch(1);
        List<Thread> threads = new ArrayList<>();

        for (int p = 0; p < PRODUCERS; p++) {
            long seed = 0x9E3779B97F4A7C15L * (p + 1);
            Thread thread = new Thread(() -> {
                await(start);
                long rng = seed;
                for (int n = 0; n < PER_PRODUCER; n++) {
                    rng = rng * 6364136223846793005L + 1442695040888963407L; // LCG
                    boolean doCancel = (rng >>> 33 & 1) == 0;
                    Cancellable<Integer> handle = flow.just(0).executeCancellable();
                    // Random moment to cut: from immediately to a few hundred µs in.
                    if (doCancel) {
                        parkNanos((rng >>> 20) % 300_000L);
                        handle.cancel();
                    }
                    try {
                        Integer result = handle.future().orTimeout(5, TimeUnit.SECONDS).join();
                        // Not cancelled in time: it ran to completion with the full sum.
                        if (result != 21) {
                            errors.add("unexpected result " + result);
                        }
                        completed.incrementAndGet();
                    } catch (CompletionException e) {
                        if (e.getCause() instanceof CancellationException) {
                            cancelled.incrementAndGet();
                        } else {
                            errors.add("unexpected failure " + e.getCause());
                        }
                    } catch (CancellationException e) {
                        cancelled.incrementAndGet();
                    }
                }
            });
            threads.add(thread);
            thread.start();
        }

        start.countDown();
        for (Thread thread : threads) {
            thread.join(Duration.ofSeconds(60).toMillis());
            if (thread.isAlive()) {
                fail("a producer hung — an execution future never settled");
            }
        }

        assertTrue(errors.isEmpty(), "invariants violated: " + errors);
        // Every execution settled one way or the other; nothing was lost.
        assertEquals(PRODUCERS * PER_PRODUCER, cancelled.get() + completed.get());
        assertTrue(cancelled.get() > 0, "no execution was ever cancelled — the test exercised nothing");
        engine.shutdown(Duration.ofSeconds(2));
    }

    private static CompletionStage<Integer> resolveSoon(int value) {
        // Completes shortly, off the caller, so the run is genuinely in flight.
        return CompletableFuture.supplyAsync(() -> value,
                CompletableFuture.delayedExecutor(150, TimeUnit.MICROSECONDS));
    }

    private static void parkNanos(long nanos) {
        java.util.concurrent.locks.LockSupport.parkNanos(nanos);
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }
}
