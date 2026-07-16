package dev.nioflow.application.facade;

import dev.nioflow.core.facade.Cancellable;
import dev.nioflow.core.facade.NioFlow;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * RFC 0024 — the exactly-once terminal is an atomic CAS, so a double terminal at
 * shutdown cannot decrement the drain counter twice.
 *
 * <p>The dangerous window is a {@code dedicated} engine being shut down while a
 * worker's {@code resumeOnBoss} is rejected (a {@code fail} off the boss) AND an
 * outside {@code cancel()} finds the boss gone (a {@code complete} off the boss).
 * A non-atomic check-then-set lets both win and run bookkeeping — and its
 * {@code activeExecutions.decrement()} — twice, so a graceful drain can report
 * clean while work still runs. The CAS elects one winner.
 *
 * <p>The cross-thread race cannot be forced deterministically through the public
 * API, so the second test is a bug-hunter (many iterations, three threads driven
 * to overlap by a barrier) whose oracle is precise: the drain counter must never
 * go NEGATIVE, which is exactly what a double decrement produces. Every joined
 * future carries a timeout so a hang is a visible failure, not an infinite wait.
 */
class DefaultNioEngineShutdownRaceTest {

    @Test
    void aCleanDedicatedShutdownDrainsToExactlyZero() {
        DefaultNioEngine engine = DefaultNioEngine.dedicated(2);
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        flow.handle("work", value -> value + 1);

        List<CompletableFuture<Integer>> futures = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            futures.add(flow.just(i).executeAsync());
        }
        futures.forEach(future -> future.orTimeout(2, TimeUnit.SECONDS).join());

        assertEquals(0, engine.inFlightCount(), "a clean drain must reach exactly zero, never below");
        assertEquals(0, engine.shutdown(Duration.ofSeconds(1)), "shutdown must report a clean drain");
    }

    @Test
    void racingShutdownCancelAndAsyncCompletionNeverDoubleCountsTheDrain() throws InterruptedException {
        for (int iteration = 0; iteration < 120; iteration++) {
            DefaultNioEngine engine = DefaultNioEngine.dedicated(1);
            NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);

            // Each execution parks on a per-execution async call, then has a
            // following stage — so COMPLETING the call drives advance → dispatch →
            // a worker → resumeOnBoss, which is one of the two off-boss terminals.
            Queue<CompletableFuture<Integer>> pending = new ConcurrentLinkedQueue<>();
            int count = 4;
            CountDownLatch inFlight = new CountDownLatch(count);
            flow.handleAsync("remote", value -> {
                CompletableFuture<Integer> call = new CompletableFuture<>();
                pending.add(call);
                inFlight.countDown();
                return call;
            }).handle("after", value -> value + 1);

            List<Cancellable<Integer>> handles = new ArrayList<>();
            for (int i = 0; i < count; i++) {
                handles.add(flow.just(i).executeCancellable());
            }
            assertTrue(inFlight.await(2, TimeUnit.SECONDS), "executions never reached the async stage");

            // Three terminals driven to overlap: shutdown (kills the boss),
            // cancel (the other off-boss complete), async completion (the off-boss
            // fail via a rejected resumeOnBoss).
            CyclicBarrier start = new CyclicBarrier(3);
            Thread shutdown = new Thread(() -> {
                awaitBarrier(start);
                engine.shutdown(Duration.ofMillis(100));
            });
            Thread cancel = new Thread(() -> {
                awaitBarrier(start);
                handles.forEach(Cancellable::cancel);
            });
            Thread complete = new Thread(() -> {
                awaitBarrier(start);
                pending.forEach(call -> call.complete(99));
            });
            shutdown.start();
            cancel.start();
            complete.start();
            shutdown.join();
            cancel.join();
            complete.join();

            for (Cancellable<Integer> handle : handles) {
                assertTrue(settled(handle.future()), "a future was left hanging by the shutdown/cancel race");
            }
            long drain = engine.inFlightCount();
            assertTrue(drain >= 0,
                    "drain went negative — a terminal was counted twice at iteration " + iteration + ": " + drain);
            assertEquals(0, engine.activeKeyLanes(), "a key lane leaked");
        }
    }

    private static boolean settled(CompletableFuture<?> future) {
        try {
            future.get(2, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception ignored) {
            // A cancelled or rejected execution settles exceptionally — still done.
        }
        return future.isDone();
    }

    private static void awaitBarrier(CyclicBarrier barrier) {
        try {
            barrier.await(2, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (BrokenBarrierException | TimeoutException e) {
            throw new IllegalStateException("barrier never tripped", e);
        }
    }
}
