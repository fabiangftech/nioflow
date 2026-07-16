package dev.nioflow.application.facade;

import dev.nioflow.core.facade.Cancellable;
import dev.nioflow.core.facade.NioFlow;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * RFC 0026 — releasing a key lane must not recurse on a worker stack nor read a
 * torn deque when it runs off the boss during a dedicated-engine shutdown.
 *
 * <p>The dangerous shape: a keyed head parked on an async call while a deep
 * backlog waits behind it, the engine shut down (boss gone), and the head
 * cancelled — so its terminal, and the release that cascades through the whole
 * backlog, run off the boss on the caller's thread. A recursive release would
 * overflow that stack; the fix drains the backlog in a loop. The queue is a
 * {@link java.util.concurrent.ConcurrentLinkedQueue} so the off-boss poll is
 * safe.
 */
class DefaultNioEngineKeyedShutdownTest {

    @Test
    void aDeepKeyedBacklogDrainedOffBossAtShutdownDoesNotOverflowTheStack() {
        DefaultNioEngine engine = DefaultNioEngine.dedicated(1);
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        // The head parks here forever (a future that never completes), holding
        // the lane so everything else queues behind it.
        CompletableFuture<Integer> hold = new CompletableFuture<>();
        flow.handleAsync("hold", value -> hold);

        Object key = "orders-42";
        int backlog = 20_000;

        Cancellable<Integer> head = flow.just(0).key(key).executeCancellable();
        List<CompletableFuture<Integer>> queued = new ArrayList<>();
        for (int i = 1; i <= backlog; i++) {
            queued.add(flow.just(i).key(key).executeAsync());
        }
        // Deterministic: wait until the entire backlog is enrolled behind the head.
        awaitBacklog(engine, key, backlog);

        // Boss gone, then cancel the head OFF the boss: the release cascades
        // through 20k queued executions. A recursive release StackOverflows here.
        engine.shutdown(Duration.ZERO);
        head.cancel();

        assertTrue(head.future().isDone(), "the cancelled head never settled");
        for (CompletableFuture<Integer> future : queued) {
            assertTrue(settled(future), "a queued execution was left hanging by the off-boss drain");
        }
        assertEquals(0, engine.keyLaneDepth(key), "the backlog was not fully drained");
        assertEquals(0, engine.activeKeyLanes(), "the key lane leaked");
        assertTrue(engine.inFlightCount() >= 0, "the drain counter went negative");
    }

    @Test
    void aKeyedHeadCancelledOffBossAtShutdownReleasesItsSuccessor() {
        // RFC 0040: laneHeld is written on the boss (the head took its lane) but
        // read on the OFF-boss cancel terminal — complete(CANCELLED) runs on the
        // caller's thread once the boss is gone at shutdown. A stale `false` read
        // there would skip releaseKey() and strand the successor; the volatile
        // makes the read see the boss's write.
        DefaultNioEngine engine = DefaultNioEngine.dedicated(1);
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        CompletableFuture<Integer> hold = new CompletableFuture<>();
        flow.handleAsync("hold", value -> hold);

        Object key = "acct-7";
        Cancellable<Integer> head = flow.just(0).key(key).executeCancellable();
        CompletableFuture<Integer> successor = flow.just(1).key(key).executeAsync();
        awaitBacklog(engine, key, 1);   // the successor is queued behind the head

        engine.shutdown(Duration.ZERO); // boss gone
        head.cancel();                  // its terminal runs OFF the boss

        assertTrue(settled(successor), "the successor was stranded: releaseKey was skipped off-boss");
        assertEquals(0, engine.keyLaneDepth(key), "the lane did not drain");
        assertEquals(0, engine.activeKeyLanes(), "the key lane leaked");
    }

    @Test
    void thePostedHandoffKeepsPerKeyFifoOrderingWhenTheHeadWasParked() {
        DefaultNioEngine engine = new DefaultNioEngine();
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        List<Integer> processed = new CopyOnWriteArrayList<>();
        flow.handle("record", value -> {
            processed.add(value);
            return value;
        });

        Object key = "k";
        List<CompletableFuture<Integer>> futures = new ArrayList<>();
        for (int i = 0; i < 200; i++) {
            futures.add(flow.just(i).key(key).executeAsync());
        }
        futures.forEach(future -> future.orTimeout(5, TimeUnit.SECONDS).join());

        // Same key, one at a time, in submission order — unchanged by posting the
        // handoff to the boss instead of advancing the successor inline.
        List<Integer> expected = new ArrayList<>();
        for (int i = 0; i < 200; i++) {
            expected.add(i);
        }
        assertEquals(expected, processed);
        assertEquals(0, engine.activeKeyLanes());
        engine.shutdown(Duration.ofMillis(200));
    }

    private static void awaitBacklog(DefaultNioEngine engine, Object key, int expected) {
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (engine.keyLaneDepth(key) < expected) {
            if (System.nanoTime() > deadline) {
                throw new IllegalStateException("backlog never reached " + expected
                        + " (was " + engine.keyLaneDepth(key) + ")");
            }
            Thread.onSpinWait();
        }
    }

    private static boolean settled(CompletableFuture<?> future) {
        try {
            future.get(2, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception ignored) {
            // Rejected/failed at shutdown settles exceptionally — still done.
        }
        return future.isDone();
    }
}
