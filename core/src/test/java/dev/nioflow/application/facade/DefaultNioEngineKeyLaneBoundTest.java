package dev.nioflow.application.facade;

import dev.nioflow.core.facade.NioFlowMetrics;
import dev.nioflow.core.model.OverflowPolicy;
import dev.nioflow.core.model.Stage;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * RFC 0039 — the per-key FIFO lane's backlog is observable (a metric) and,
 * optionally, bounded. A hot key whose head stalls otherwise grows its backlog
 * without limit; the bound is per-KEY backpressure, checked at admission because
 * enrollment runs on the boss, which must never park.
 */
class DefaultNioEngineKeyLaneBoundTest extends EngineTestSupport {

    private CountDownLatch release;
    private CountDownLatch headEntered;

    // A head stage that parks on a worker until release, so a backlog builds
    // behind the running head for the same key.
    private Stage gatedHead() {
        release = new CountDownLatch(1);
        headEntered = new CountDownLatch(1);
        return new Stage("gated", value -> {
            headEntered.countDown();
            awaitQuietly(release);
            return value;
        }, false, null, null, List.of());
    }

    private CompletableFuture<Object> callKeyed(Object input, Object key) {
        return engine.call(input, null, engine.chain(), key);
    }

    // Wait until `count` executions have enrolled behind the key's head, so the
    // backlog is deterministic before the next admission is tested.
    private void awaitBacklog(Object key, int count) {
        long deadline = System.nanoTime() + Duration.ofSeconds(2).toNanos();
        while (engine.keyLaneDepth(key) < count) {
            if (System.nanoTime() > deadline) {
                throw new IllegalStateException("backlog never reached " + count
                        + " (was " + engine.keyLaneDepth(key) + ")");
            }
            Thread.onSpinWait();
        }
    }

    // ── the metric ───────────────────────────────────────────────────────────

    @Test
    void keyLaneDepthAndActiveLanesAreReported() throws Exception {
        AtomicInteger maxDepth = new AtomicInteger();
        AtomicInteger activeSeen = new AtomicInteger();
        engine.metrics(new NioFlowMetrics() {
            @Override
            public void keyLaneDepth(int depth) {
                maxDepth.accumulateAndGet(depth, Math::max);
            }

            @Override
            public void keyLanesActive(int active) {
                activeSeen.accumulateAndGet(active, Math::max);
            }
        });
        engine.append(gatedHead());

        CompletableFuture<Object> head = callKeyed("a", "k");
        assertTrue(headEntered.await(2, TimeUnit.SECONDS));
        CompletableFuture<Object> q1 = callKeyed("b", "k");
        CompletableFuture<Object> q2 = callKeyed("c", "k");

        // Wait on the METRIC, not the queue size: the gauge is emitted right after
        // the enqueue, on the boss, so polling depth could see the size before the
        // meter fired.
        awaitValue(maxDepth, 2, "the deepest backlog was not reported");
        assertEquals(1, activeSeen.get(), "the active-lane count was not reported");

        release.countDown();
        CompletableFuture.allOf(head, q1, q2).get(2, TimeUnit.SECONDS);
        engine.shutdown(Duration.ofMillis(200));
    }

    // ── FAIL: the excess keyed call is rejected ───────────────────────────────

    @Test
    void failPolicyRejectsBeyondTheKeyLaneCapacity() throws Exception {
        engine.keyLaneCapacity(2, OverflowPolicy.FAIL);
        engine.append(gatedHead());

        CompletableFuture<Object> head = callKeyed("a", "k");
        assertTrue(headEntered.await(2, TimeUnit.SECONDS));
        CompletableFuture<Object> q1 = callKeyed("b", "k");
        CompletableFuture<Object> q2 = callKeyed("c", "k");
        awaitBacklog("k", 2);

        // Backlog is at capacity (2 behind the head): the next same-key call fails.
        assertThrows(RejectedExecutionException.class, () -> callKeyed("d", "k"));
        // A DIFFERENT key is unaffected — the bound is per key.
        Stage passthrough = new Stage("free", value -> value, false, null, null, List.of());
        assertEquals("z", engine.call("z", null, List.of(passthrough), "other").get(2, TimeUnit.SECONDS));

        release.countDown();
        CompletableFuture.allOf(head, q1, q2).get(2, TimeUnit.SECONDS);
        engine.shutdown(Duration.ofMillis(200));
    }

    // ── DROP: the excess call's future fails and is reported ───────────────────

    @Test
    void dropPolicyFailsTheFutureAndReportsBeyondTheKeyLaneCapacity() throws Exception {
        engine.keyLaneCapacity(2, OverflowPolicy.DROP);
        AtomicReference<Throwable> dropped = new AtomicReference<>();
        engine.addErrorHandler(dropped::set);
        engine.append(gatedHead());

        CompletableFuture<Object> head = callKeyed("a", "k");
        assertTrue(headEntered.await(2, TimeUnit.SECONDS));
        CompletableFuture<Object> q1 = callKeyed("b", "k");
        CompletableFuture<Object> q2 = callKeyed("c", "k");
        awaitBacklog("k", 2);

        CompletableFuture<Object> over = callKeyed("d", "k");
        CompletionException ex = assertThrows(CompletionException.class, over::join);
        assertInstanceOf(RejectedExecutionException.class, ex.getCause());
        assertInstanceOf(RejectedExecutionException.class, dropped.get());

        release.countDown();
        CompletableFuture.allOf(head, q1, q2).get(2, TimeUnit.SECONDS);
        engine.shutdown(Duration.ofMillis(200));
    }

    // ── BLOCK: the producer parks until the hot key drains ─────────────────────

    @Test
    void blockPolicyParksTheProducerUntilTheKeyDrains() throws Exception {
        engine.keyLaneCapacity(2, OverflowPolicy.BLOCK);
        engine.append(gatedHead());

        CompletableFuture<Object> head = callKeyed("a", "k");
        assertTrue(headEntered.await(2, TimeUnit.SECONDS));
        CompletableFuture<Object> q1 = callKeyed("b", "k");
        CompletableFuture<Object> q2 = callKeyed("c", "k");
        awaitBacklog("k", 2);

        // The 3rd backlog call must PARK the producer (capacity 2 is full).
        AtomicBoolean submitted = new AtomicBoolean();
        AtomicReference<CompletableFuture<Object>> overRef = new AtomicReference<>();
        Thread producer = Thread.ofVirtual().start(() -> {
            overRef.set(callKeyed("d", "k"));
            submitted.set(true);
        });

        Thread.sleep(200);
        assertFalse(submitted.get(), "producer should be parked while the key lane is full");

        // Draining the head frees a slot: the head completes, the backlog shrinks,
        // and the parked producer is signalled and proceeds.
        release.countDown();
        producer.join(Duration.ofSeconds(2));
        assertTrue(submitted.get(), "producer never resumed after the key lane drained");

        overRef.get().get(2, TimeUnit.SECONDS);
        CompletableFuture.allOf(head, q1, q2).get(2, TimeUnit.SECONDS);
        engine.shutdown(Duration.ofMillis(200));
    }

    // ── default: unbounded, no behavior change ─────────────────────────────────

    @Test
    void unboundedByDefaultNeverRejectsAKeyedCall() throws Exception {
        engine.append(new Stage("free", value -> value, false, null, null, List.of()));

        for (int i = 0; i < 50; i++) {
            assertEquals(i, callKeyed(i, "k").get(2, TimeUnit.SECONDS));
        }
        engine.shutdown(Duration.ofMillis(200));
    }

    private static void awaitValue(AtomicInteger actual, int expected, String message) {
        long deadline = System.nanoTime() + Duration.ofSeconds(2).toNanos();
        while (actual.get() < expected) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError(message + " (was " + actual.get() + ")");
            }
            Thread.onSpinWait();
        }
        assertEquals(expected, actual.get(), message);
    }

    private static void awaitQuietly(CountDownLatch latch) {
        try {
            latch.await(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
