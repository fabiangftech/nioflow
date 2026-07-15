package dev.nioflow.application.facade;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The MPSC event loop in isolation: it is the one piece of genuinely concurrent
 * code RFC 0009 adds, so it is de-risked here before the engine ever sees it.
 */
class BossLoopTest {

    private static final ThreadFactory FACTORY =
            Thread.ofPlatform().name("boss-loop-test-", 0).daemon(true).factory();

    private BossLoop loop() {
        return new BossLoop(FACTORY);
    }

    @Test
    void runsTasksInSubmissionOrderFromASingleProducer() throws InterruptedException {
        BossLoop loop = loop();
        int n = 10_000;
        List<Integer> seen = new ArrayList<>();
        CountDownLatch done = new CountDownLatch(n);
        for (int i = 0; i < n; i++) {
            int value = i;
            loop.execute(() -> {
                seen.add(value); // single consumer — no synchronization needed
                done.countDown();
            });
        }
        assertTrue(done.await(10, TimeUnit.SECONDS), "all tasks ran");
        for (int i = 0; i < n; i++) {
            assertEquals(i, seen.get(i), "FIFO order preserved");
        }
        loop.shutdown();
    }

    @Test
    void runsEveryTaskUnderManyConcurrentProducers() throws InterruptedException {
        BossLoop loop = loop();
        int producers = 8;
        int perProducer = 20_000;
        AtomicInteger total = new AtomicInteger();
        CountDownLatch ready = new CountDownLatch(producers);
        CountDownLatch go = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(producers * perProducer);
        List<Thread> threads = new ArrayList<>();
        for (int p = 0; p < producers; p++) {
            Thread t = new Thread(() -> {
                ready.countDown();
                await(go);
                for (int i = 0; i < perProducer; i++) {
                    loop.execute(() -> {
                        total.incrementAndGet();
                        done.countDown();
                    });
                }
            });
            t.start();
            threads.add(t);
        }
        await(ready);
        go.countDown();
        assertTrue(done.await(30, TimeUnit.SECONDS), "no task lost under contention");
        assertEquals(producers * perProducer, total.get());
        threads.forEach(BossLoopTest::join);
        loop.shutdown();
    }

    @Test
    void aTaskSubmittedWhileTheLoopIsParkedStillRuns() throws InterruptedException {
        BossLoop loop = loop();
        // Let it drain and park (spin budget is ~µs; a short settle is plenty).
        Thread.sleep(50);
        CountDownLatch ran = new CountDownLatch(1);
        loop.execute(ran::countDown);
        assertTrue(ran.await(5, TimeUnit.SECONDS), "the wake-up from PARKED fired");
        loop.shutdown();
    }

    @Test
    void executeAfterShutdownIsRejected() {
        BossLoop loop = loop();
        loop.shutdown();
        assertThrows(RejectedExecutionException.class, () -> loop.execute(() -> {}));
    }

    @Test
    void shutdownDrainsQueuedTasksThenTerminates() throws InterruptedException {
        BossLoop loop = loop();
        int n = 5_000;
        AtomicInteger ran = new AtomicInteger();
        for (int i = 0; i < n; i++) {
            loop.execute(ran::incrementAndGet);
        }
        loop.shutdown();
        assertTrue(loop.awaitTermination(10, TimeUnit.SECONDS), "the loop thread exited");
        assertTrue(loop.isTerminated());
        assertEquals(n, ran.get(), "graceful shutdown ran everything already queued");
    }

    @Test
    void reentrantSubmissionFromInsideATaskRuns() throws InterruptedException {
        BossLoop loop = loop();
        // A task that submits more tasks — the engine does this on every hop
        // (a continuation posts the next continuation to the same boss).
        ConcurrentLinkedQueue<Integer> order = new ConcurrentLinkedQueue<>();
        CountDownLatch done = new CountDownLatch(3);
        loop.execute(() -> {
            order.add(1);
            done.countDown();
            loop.execute(() -> {
                order.add(2);
                done.countDown();
                loop.execute(() -> {
                    order.add(3);
                    done.countDown();
                });
            });
        });
        assertTrue(done.await(5, TimeUnit.SECONDS));
        assertEquals(List.of(1, 2, 3), new ArrayList<>(order));
        loop.shutdown();
    }

    @Test
    void aThrowingTaskDoesNotKillTheLoop() throws InterruptedException {
        BossLoop loop = loop();
        loop.execute(() -> {
            throw new RuntimeException("boom");
        });
        CountDownLatch afterBoom = new CountDownLatch(1);
        loop.execute(afterBoom::countDown);
        assertTrue(afterBoom.await(5, TimeUnit.SECONDS), "the loop survived a throwing task");
        assertFalse(loop.isTerminated());
        loop.shutdown();
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    private static void join(Thread thread) {
        try {
            thread.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }
}
