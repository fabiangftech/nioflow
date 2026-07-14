package dev.nioflow.application.facade;

import dev.nioflow.core.facade.NioFlow;
import dev.nioflow.core.facade.NioFlowMetrics;
import dev.nioflow.core.model.Retry;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The ninth link: an AsyncStage is a Stage that hands back a CompletionStage
 * instead of a value — so a worker INVOKES it and leaves, and nothing waits on a
 * thread. Everything here is a claim the RFC makes, pinned.
 */
class DefaultNioFlowAsyncStageTest extends EngineTestSupport {

    @Test
    void theCallsResultContinuesDownTheChain() {
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        flow.handleAsync("double", value -> CompletableFuture.completedFuture(value * 2))
                .handle("plus-one", value -> value + 1);
        engine.seal();

        assertEquals(21, flow.just(10).execute());
    }

    /** It is just a link: recover() catches it exactly like a stage failure. */
    @Test
    void aFailingCompletionStageIsCaughtByRecover() {
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        flow.handleAsync("remote", value -> CompletableFuture.failedFuture(new IllegalStateException("down")))
                .recover(error -> error instanceof IllegalStateException ? -1 : -2);
        engine.seal();

        assertEquals(-1, flow.just(1).execute());
    }

    /** A call that throws before it ever returns a stage is the same failure. */
    @Test
    void aSynchronousThrowFromTheCallIsAnOrdinaryStageFailure() {
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        flow.handleAsync("remote", value -> {
            throw new IllegalStateException("could not even build the request");
        }).recover(error -> -1);
        engine.seal();

        assertEquals(-1, flow.just(1).execute());
    }

    /**
     * A null CompletionStage is a bug in the call, not an empty result: it fails
     * the value loudly (and recoverably) instead of hanging the execution forever
     * waiting on a stage that does not exist.
     */
    @Test
    void aNullCompletionStageFailsTheValueInsteadOfHangingIt() {
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        flow.handleAsync("remote", value -> null)
                .recover(error -> error instanceof IllegalStateException ? -1 : -2);
        engine.seal();

        assertEquals(-1, flow.just(1).execute());
    }

    @Test
    void retryReInvokesTheCallAndTheLastFailureReachesRecover() {
        AtomicInteger attempts = new AtomicInteger();
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        flow.handleAsync("flaky", value -> attempts.incrementAndGet() < 3
                        ? CompletableFuture.failedFuture(new IllegalStateException("not yet"))
                        : CompletableFuture.completedFuture(value * 2),
                Retry.of(3, Duration.ofMillis(5)));
        engine.seal();

        assertEquals(20, flow.just(10).execute());
        assertEquals(3, attempts.get());
    }

    @Test
    void exhaustedRetriesFlowToRecover() {
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        flow.handleAsync("always-down",
                        value -> CompletableFuture.failedFuture(new IllegalStateException("down")),
                        Retry.of(2, Duration.ofMillis(5)))
                .recover(error -> -1);
        engine.seal();

        assertEquals(-1, flow.just(1).execute());
    }

    /**
     * The timeout does what a Stage's cannot: it CANCELS the call. A parked
     * worker can only be abandoned — a CompletionStage is a handle.
     */
    @Test
    void theTimeoutCancelsTheCallAndFailsTheValueWithATimeout() {
        CompletableFuture<Integer> remote = new CompletableFuture<>();  // never completes on its own
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        flow.handleAsync("hangs", value -> remote, Duration.ofMillis(50))
                .recover(error -> error instanceof TimeoutException ? -1 : -2);
        engine.seal();

        assertEquals(-1, flow.just(1).execute());
        assertTrue(remote.isCancelled(), "the timeout must cancel the call, not merely stop waiting for it");
    }

    /**
     * The whole RFC in one test. The engine gets ONE platform worker: an async
     * stage waiting on a call that never completes must leave that worker free
     * for the next execution. A stage that BLOCKED on the same call would hold
     * it forever — see the second half.
     */
    @Test
    void anAsyncStageWaitingForeverStillReleasesTheWorker() throws Exception {
        ExecutorService boss = Executors.newSingleThreadExecutor();
        ExecutorService oneWorker = Executors.newFixedThreadPool(1);
        DefaultNioEngine oneWorkerEngine = new DefaultNioEngine(boss, oneWorker);
        try {
            NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, oneWorkerEngine);
            flow.handleAsync("remote", value -> value == 1
                    ? new CompletableFuture<>()                          // hangs forever
                    : CompletableFuture.completedFuture(value * 10));
            oneWorkerEngine.seal();

            CompletableFuture<Integer> hung = flow.just(1).executeAsync();
            // The single worker invoked the call and went back to the pool, so
            // the next execution runs on it while the first is still in flight.
            assertEquals(20, flow.just(2).executeAsync().get(2, TimeUnit.SECONDS));
            assertTrue(!hung.isDone(), "the hung execution must still be waiting — on nothing but a future");
        } finally {
            oneWorkerEngine.shutdown(Duration.ofMillis(100));
        }
    }

    /** The other half: a blocking stage on one worker starves the next execution. */
    @Test
    void aBlockingStageOnTheSameWorkerStarvesTheNextExecution() {
        ExecutorService boss = Executors.newSingleThreadExecutor();
        ExecutorService oneWorker = Executors.newFixedThreadPool(1);
        DefaultNioEngine oneWorkerEngine = new DefaultNioEngine(boss, oneWorker);
        CountDownLatch released = new CountDownLatch(1);
        try {
            NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, oneWorkerEngine);
            flow.handle("remote", value -> {
                if (value == 1) {
                    await(released);                                     // parks the only worker
                }
                return value * 10;
            });
            oneWorkerEngine.seal();

            flow.just(1).executeAsync();
            CompletableFuture<Integer> behind = flow.just(2).executeAsync();

            assertThrows(TimeoutException.class, () -> behind.get(300, TimeUnit.MILLISECONDS),
                    "a parked worker is a worker nobody else can have — that is what handleAsync exists to avoid");
        } finally {
            released.countDown();
            oneWorkerEngine.shutdown(Duration.ofMillis(200));
        }
    }

    /**
     * An async stage is a DISPATCH BOUNDARY: it ends the fused run. Counted in
     * threads, not read off the plan — the workers are virtual and one per task,
     * so "same thread" IS "fused".
     */
    @Test
    void anAsyncStageBreaksTheFusedRun() {
        ConcurrentLinkedQueue<Long> fusedThreads = new ConcurrentLinkedQueue<>();
        NioFlow<Integer, Integer> fused = DefaultNioFlow.from(Integer.class, engine);
        fused.handle("a", value -> mark(fusedThreads, value))
                .handle("b", value -> mark(fusedThreads, value));
        engine.seal();
        fused.just(1).execute();

        List<Long> fusedRun = List.copyOf(fusedThreads);
        assertEquals(2, fusedRun.size());
        assertEquals(fusedRun.get(0), fusedRun.get(1),
                "two plain stages must fuse: one worker run, one thread");

        ConcurrentLinkedQueue<Long> brokenThreads = new ConcurrentLinkedQueue<>();
        DefaultNioEngine other = new DefaultNioEngine();
        try {
            NioFlow<Integer, Integer> broken = DefaultNioFlow.from(Integer.class, other);
            broken.handle("a", value -> mark(brokenThreads, value))
                    .handleAsync("remote",
                            value -> CompletableFuture.completedFuture(mark(brokenThreads, value)))
                    .handle("b", value -> mark(brokenThreads, value));
            other.seal();
            broken.just(1).execute();
        } finally {
            other.shutdown(Duration.ofMillis(100));
        }

        List<Long> brokenRun = List.copyOf(brokenThreads);
        assertEquals(3, brokenRun.size());
        assertNotEquals(brokenRun.get(0), brokenRun.get(1),
                "the async stage must dispatch on its own: it cannot fuse into the stage before it");
        assertNotEquals(brokenRun.get(1), brokenRun.get(2),
                "and the stage after it is a dispatch of its own too");
    }

    @Test
    void anAsyncStageRunsInsideALaneAndInsideAFork() throws Exception {
        CountDownLatch forked = new CountDownLatch(1);
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        flow.when(value -> value % 2 == 0)
                .then(lane -> lane.handleAsync("even", value -> CompletableFuture.completedFuture(value * 10)))
                .otherwise(lane -> lane.handleAsync("odd", value -> CompletableFuture.completedFuture(-value)))
                .fork("notify", sub -> sub
                        .handleAsync("ping", CompletableFuture::completedFuture)
                        .background(value -> forked.countDown()));
        engine.seal();

        assertEquals(20, flow.just(2).execute());
        assertEquals(-3, flow.just(3).execute());
        assertTrue(forked.await(2, TimeUnit.SECONDS), "the fork's async stage must run too");
    }

    /** It reports like a stage: its own name, its own latency, its own retries. */
    @Test
    void itReportsStageMetricsUnderItsOwnName() {
        ConcurrentLinkedQueue<String> completed = new ConcurrentLinkedQueue<>();
        ConcurrentLinkedQueue<String> retried = new ConcurrentLinkedQueue<>();
        engine.metrics(new NioFlowMetrics() {
            @Override
            public void stageCompleted(String stage, long nanos) {
                completed.add(stage);
            }

            @Override
            public void stageRetried(String stage) {
                retried.add(stage);
            }
        });
        AtomicInteger attempts = new AtomicInteger();
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        flow.handleAsync("remote", value -> attempts.incrementAndGet() < 2
                        ? CompletableFuture.failedFuture(new IllegalStateException("not yet"))
                        : CompletableFuture.completedFuture(value),
                Retry.of(2, Duration.ofMillis(5)));
        engine.seal();

        flow.just(1).execute();

        assertEquals(List.of("remote"), List.copyOf(completed));
        assertEquals(List.of("remote"), List.copyOf(retried));
    }

    // The id, not the name: a virtual thread's name is the empty string, and
    // "same thread" is exactly what fusion means here.
    private static Integer mark(ConcurrentLinkedQueue<Long> threads, Integer value) {
        threads.add(Thread.currentThread().threadId());
        return value;
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await(5, TimeUnit.SECONDS);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }

}
