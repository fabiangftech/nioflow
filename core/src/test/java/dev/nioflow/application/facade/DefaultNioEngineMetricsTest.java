package dev.nioflow.application.facade;

import dev.nioflow.core.facade.NioFlow;
import dev.nioflow.core.facade.NioFlowMetrics;
import dev.nioflow.core.model.OverflowPolicy;
import dev.nioflow.core.model.Retry;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DefaultNioEngineMetricsTest {

    /** In-memory recorder: collects every SPI callback for assertions. */
    private static final class RecordingMetrics implements NioFlowMetrics {

        final AtomicInteger completed = new AtomicInteger();
        final AtomicInteger failed = new AtomicInteger();
        final AtomicInteger filtered = new AtomicInteger();
        final AtomicInteger dropped = new AtomicInteger();
        final AtomicLong lastExecutionNanos = new AtomicLong();
        final List<String> stages = new CopyOnWriteArrayList<>();
        final List<String> recoveries = new CopyOnWriteArrayList<>();
        final List<String> retries = new CopyOnWriteArrayList<>();
        final List<Integer> queueDepths = new CopyOnWriteArrayList<>();
        final List<String> forksStarted = new CopyOnWriteArrayList<>();
        final List<String> forksCompleted = new CopyOnWriteArrayList<>();
        final List<String> forksFailed = new CopyOnWriteArrayList<>();
        final List<Integer> forkGauge = new CopyOnWriteArrayList<>();

        @Override
        public void executionCompleted(long nanos) {
            completed.incrementAndGet();
            lastExecutionNanos.set(nanos);
        }

        @Override
        public void executionFailed(Throwable error, long nanos) {
            failed.incrementAndGet();
        }

        @Override
        public void executionFiltered(long nanos) {
            filtered.incrementAndGet();
        }

        @Override
        public void stageCompleted(String stage, long nanos) {
            stages.add(stage);
        }

        @Override
        public void recoveryApplied(String recovery) {
            recoveries.add(recovery);
        }

        @Override
        public void stageRetried(String stage) {
            retries.add(stage);
        }

        @Override
        public void valueDropped() {
            dropped.incrementAndGet();
        }

        @Override
        public void queueDepth(int pending) {
            queueDepths.add(pending);
        }

        @Override
        public void forkStarted(String fork) {
            forksStarted.add(fork);
        }

        @Override
        public void forkCompleted(String fork, long nanos) {
            forksCompleted.add(fork);
        }

        @Override
        public void forkFailed(String fork, Throwable error, long nanos) {
            forksFailed.add(fork);
        }

        @Override
        public void forksInFlight(int running) {
            forkGauge.add(running);
        }
    }

    private static NioFlow<Integer, Integer> flowOver(DefaultNioEngine engine) {
        return DefaultNioFlow.from(Integer.class, engine);
    }

    @Test
    void recordsExecutionAndPerStageTimings() {
        var engine = new DefaultNioEngine();
        var recorder = new RecordingMetrics();
        engine.metrics(recorder);
        NioFlow<Integer, Integer> flow = flowOver(engine);
        flow.handle("plus", value -> value + 1)
                .handle("double", value -> value * 2);

        assertEquals(12, flow.just(5).execute());

        assertEquals(1, recorder.completed.get());
        assertTrue(recorder.lastExecutionNanos.get() > 0);
        assertEquals(List.of("plus", "double"), recorder.stages); // timed inside the fused run
        engine.shutdown(Duration.ofMillis(100));
    }

    @Test
    void classifiesFailedAndFilteredExecutions() {
        var engine = new DefaultNioEngine();
        var recorder = new RecordingMetrics();
        engine.metrics(recorder);
        NioFlow<Integer, Integer> flow = flowOver(engine);
        flow.filter(value -> value > 0)
                .handle("boom-on-two", value -> {
                    if (value == 2) {
                        throw new IllegalStateException("boom");
                    }
                    return value;
                });

        assertEquals(1, flow.just(1).execute());
        assertThrows(CompletionException.class, () -> flow.just(2).execute());
        flow.just(-1).execute(); // cut by the filter

        assertEquals(1, recorder.completed.get());
        assertEquals(1, recorder.failed.get());
        assertEquals(1, recorder.filtered.get());
        engine.shutdown(Duration.ofMillis(100));
    }

    @Test
    void recordsAppliedRecoveries() {
        var engine = new DefaultNioEngine();
        var recorder = new RecordingMetrics();
        engine.metrics(recorder);
        NioFlow<Integer, Integer> flow = flowOver(engine);
        flow.handle("boom", value -> {
                    throw new IllegalStateException("boom");
                })
                .recover("fallback", error -> -1);

        assertEquals(-1, flow.just(1).execute());

        assertEquals(List.of("fallback"), recorder.recoveries);
        assertEquals(1, recorder.completed.get()); // recovered executions complete normally
        engine.shutdown(Duration.ofMillis(100));
    }

    @Test
    void recordsDropsAndQueueDepth() {
        var engine = new DefaultNioEngine(1, OverflowPolicy.DROP);
        var recorder = new RecordingMetrics();
        engine.metrics(recorder);
        flowOver(engine).handle("identity", value -> value);

        engine.inject(1);
        engine.inject(2); // capacity 1: dropped

        assertEquals(1, recorder.dropped.get());
        assertEquals(1, engine.await());
        assertEquals(List.of(1, 0), recorder.queueDepths); // pushed on inject and on await
        engine.shutdown(Duration.ofMillis(100));
    }

    /**
     * The timeout+retry path is a different dispatch (per-attempt budget on the
     * TimerWheel) than the inline retry loop: it must report the same metrics.
     */
    @Test
    void retriesAndRecoveriesAreReportedOnTheTimeoutPathToo() {
        DefaultNioEngine engine = new DefaultNioEngine();
        RecordingMetrics metrics = new RecordingMetrics();
        engine.metrics(metrics);
        AtomicInteger attempts = new AtomicInteger();

        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        flow.handle("flaky", value -> {
                    if (attempts.incrementAndGet() < 3) {
                        throw new IllegalStateException("not yet");
                    }
                    return value * 2;
                }, Duration.ofSeconds(2), Retry.of(3, Duration.ofMillis(1)))
                .handle("timed", value -> value, Duration.ofSeconds(2))   // keeps the recovery unfused
                .recover("net", error -> -1);

        assertEquals(10, flow.just(5).execute());
        assertEquals(3, attempts.get());
        assertEquals(2, metrics.retries.size());              // two failures, two retries
        assertEquals(List.of("flaky", "flaky"), metrics.retries);

        // Now exhaust the attempts: the failure reaches the dispatched recovery.
        attempts.set(-10);
        assertEquals(-1, flow.just(5).execute());
        assertEquals(List.of("net"), metrics.recoveries);

        engine.shutdown(Duration.ofMillis(200));
    }

    /**
     * The metrics SPI is installed through a thread-safe reference and read
     * once per execution: a recorder installed from another thread is picked up
     * by the executions that start afterwards, and it is never half-visible.
     */
    @Test
    void aRecorderInstalledFromAnotherThreadIsSeenByLaterExecutions() throws Exception {
        DefaultNioEngine engine = new DefaultNioEngine();
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        flow.handle("double", value -> value * 2);

        assertEquals(2, flow.just(1).execute());   // no metrics installed yet: zero instrumentation

        RecordingMetrics metrics = new RecordingMetrics();
        Thread installer = new Thread(() -> engine.metrics(metrics));
        installer.start();
        installer.join();

        assertEquals(4, flow.just(2).execute());

        assertEquals(1, metrics.completed.get());              // only the execution after the install
        assertEquals(List.of("double"), metrics.stages);
        assertTrue(metrics.lastExecutionNanos.get() > 0);

        engine.shutdown(Duration.ofMillis(200));
    }

    /** Installing null puts the engine back to zero instrumentation. */
    @Test
    void installingNullMetricsDisablesInstrumentation() {
        DefaultNioEngine engine = new DefaultNioEngine();
        RecordingMetrics metrics = new RecordingMetrics();
        engine.metrics(metrics);

        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        flow.handle("double", value -> value * 2);
        assertEquals(2, flow.just(1).execute());
        assertEquals(1, metrics.completed.get());

        engine.metrics(null);
        assertEquals(4, flow.just(2).execute());

        assertEquals(1, metrics.completed.get());   // the second execution reported nothing
        engine.shutdown(Duration.ofMillis(200));
    }

    /**
     * A fork reports apart from the request that spawned it: its latency is NOT
     * the request's (the response never waited for it), so it must never land in
     * the execution histogram. Its stages, on the other hand, are ordinary stages
     * and report as such — which is a main reason to prefer fork over a
     * hand-rolled background.
     */
    @Test
    void aForkReportsThroughTheForkHooksAndNotAsAnExecution() {
        var metrics = new RecordingMetrics();
        var engine = new DefaultNioEngine();
        engine.metrics(metrics);
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        flow.fork("audit", sub -> sub.handle("persist", value -> value * 2));
        flow.handle("main", value -> value + 1);

        assertEquals(2, flow.just(1).execute());
        assertEquals(0, engine.shutdown(Duration.ofSeconds(2)), "the fork must drain");

        // ONE execution (the request), ONE fork — not two executions.
        assertEquals(1, metrics.completed.get());
        assertEquals(List.of("audit"), metrics.forksStarted);
        assertEquals(List.of("audit"), metrics.forksCompleted);
        assertTrue(metrics.forksFailed.isEmpty());
        // The fork's stage is a stage like any other.
        assertTrue(metrics.stages.contains("persist"), () -> "stages: " + metrics.stages);
        assertTrue(metrics.stages.contains("main"), () -> "stages: " + metrics.stages);
        // The gauge went up and came back down.
        assertEquals(1, metrics.forkGauge.get(0));
        assertEquals(0, metrics.forkGauge.get(metrics.forkGauge.size() - 1));
    }

    @Test
    void anUnrecoveredForkFailureReportsForkFailedAndNotExecutionFailed() {
        var metrics = new RecordingMetrics();
        var engine = new DefaultNioEngine();
        engine.metrics(metrics);
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        flow.fork("doomed", sub -> sub.handle("boom", value -> {
            throw new IllegalStateException("fork blew up");
        }));
        flow.handle("main", value -> value + 1);

        assertEquals(2, flow.just(1).execute());   // the caller never sees it
        assertEquals(0, engine.shutdown(Duration.ofSeconds(2)));

        assertEquals(List.of("doomed"), metrics.forksFailed);
        assertTrue(metrics.forksCompleted.isEmpty());
        assertEquals(1, metrics.completed.get());  // the REQUEST completed...
        assertEquals(0, metrics.failed.get());     // ...and nothing failed at execution level
    }
}
