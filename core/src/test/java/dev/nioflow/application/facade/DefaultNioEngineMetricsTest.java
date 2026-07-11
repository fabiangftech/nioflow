package dev.nioflow.application.facade;

import dev.nioflow.core.facade.NioFlow;
import dev.nioflow.core.facade.NioFlowMetrics;
import dev.nioflow.core.model.OverflowPolicy;
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
        final List<Integer> queueDepths = new CopyOnWriteArrayList<>();

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
        public void valueDropped() {
            dropped.incrementAndGet();
        }

        @Override
        public void queueDepth(int pending) {
            queueDepths.add(pending);
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
}
