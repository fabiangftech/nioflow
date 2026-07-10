package dev.nioflow.unit;

import dev.nioflow.application.facade.NioFlow;
import dev.nioflow.unit.utils.RecordingMetrics;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CompletionException;

import static org.junit.jupiter.api.Assertions.*;

class NioFlowMetricsTest {

    @Test
    void lifecycleCountersTrackEveryOutcome() {
        try (NioFlow<Integer> pipeline = new NioFlow<>()) {
            RecordingMetrics metrics = new RecordingMetrics();
            pipeline.metrics(metrics)
                    .filter(x -> x != 2)
                    .submit(x -> {
                        if (x == 3) {
                            throw new IllegalStateException("boom");
                        }
                        return x * 10;
                    });

            pipeline.justAll(List.of(1, 2, 3));
            assertThrows(CompletionException.class, pipeline::join);

            assertEquals(3, metrics.injected.get());
            assertEquals(1, metrics.completed.get()); // value 1
            assertEquals(1, metrics.dropped.get());   // value 2, filtered
            assertEquals(1, metrics.failed.get());    // value 3
        }
    }

    @Test
    void recoveredValuesCountAsCompletedNotFailed() {
        try (NioFlow<Integer> pipeline = new NioFlow<>()) {
            RecordingMetrics metrics = new RecordingMetrics();
            pipeline.metrics(metrics)
                    .submit(x -> {
                        throw new IllegalStateException("boom");
                    })
                    .onErrorResume(error -> -1);

            pipeline.just(1);
            pipeline.join();

            assertEquals(1, metrics.completed.get());
            assertEquals(0, metrics.failed.get());
        }
    }

    @Test
    void perStageLatencyCarriesNameKindAndOutcome() {
        try (NioFlow<Integer> pipeline = new NioFlow<>()) {
            RecordingMetrics metrics = new RecordingMetrics();
            pipeline.metrics(metrics)
                    .handle("validate", x -> x + 1)
                    .submit("save", x -> x * 10)
                    .handle(x -> x); // unnamed

            pipeline.just(1);
            pipeline.join();

            assertEquals(3, metrics.stages.size());
            assertTrue(metrics.stages.contains("validate:handle:success:timed"));
            assertTrue(metrics.stages.contains("save:submit:success:timed"));
            assertTrue(metrics.stages.contains("unnamed:handle:success:timed"));
        }
    }

    @Test
    void failingStagesReportTheErrorOutcome() {
        try (NioFlow<Integer> pipeline = new NioFlow<>()) {
            RecordingMetrics metrics = new RecordingMetrics();
            pipeline.metrics(metrics)
                    .submit("flaky", x -> {
                        throw new IllegalStateException("boom");
                    });

            pipeline.just(1);
            assertThrows(CompletionException.class, pipeline::join);

            assertEquals(List.of("flaky:submit:error:timed"), metrics.stages);
        }
    }

    @Test
    void fanOutReportsTheNumberOfChildren() {
        try (NioFlow<Integer> pipeline = new NioFlow<>()) {
            RecordingMetrics metrics = new RecordingMetrics();
            pipeline.metrics(metrics)
                    .fanOut(x -> List.of(x, x + 1, x + 2));

            pipeline.just(1);
            pipeline.join();

            assertEquals(List.of(3), metrics.fanOuts);
            assertEquals(1, metrics.injected.get());
            assertEquals(3, metrics.completed.get());
        }
    }
}
