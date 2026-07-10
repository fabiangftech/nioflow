package dev.nioflow.unit;

import dev.nioflow.application.facade.DefaultNioFlow;
import dev.nioflow.unit.utils.RecordingMetrics;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CompletionException;

import static org.junit.jupiter.api.Assertions.*;

class DefaultNioFlowMetricsTest {

    @Test
    void lifecycleCountersTrackEveryOutcome() {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            RecordingMetrics metrics = new RecordingMetrics();
            defaultNioFlow.metrics(metrics)
                    .filter(x -> x != 2)
                    .submit(x -> {
                        if (x == 3) {
                            throw new IllegalStateException("boom");
                        }
                        return x * 10;
                    });

            defaultNioFlow.justAll(List.of(1, 2, 3));
            assertThrows(CompletionException.class, defaultNioFlow::join);

            assertEquals(3, metrics.injected.get());
            assertEquals(1, metrics.completed.get()); // value 1
            assertEquals(1, metrics.dropped.get());   // value 2, filtered
            assertEquals(1, metrics.failed.get());    // value 3
        }
    }

    @Test
    void recoveredValuesCountAsCompletedNotFailed() {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            RecordingMetrics metrics = new RecordingMetrics();
            defaultNioFlow.metrics(metrics)
                    .submit(x -> {
                        throw new IllegalStateException("boom");
                    })
                    .onErrorResume(error -> -1);

            defaultNioFlow.just(1);
            defaultNioFlow.join();

            assertEquals(1, metrics.completed.get());
            assertEquals(0, metrics.failed.get());
        }
    }

    @Test
    void perStageLatencyCarriesNameKindAndOutcome() {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            RecordingMetrics metrics = new RecordingMetrics();
            defaultNioFlow.metrics(metrics)
                    .handle("validate", x -> x + 1)
                    .submit("save", x -> x * 10)
                    .handle(x -> x); // unnamed

            defaultNioFlow.just(1);
            defaultNioFlow.join();

            assertEquals(3, metrics.stages.size());
            assertTrue(metrics.stages.contains("validate:handle:success:timed"));
            assertTrue(metrics.stages.contains("save:submit:success:timed"));
            assertTrue(metrics.stages.contains("unnamed:handle:success:timed"));
        }
    }

    @Test
    void failingStagesReportTheErrorOutcome() {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            RecordingMetrics metrics = new RecordingMetrics();
            defaultNioFlow.metrics(metrics)
                    .submit("flaky", x -> {
                        throw new IllegalStateException("boom");
                    });

            defaultNioFlow.just(1);
            assertThrows(CompletionException.class, defaultNioFlow::join);

            assertEquals(List.of("flaky:submit:error:timed"), metrics.stages);
        }
    }

    @Test
    void fanOutReportsTheNumberOfChildren() {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            RecordingMetrics metrics = new RecordingMetrics();
            defaultNioFlow.metrics(metrics)
                    .fanOut(x -> List.of(x, x + 1, x + 2));

            defaultNioFlow.just(1);
            defaultNioFlow.join();

            assertEquals(List.of(3), metrics.fanOuts);
            assertEquals(1, metrics.injected.get());
            assertEquals(3, metrics.completed.get());
        }
    }
}
