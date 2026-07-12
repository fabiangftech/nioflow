package dev.nioflow.core.facade;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

/**
 * The SPI is all-defaults on purpose: an adapter implements the callbacks it
 * cares about, and the engine can push everything else into a no-op instead of
 * null-checking each callback.
 */
class NioFlowMetricsTest {

    @Test
    void everyCallbackDefaultsToANoOp() {
        NioFlowMetrics metrics = new NioFlowMetrics() {
        };

        assertDoesNotThrow(() -> {
            metrics.executionCompleted(1);
            metrics.executionFailed(new IllegalStateException("boom"), 1);
            metrics.executionFiltered(1);
            metrics.stageCompleted("stage", 1);
            metrics.stageRetried("stage");
            metrics.recoveryApplied("recovery");
            metrics.valueDropped();
            metrics.queueDepth(3);
        });
    }
}
