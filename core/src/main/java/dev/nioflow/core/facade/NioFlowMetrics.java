package dev.nioflow.core.facade;

/**
 * Metrics SPI: the engine pushes events here when an implementation is
 * installed via NioEngine.metrics(). All methods default to no-op so
 * implementations override only what they need. Callbacks run on engine
 * threads (boss or workers) — they must be fast and never throw.
 */
public interface NioFlowMetrics {

    default void executionCompleted(long nanos) {
    }

    default void executionFailed(Throwable error, long nanos) {
    }

    default void executionFiltered(long nanos) {
    }

    default void stageCompleted(String stage, long nanos) {
    }

    default void recoveryApplied(String recovery) {
    }

    default void valueDropped() {
    }

    default void queueDepth(int pending) {
    }
}
