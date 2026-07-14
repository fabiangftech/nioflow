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

    default void stageRetried(String stage) {
    }

    /** A detached sub-flow (fork) was spawned. */
    default void forkStarted(String fork) {
    }

    /**
     * A fork finished on its own. Its latency is NOT the request's — the main
     * line completed without waiting for it — which is why forks report here
     * instead of into executionCompleted.
     */
    default void forkCompleted(String fork, long nanos) {
    }

    /** A fork failed and no recover() inside it caught it. */
    default void forkFailed(String fork, Throwable error, long nanos) {
    }

    /** Forks currently running: what tells you whether a fork storm is real. */
    default void forksInFlight(int running) {
    }

    default void valueDropped() {
    }

    default void queueDepth(int pending) {
    }
}
