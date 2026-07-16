package dev.nioflow.core.facade;

/**
 * Metrics SPI: the engine pushes events here when an implementation is
 * installed via NioEngine.metrics(). All methods default to no-op so
 * implementations override only what they need. Callbacks run on engine
 * threads (boss or workers) — they should be fast and should not throw.
 *
 * <p>A throw is nonetheless <b>contained</b>: the engine wraps every call to
 * this SPI and routes a thrown Throwable to the error handlers, so a buggy or
 * failing sink (an exporter outage, a sink that throws during its own shutdown)
 * can never hang a request future, skip an advance, or misroute a good value
 * into recover(). See RFC 0023.
 */
public interface NioFlowMetrics {

    default void executionCompleted(long nanos) {
    }

    default void executionFailed(Throwable error, long nanos) {
    }

    default void executionFiltered(long nanos) {
    }

    /**
     * An execution was cancelled from the outside (Cancellable.cancel(), a
     * disposed Mono). It lands in neither executionCompleted nor
     * executionFailed: a request the client walked away from is not a success
     * and not an error, and counting it as either is what makes a latency
     * dashboard lie.
     */
    default void executionCancelled(long nanos) {
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
