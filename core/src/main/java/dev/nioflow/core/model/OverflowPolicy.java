package dev.nioflow.core.model;

/**
 * What {@code just} does with a new value when the nio-flow is at its backpressure
 * capacity. Only injection is affected: values already flowing always re-enter the
 * engine's queues, whatever the policy.
 */
public enum OverflowPolicy {

    /**
     * The producer thread waits inside {@code just} until a value finishes and
     * frees a slot — natural flow control for producers that should slow down to
     * the nio-flow's pace. Mind feedback loops: a stage blocked injecting can
     * deadlock a full nio-flow.
     */
    BLOCK,

    /**
     * The new value is discarded silently — for lossy streams where staying
     * current beats completeness. Dropped values fire no handlers and never count
     * as in flight.
     */
    DROP,

    /**
     * {@code just} throws {@code RejectedExecutionException} — for producers that
     * must notice overload and react themselves.
     */
    FAIL
}
