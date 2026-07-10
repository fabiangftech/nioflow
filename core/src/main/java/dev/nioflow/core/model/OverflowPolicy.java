package dev.nioflow.core.model;

/** What {@code just} does when the nio-flow is at its backpressure capacity. */
public enum OverflowPolicy {

    /** The producer thread waits until a value finishes and frees a slot. */
    BLOCK,

    /** The new value is discarded silently. */
    DROP,

    /** {@code just} throws {@code RejectedExecutionException}. */
    FAIL
}
