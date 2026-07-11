package dev.nioflow.core.model;

/**
 * What inject() does when the in-flight capacity is full:
 * BLOCK waits for a slot (the producer thread parks), DROP discards the new
 * value without running it (reported to error handlers), FAIL throws
 * RejectedExecutionException to the producer.
 */
public enum OverflowPolicy {

    BLOCK,
    DROP,
    FAIL
}
