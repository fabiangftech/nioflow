package dev.nioflow.core.model;

/**
 * A reaped stage result for one flowing value: on success the flow resumes with
 * {@code result}; on failure {@code error} is set and the flow short-circuits.
 *
 * @param flow   the value the result belongs to
 * @param result the stage's output, or null when the stage failed
 * @param error  the stage's failure, or null when it succeeded
 */
public record Completion(FlowValue flow, Object result, Throwable error) {
}
