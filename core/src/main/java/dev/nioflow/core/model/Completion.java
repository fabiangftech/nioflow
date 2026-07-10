package dev.nioflow.core.model;

/**
 * A reaped stage result for one flowing value: on success the flow resumes with
 * {@code result}; on failure {@code error} is set and the flow short-circuits.
 */
public record Completion(FlowValue flow, Object result, Throwable error) {
}
