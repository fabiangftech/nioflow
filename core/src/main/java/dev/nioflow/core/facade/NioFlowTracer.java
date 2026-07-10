package dev.nioflow.core.facade;

/**
 * Trace port: the engine reports every transition of every value here, identified
 * by the value's injection sequence (fan-out children share the parent's).
 * Registering a tracer with {@code Pipeline.trace(...)} is the opt-in — there is
 * no cost when none is registered. Implementations must be fast and must never
 * throw; they run on the engine's threads and receive raw payloads, so mind what
 * you log. Batch groups are not traced per value.
 */
public interface NioFlowTracer {

    /** A value entered the nio-flow. */
    default void injected(long value, Object payload) {
    }

    /** A handle/submit stage ran; {@code error} is null on success, {@code out} on failure. */
    default void stage(long value, String name, boolean async, Object in, Object out, Throwable error) {
    }

    /** A {@code when}/{@code match} decision routed the value. */
    default void lane(long value, int decision, boolean outcome) {
    }

    /** A filter dropped the value. */
    default void dropped(long value, Object payload) {
    }

    /** The value split into {@code children} values. */
    default void fannedOut(long value, int children) {
    }

    /** A recovery turned the error into {@code fallback} and the value resumed. */
    default void recovered(long value, Throwable error, Object fallback) {
    }

    /** The value failed for good. */
    default void failed(long value, Throwable error) {
    }

    /** The value reached the end of the chain. */
    default void completed(long value, Object result) {
    }
}
