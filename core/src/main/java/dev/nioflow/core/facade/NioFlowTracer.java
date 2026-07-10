package dev.nioflow.core.facade;

/**
 * Trace port: the engine reports every transition of every value here, identified
 * by the value's injection sequence (fan-out children share the parent's).
 * Registering a tracer with {@code NioFlow.trace(...)} is the opt-in — there is
 * no cost when none is registered. Implementations must be fast and must never
 * throw; they run on the engine's threads and receive raw payloads, so mind what
 * you log. Batch groups are not traced per value. Every method has an empty
 * default, so an implementation only overrides the transitions it cares about.
 */
public interface NioFlowTracer {

    /**
     * A value entered the nio-flow.
     *
     * @param value   the value's injection sequence — its identity in every later event
     * @param payload the injected payload
     */
    default void injected(long value, Object payload) {
    }

    /**
     * A handle/submit stage ran for the value.
     *
     * @param value the value's injection sequence
     * @param name  the stage name, or null when the stage is unnamed
     * @param async true for {@code submit} stages, false for {@code handle}
     * @param in    the payload entering the stage
     * @param out   the payload leaving the stage, or null on failure
     * @param error the failure, or null on success
     */
    default void stage(long value, String name, boolean async, Object in, Object out, Throwable error) {
    }

    /**
     * A {@code when}/{@code match} decision routed the value.
     *
     * @param value    the value's injection sequence
     * @param decision the fork's decision id, as rendered by diagnostics
     * @param outcome  the predicate's result — which lane the value takes
     */
    default void lane(long value, int decision, boolean outcome) {
    }

    /**
     * A filter dropped the value.
     *
     * @param value   the value's injection sequence
     * @param payload the payload at the moment it was dropped
     */
    default void dropped(long value, Object payload) {
    }

    /**
     * The value split into {@code children} independent values.
     *
     * @param value    the parent's injection sequence, inherited by every child
     * @param children how many values the parent split into
     */
    default void fannedOut(long value, int children) {
    }

    /**
     * A recovery turned the error into {@code fallback} and the value resumed.
     *
     * @param value    the value's injection sequence
     * @param error    the failure the recovery caught
     * @param fallback the replacement payload the value resumes with
     */
    default void recovered(long value, Throwable error, Object fallback) {
    }

    /**
     * The value failed for good, after exhausting recoveries.
     *
     * @param value the value's injection sequence
     * @param error the terminal failure
     */
    default void failed(long value, Throwable error) {
    }

    /**
     * The value reached the end of the chain.
     *
     * @param value  the value's injection sequence
     * @param result the payload the value finished with
     */
    default void completed(long value, Object result) {
    }
}
