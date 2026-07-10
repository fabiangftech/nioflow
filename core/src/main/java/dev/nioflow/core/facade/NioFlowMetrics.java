package dev.nioflow.core.facade;

/**
 * Observability port: the engine reports value lifecycle events and stage timings
 * here. Implementations must be fast and must never throw — callbacks run on the
 * engine's own threads. Plug an adapter from {@code dev.nioflow.infrastructure} or
 * implement it yourself; register with {@code NioFlow.metrics(...)} before
 * injecting values. Every method has an empty default, so an implementation only
 * overrides the events it cares about.
 *
 * <p>The in-flight gauge derives from the events: {@code injected} − {@code completed}
 * − {@code failed} − {@code dropped}, plus {@code children − 1} per {@code fannedOut}.
 * Like {@code onComplete}, {@code completed} fires when a value reaches the end of
 * the declared chain — appending stages after a value parked makes it fire again
 * when the value re-parks.
 */
public interface NioFlowMetrics {

    /** A value was admitted into the nio-flow. */
    default void injected() {
    }

    /** A value reached the end of the chain. */
    default void completed() {
    }

    /**
     * A value failed for good, after exhausting recoveries.
     *
     * @param error the terminal failure — wrapped in a {@code StageException} when
     *              the failing stage was named
     */
    default void failed(Throwable error) {
    }

    /** A value was deliberately dropped by a filter. */
    default void dropped() {
    }

    /**
     * A value split into {@code children} independent values.
     *
     * @param children how many values the parent split into; 0 means the parent
     *                 vanished, like a filter drop
     */
    default void fannedOut(int children) {
    }

    /**
     * A handle/submit stage execution finished.
     *
     * @param name         the stage name, or null when the stage is unnamed
     * @param async        true for {@code submit} stages, false for {@code handle}
     * @param elapsedNanos wall-clock duration of this execution, in nanoseconds
     * @param success      false when the execution threw or timed out
     */
    default void stage(String name, boolean async, long elapsedNanos, boolean success) {
    }
}
