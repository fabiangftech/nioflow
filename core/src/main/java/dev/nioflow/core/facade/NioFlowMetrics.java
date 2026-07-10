package dev.nioflow.core.facade;

/**
 * Observability port: the engine reports value lifecycle events and stage timings
 * here. Implementations must be fast and must never throw — callbacks run on the
 * engine's own threads. Plug an adapter from {@code io.nio-flow.infrastructure} or
 * implement it yourself; register with {@code Pipeline.metrics(...)} before
 * injecting values.
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

    /** A value failed for good, after exhausting recoveries. */
    default void failed(Throwable error) {
    }

    /** A value was deliberately dropped by a filter. */
    default void dropped() {
    }

    /** A value split into {@code children} values (0 means the parent vanished). */
    default void fannedOut(int children) {
    }

    /** A handle/submit stage execution finished; {@code name} is null when unnamed. */
    default void stage(String name, boolean async, long elapsedNanos, boolean success) {
    }
}
