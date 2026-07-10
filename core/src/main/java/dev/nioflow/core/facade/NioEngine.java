package dev.nioflow.core.facade;


import dev.nioflow.core.model.Diagnostics;
import dev.nioflow.core.model.Link;

import java.time.Duration;
import java.util.Map;
import java.util.function.Consumer;

/**
 * The machinery behind a {@link NioFlow}: holds the shared chain of links and moves
 * the injected values through it. Untyped on purpose, so {@code adapt} can hand out a
 * differently-typed nio-flow view over the same running engine.
 *
 * <p>Contract: order is preserved per value, never across values — a fast value may
 * finish before a slower one that entered first, and a value blocked on slow IO must
 * not delay the values behind it. An error short-circuits only the value that failed
 * and is delivered to the error handlers without blocking the caller.
 */
public interface NioEngine {

    /**
     * Injects a new value; it starts walking the chain from the first link.
     *
     * @param input the value to inject
     */
    void inject(Object input);

    /**
     * Like {@link #inject(Object)} with seed metadata for the value's {@code FlowContext}.
     *
     * @param input   the value to inject
     * @param context initial metadata copied into the value's {@code FlowContext}
     */
    void inject(Object input, Map<String, Object> context);

    /**
     * Appends a link to the shared chain; values already at the end resume.
     *
     * @param link the next link of the chain
     */
    void append(Link link);

    /** Freezes the chain: any further {@link #append(Link)} throws. Idempotent. */
    void seal();

    /**
     * Reserves a unique id for a {@code when} fork's decision.
     *
     * @return an id never handed out before by this engine
     */
    int nextDecision();

    /**
     * Registers a non-blocking error handler. The most recent failures (a bounded
     * history) are replayed to it, so late registration still sees what went wrong
     * without the engine retaining every throwable forever.
     *
     * @param handler receives each terminal failure; must be fast and never throw
     */
    void addErrorHandler(Consumer<Throwable> handler);

    /**
     * Registers a handler invoked each time a value finishes the chain.
     *
     * @param handler receives each finished value; must be fast and never throw
     */
    void addCompleteHandler(Consumer<Object> handler);

    /**
     * Registers the observability sink; replaces any previous one.
     *
     * @param metrics the sink the engine reports lifecycle events and timings to
     */
    void metrics(NioFlowMetrics metrics);

    /**
     * Registers the transition tracer; replaces any previous one.
     *
     * @param tracer the sink for per-value transitions
     */
    void trace(NioFlowTracer tracer);

    /**
     * A consistent point-in-time snapshot of the chain shape and counters.
     *
     * @return the snapshot; its {@code toString()} renders the full dump
     */
    Diagnostics diagnostics();

    /**
     * Waits until every value has parked or failed, then returns the result of the
     * newest injected value that finished. If any value failed since the last call,
     * the failure is rethrown once and cleared, so the nio-flow stays usable.
     *
     * @return the result of the newest injected value that finished the chain
     */
    Object await();

    /**
     * Like {@link #await()} but gives up after the timeout with a {@code TimeoutException}.
     *
     * @param timeout how long to wait for quiescence before giving up
     * @return the result of the newest injected value that finished the chain
     */
    Object await(Duration timeout);

    /**
     * Graceful stop: waits up to {@code gracePeriod} for every in-flight value to
     * finish, then stops the loops and releases its own resources — never an external
     * executor. Idempotent: further calls return immediately.
     *
     * @param gracePeriod how long to let in-flight values finish before stopping
     */
    void shutdown(Duration gracePeriod);
}
