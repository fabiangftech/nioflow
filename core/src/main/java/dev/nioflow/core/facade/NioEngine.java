package dev.nioflow.core.facade;


import dev.nioflow.core.model.Diagnostics;
import dev.nioflow.core.model.Link;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
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
     * Injects a new value and returns a future resolved with that value's — and
     * only that value's — own outcome: completed with its end-of-chain result,
     * failed on terminal error (after every recovery had its chance), cancelled
     * when the value leaves deliberately (a {@code filter} drop, an empty fan-out,
     * a DROP backpressure policy). Rejected admission (closed engine, FAIL policy)
     * fails the future instead of throwing.
     *
     * @param input   the value to inject
     * @param context initial metadata copied into the value's {@code FlowContext}
     * @return a future delivering this value's own outcome
     */
    CompletableFuture<Object> call(Object input, Map<String, Object> context);

    /**
     * Like {@link #call(Object, Map)} but over an explicit, caller-private chain:
     * the value walks {@code chain} instead of the shared one and is released at
     * its end, never parked. The backbone of scoped flows — ephemeral per-caller
     * chains sharing this engine's threads, executor and backpressure.
     *
     * @param input   the value to inject
     * @param context initial metadata copied into the value's {@code FlowContext}
     * @param chain   the private chain this value walks, owned by the caller
     * @return a future delivering this value's own outcome
     */
    CompletableFuture<Object> call(Object input, Map<String, Object> context, List<Link> chain);

    /**
     * The current version of the shared chain, as an immutable snapshot — what a
     * scope starts from.
     *
     * @return the links of the current chain version, in order
     */
    List<Link> chain();

    /**
     * Appends a link to the shared chain; values already at the end resume.
     *
     * @param link the next link of the chain
     */
    void append(Link link);

    /** Freezes the chain: any further {@link #append(Link)} throws. Idempotent. */
    void seal();

    /**
     * Stops parking finished values: from now on a value reaching the end of the
     * chain is released, like under {@link #seal()}, but the chain stays open to
     * appends and edits — those simply no longer resume already-finished values.
     * The switch for long-running request-driven flows that must stay editable
     * without retaining every finished value. Idempotent, not reversible.
     */
    void release();

    /** Where a {@link #splice} places its links relative to the anchor. */
    enum Splice {
        /** The links go right before the anchor; the anchor stays. */
        BEFORE,
        /** The links go right after the anchor; the anchor stays. */
        AFTER,
        /** The links take the anchor's place; an empty list removes it. */
        REPLACE
    }

    /**
     * Structural edit: starts a new version of the chain with {@code links} placed
     * relative to the anchor — the first named stage called {@code anchor}. The old
     * version stays frozen and every value already in flight finishes on it; values
     * injected after the edit walk the new version. Values parked at the end of the
     * old version are released: a later {@link #append(Link)} no longer resumes them.
     * Each spliced link additionally inherits the anchor's guards, so an edit inside
     * a fork lane stays inside that lane.
     *
     * <p>A {@code REPLACE} with links remembers them as the anchor's <em>region</em>:
     * a later splice with the same anchor targets the whole region — REPLACE swaps
     * every link of it, BEFORE/AFTER insert around it — so a multi-link segment
     * (e.g. a fork of routes) can be re-declared repeatedly under one name. A
     * REPLACE with no links removes the region, or the named stage itself.
     *
     * @param anchor   the name of the stage or region the edit is anchored to
     * @param position where the links go relative to the anchor
     * @param links    the links to splice in; may be empty only for {@code REPLACE}
     * @throws IllegalArgumentException when no stage is named {@code anchor}
     * @throws IllegalStateException    when the nio-flow is sealed
     */
    void splice(String anchor, Splice position, List<Link> links);

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
