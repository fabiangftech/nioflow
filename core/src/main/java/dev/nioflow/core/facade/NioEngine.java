package dev.nioflow.core.facade;

import dev.nioflow.core.model.Link;
import dev.nioflow.core.model.Splice;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

public interface NioEngine {

    void inject(Object input);

    void inject(Object input, Map<String, Object> context);

    CompletableFuture<Object> call(Object input, Map<String, Object> context);

    CompletableFuture<Object> call(Object input, Map<String, Object> context, List<Link> chain);

    /**
     * Keyed call: executions sharing a (non-null) key are pinned to the same
     * boss and processed strictly one at a time, in submission order —
     * Kafka-partition style ordering per business key. Distinct keys keep
     * full parallelism; a null key behaves exactly like the unkeyed call.
     */
    CompletableFuture<Object> call(Object input, Map<String, Object> context, List<Link> chain, Object key);

    /**
     * The same call, plus the handle to stop it. Separate from call() rather
     * than replacing it: the plain call returns the execution's raw future with
     * no wrapper allocated, and a caller who cannot cancel should not pay for
     * the handle.
     *
     * <p>The future it hands back carries the CANCELLED sentinel like any other
     * raw future; mapping that to a CancellationException is the flow facade's
     * job, not the engine's.
     */
    Cancellable<Object> callCancellable(Object input, Map<String, Object> context,
                                        List<Link> chain, Object key);

    /**
     * Validates a chain and compiles it into a reusable {@link PreparedChain}.
     * Throws {@link ChainValidationException} on a broken definition — the same
     * check {@code seal()} runs — so a {@link Pipeline}'s problems surface at
     * build time, not at the first request. For the fixed per-request pipeline.
     */
    PreparedChain prepare(List<Link> chain);

    /**
     * Compiles a per-request chain into a {@link PreparedChain} WITHOUT
     * validating it: a per-request pipeline mints anonymous names from a
     * counter of its own, so its names may legitimately duplicate the shared
     * chain's — which validation forbids. For the cached dynamic snapshot.
     */
    PreparedChain planFor(List<Link> chain);

    /**
     * Runs an execution off a prebuilt plan: no defensive copy, no decision
     * rescan, dispatch straight off the compiled fusion windows. Same result as
     * the {@link #call(Object, Map, List, Object)} form — the plan is an
     * optimization, never a semantic.
     */
    CompletableFuture<Object> call(Object input, Map<String, Object> context, PreparedChain prepared, Object key);

    /** The prepared-plan call, plus the handle to stop it — see the List form. */
    Cancellable<Object> callCancellable(Object input, Map<String, Object> context,
                                        PreparedChain prepared, Object key);

    List<Link> chain();

    void append(Link link);

    void seal();

    void release();

    void splice(String anchor, Splice position, List<Link> links);

    /**
     * Remembers the contiguous span [first, last] (by link identity) as a
     * named region, so spliceRegion can swap the whole span atomically.
     * Registered by use(name, segment) at build time.
     */
    void rememberRegion(String name, Link first, Link last);

    /**
     * Atomically replaces the whole remembered region — one chain swap, one
     * validation (on sealed chains), one recompile — and re-points the
     * region at the new links so it can be swapped again. An empty list
     * removes the region and its registration. Fails if a boundary link was
     * edited away by a single-link splice.
     */
    void spliceRegion(String region, List<Link> links);

    int nextDecision();

    void metrics(NioFlowMetrics metrics);

    void addErrorHandler(Consumer<Throwable> handler);

    void addCompleteHandler(Consumer<Object> handler);

    Object await();

    Object await(Duration timeout);

    /**
     * Graceful drain: stops accepting new work immediately (call/inject are
     * rejected), waits up to the grace period for in-flight executions to
     * finish, and returns how many were still running when it gave up
     * (0 = clean drain). Engine-owned executors are terminated afterwards;
     * JVM-shared executors survive and stragglers complete on their own.
     */
    int shutdown(Duration gracePeriod);
}
