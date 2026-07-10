package dev.nioflow.application.facade;

import dev.nioflow.core.facade.NioEngine;
import dev.nioflow.core.facade.NioFlowMetrics;
import dev.nioflow.core.facade.NioFlowTracer;
import dev.nioflow.core.model.Diagnostics;
import dev.nioflow.core.model.Link;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

/**
 * The engine of a scoped nio-flow: an ephemeral, caller-private chain over the live
 * engine's resources. Links declared through the scope extend a snapshot of the
 * shared chain taken at creation; injections are buffered so the caller may declare
 * links and inject in any order; {@code await} then runs every buffered value
 * through the scope's private chain and waits for those values only — never for
 * values of the shared chain or of other scopes. This is what lets one empty,
 * long-lived nio-flow serve many independent callers, each declaring its own stages
 * at runtime.
 *
 * <p>Scope values ride the live engine — its threads, executor, backpressure and
 * global observers — as private-chain calls, so they are released on finish and
 * never parked. {@code onComplete}/{@code onError} on the scope observe only the
 * scope's own values. Global concerns are not the scope's to change: structural
 * edits, {@code metrics} and {@code trace} are rejected, and closing a scope never
 * stops the shared engine.
 */
final class ScopeNioEngine implements NioEngine {

    private final NioEngine live;
    private final List<Link> links;
    private final List<Pending> buffered = new ArrayList<>();
    private final List<CompletableFuture<Object>> awaiting = new ArrayList<>();
    private final List<Consumer<Object>> completeHandlers = new ArrayList<>();
    private final List<Consumer<Throwable>> errorHandlers = new ArrayList<>();
    private Object lastResult;
    private boolean sealed;

    /** A buffered injection, waiting for the terminal operation to dispatch it. */
    private record Pending(Object input, Map<String, Object> context) {
    }

    /**
     * @param live the shared engine the scope rides; its current chain version is
     *             the scope's starting point
     */
    ScopeNioEngine(NioEngine live) {
        this.live = live;
        this.links = new ArrayList<>(live.chain());
    }

    @Override
    public synchronized void append(Link link) {
        if (sealed) {
            throw new IllegalStateException("scope is sealed: its chain can no longer change");
        }
        links.add(link);
    }

    @Override
    public void inject(Object input) {
        inject(input, Map.of());
    }

    @Override
    public synchronized void inject(Object input, Map<String, Object> context) {
        buffered.add(new Pending(input, context));
    }

    @Override
    public synchronized CompletableFuture<Object> call(Object input, Map<String, Object> context) {
        return dispatch(input, context);
    }

    @Override
    public CompletableFuture<Object> call(Object input, Map<String, Object> context, List<Link> chain) {
        return live.call(input, context, chain); // a nested scope dispatches through us
    }

    @Override
    public synchronized List<Link> chain() {
        return List.copyOf(links); // lets scoped() nest: the child starts from these
    }

    /** Runs one value through the scope's chain as declared right now. */
    private CompletableFuture<Object> dispatch(Object input, Map<String, Object> context) {
        CompletableFuture<Object> reply = live.call(input, context, List.copyOf(links));
        observe(reply);
        awaiting.add(reply);
        return reply;
    }

    /** Wires the scope-local observers to one value's outcome. */
    private void observe(CompletableFuture<Object> reply) {
        List<Consumer<Object>> completions = List.copyOf(completeHandlers);
        List<Consumer<Throwable>> failures = List.copyOf(errorHandlers);
        reply.whenComplete((value, error) -> {
            if (error == null) {
                completions.forEach(handler -> deliver(handler, value));
            } else if (!(error instanceof CancellationException)) {
                // cancelled = the value left deliberately (filter, empty fanOut):
                // like on the shared chain, no handler fires
                failures.forEach(handler -> deliver(handler, error));
            }
        });
    }

    @Override
    public synchronized Object await() {
        dispatchBuffered();
        List<CompletableFuture<Object>> waited = List.copyOf(awaiting);
        awaiting.clear();
        Throwable failure = null;
        for (CompletableFuture<Object> reply : waited) {
            try {
                lastResult = reply.join();
            } catch (CancellationException dropped) {
                // filtered values leave deliberately and don't count
            } catch (CompletionException error) {
                if (failure == null) {
                    failure = error.getCause() != null ? error.getCause() : error;
                }
            }
        }
        if (failure != null) {
            // like the shared join(): thrown once, then the scope stays usable
            throw new CompletionException(failure);
        }
        return lastResult;
    }

    @Override
    public synchronized Object await(Duration timeout) {
        dispatchBuffered();
        long deadline = System.nanoTime() + timeout.toNanos();
        Throwable failure = null;
        while (!awaiting.isEmpty()) {
            CompletableFuture<Object> reply = awaiting.getFirst();
            try {
                lastResult = reply.get(Math.max(1, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
            } catch (CancellationException dropped) {
                // filtered values leave deliberately and don't count
            } catch (ExecutionException error) {
                if (failure == null) {
                    failure = error.getCause() != null ? error.getCause() : error;
                }
            } catch (TimeoutException stillBusy) {
                // the rest stays awaited, so a later join can still complete
                throw new CompletionException(stillBusy);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new CompletionException(interrupted);
            }
            awaiting.removeFirst();
        }
        if (failure != null) {
            throw new CompletionException(failure);
        }
        return lastResult;
    }

    /** Must be called with the scope's monitor held. */
    private void dispatchBuffered() {
        for (Pending pending : buffered) {
            dispatch(pending.input(), pending.context());
        }
        buffered.clear();
    }

    @Override
    public int nextDecision() {
        return live.nextDecision(); // ids stay unique engine-wide, across every scope
    }

    @Override
    public synchronized void addErrorHandler(Consumer<Throwable> handler) {
        errorHandlers.add(handler); // scope-local: observes only this scope's values
    }

    @Override
    public synchronized void addCompleteHandler(Consumer<Object> handler) {
        completeHandlers.add(handler); // scope-local: observes only this scope's values
    }

    @Override
    public synchronized void seal() {
        sealed = true;
    }

    @Override
    public void release() {
        // scope values are private-chain calls: released on finish, never parked
    }

    @Override
    public void splice(String anchor, Splice position, List<Link> links) {
        throw new IllegalStateException(
                "structural edits belong to the shared nio-flow, not a scope: a scope just declares its own links");
    }

    @Override
    public void metrics(NioFlowMetrics metrics) {
        throw new IllegalStateException("register observability on the shared nio-flow, not on a scope");
    }

    @Override
    public void trace(NioFlowTracer tracer) {
        throw new IllegalStateException("register observability on the shared nio-flow, not on a scope");
    }

    @Override
    public Diagnostics diagnostics() {
        return live.diagnostics();
    }

    @Override
    public void shutdown(Duration gracePeriod) {
        // closing a scope must never stop the shared engine; the scope itself owns
        // nothing to release
    }

    /** Runs a scope observer, swallowing anything it throws. */
    private static <V> void deliver(Consumer<V> handler, V payload) {
        try {
            handler.accept(payload);
        } catch (Throwable ignored) {
            // an observer must never break the caller's wait
        }
    }
}
