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

    List<Link> chain();

    void append(Link link);

    void seal();

    void release();

    void splice(String anchor, Splice position, List<Link> links);

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
