package dev.nioflow.application.facade;

import dev.nioflow.core.facade.FlowResult;
import dev.nioflow.core.facade.NioEngine;
import dev.nioflow.core.facade.NioFlow;
import dev.nioflow.core.model.Guard;
import dev.nioflow.core.model.Link;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

public class DefaultNioFlow<I, T> extends AbstractNioFlow<I, T> implements AutoCloseable {

    private final NioEngine nioEngine;
    private final AtomicInteger anonymousLinks;
    private final List<Guard> guards;

    public DefaultNioFlow() {
        this(new DefaultNioEngine());
    }

    public DefaultNioFlow(NioEngine nioEngine) {
        this(nioEngine, new AtomicInteger(), List.of());
    }

    private DefaultNioFlow(NioEngine nioEngine, AtomicInteger anonymousLinks, List<Guard> guards) {
        this.nioEngine = nioEngine;
        this.anonymousLinks = anonymousLinks;
        this.guards = guards;
    }

    /**
     * Typed entry point: the Class anchors I (what just accepts) and the
     * pipeline starts with T = I; only adapt() changes T from there on.
     */
    public static <I> DefaultNioFlow<I, I> from(Class<I> type) {
        return new DefaultNioFlow<>();
    }

    public static <I> DefaultNioFlow<I, I> from(Class<I> type, NioEngine nioEngine) {
        return new DefaultNioFlow<>(nioEngine);
    }

    /**
     * Opens an independent execution: it starts from a snapshot of the shared
     * chain and any links added afterwards live only in that execution. N
     * concurrent requests can each do just(...)...execute() without clashing.
     */
    @Override
    public NioFlow<I, T> just(I input) {
        return new ExecutionNioFlow<>(nioEngine, input);
    }

    @Override
    public NioFlow<I, T> justAll(Iterable<I> inputs) {
        inputs.forEach(nioEngine::inject);
        return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public NioFlow<I, T> onComplete(Consumer<T> callback) {
        // Shared-definition scope: every execution of this engine reports here.
        nioEngine.addCompleteHandler(value -> callback.accept((T) value));
        return this;
    }

    @Override
    public NioFlow<I, T> onError(Consumer<Throwable> callback) {
        nioEngine.addErrorHandler(callback);
        return this;
    }

    @Override
    public NioFlow<I, T> key(Object key) {
        throw new IllegalStateException("key() orders one execution; start it with just(input) first");
    }

    @Override
    public T execute() {
        throw new IllegalStateException("This flow has no input; start an execution with just(input)");
    }

    @Override
    public CompletableFuture<T> executeAsync() {
        throw new IllegalStateException("This flow has no input; start an execution with just(input)");
    }

    @Override
    public FlowResult<T> executeResult() {
        throw new IllegalStateException("This flow has no input; start an execution with just(input)");
    }

    /**
     * Lifecycle lives ONLY here, on the root flow that owns the engine — the
     * NioFlow contract, branches, lanes and executions do not expose close().
     */
    @Override
    public void close() {
        nioEngine.shutdown(Duration.ofSeconds(5));
    }

    @Override
    NioEngine engine() {
        return nioEngine;
    }

    @Override
    void appendLink(Link link) {
        nioEngine.append(link);
    }

    @Override
    List<Guard> guards() {
        return guards;
    }

    @Override
    AbstractNioFlow<I, T> withGuards(List<Guard> guards) {
        return new DefaultNioFlow<>(nioEngine, anonymousLinks, guards);
    }

    @Override
    String anonymousName(String prefix) {
        return prefix + "-" + anonymousLinks.getAndIncrement();
    }
}
