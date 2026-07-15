package dev.nioflow.application.facade;

import dev.nioflow.core.facade.Cancellable;
import dev.nioflow.core.facade.Context;
import dev.nioflow.core.facade.FlowResult;
import dev.nioflow.core.facade.NioEngine;
import dev.nioflow.core.facade.PipelineRun;
import dev.nioflow.core.facade.PreparedChain;
import dev.nioflow.core.model.FlowSignal;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * One request over a prebuilt {@link dev.nioflow.core.facade.Pipeline}: it
 * carries only what varies per request — input, key, seeded context and
 * execution-scoped callbacks — and dispatches off the pipeline's compiled plan.
 * A fresh instance per {@code just()}, built on the request thread, so it needs
 * no synchronization.
 *
 * <p>The terminals are exactly {@link ExecutionNioFlow}'s: both route the
 * engine's raw future through {@link Requests}, so a Filter cut, a cancellation
 * and the seeded-context merge behave identically. The only difference is the
 * chain — here a {@link PreparedChain} instead of a per-request list.
 */
final class DefaultPipelineRun<R> implements PipelineRun<R> {

    private final NioEngine nioEngine;
    private final PreparedChain prepared;
    private final Object seed;
    private Object key;
    private Map<String, Object> context;
    private Consumer<Object> onComplete;
    private Consumer<Throwable> onError;

    DefaultPipelineRun(NioEngine nioEngine, PreparedChain prepared, Object seed) {
        this.nioEngine = nioEngine;
        this.prepared = prepared;
        this.seed = seed;
    }

    @Override
    public PipelineRun<R> key(Object key) {
        this.key = key;
        return this;
    }

    @Override
    public <V> PipelineRun<R> with(Context.Key<V> key, V value) {
        if (context == null) {
            context = new HashMap<>();
        }
        context.put(key.name(), value);
        return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public PipelineRun<R> onComplete(Consumer<R> callback) {
        Consumer<Object> untyped = value -> callback.accept((R) value);
        onComplete = onComplete == null ? untyped : onComplete.andThen(untyped);
        return this;
    }

    @Override
    public PipelineRun<R> onError(Consumer<Throwable> callback) {
        onError = onError == null ? callback : onError.andThen(callback);
        return this;
    }

    @Override
    public R execute() {
        return Requests.typed(rawFuture(null).join());
    }

    @Override
    public CompletableFuture<R> executeAsync() {
        return rawFuture(null).thenApply(Requests::typed);
    }

    @Override
    public CompletableFuture<R> executeAsync(Map<String, Object> runContext) {
        return rawFuture(runContext).thenApply(Requests::typed);
    }

    @Override
    @SuppressWarnings("unchecked")
    public FlowResult<R> executeResult() {
        Object value = rawFuture(null).join();
        if (value == FlowSignal.FILTERED) {
            return new FlowResult.Filtered<>();
        }
        if (value == FlowSignal.CANCELLED) {
            return new FlowResult.Cancelled<>();
        }
        return new FlowResult.Completed<>((R) value);
    }

    @Override
    public Cancellable<R> executeCancellable() {
        return executeCancellable(null);
    }

    @Override
    public Cancellable<R> executeCancellable(Map<String, Object> runContext) {
        Cancellable<Object> raw = nioEngine.callCancellable(
                seed, Requests.mergeContext(context, runContext), prepared, key);
        return Requests.cancellable(raw, onComplete, onError);
    }

    private CompletableFuture<Object> rawFuture(Map<String, Object> runContext) {
        CompletableFuture<Object> raw = nioEngine.call(
                seed, Requests.mergeContext(context, runContext), prepared, key);
        return Requests.decorate(raw, onComplete, onError);
    }
}
