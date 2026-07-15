package dev.nioflow.infrastructure.reactive;

import dev.nioflow.core.facade.Cancellable;
import dev.nioflow.core.facade.Context;
import dev.nioflow.core.facade.FlowResult;
import dev.nioflow.core.facade.NioStep;
import dev.nioflow.core.facade.Segment;
import dev.nioflow.core.model.RateLimit;
import dev.nioflow.core.model.Retry;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

/**
 * Delegating mirror of a per-request pipeline. See DefaultReactiveFlow — the
 * default budget it carries comes from the flow that opened this pipeline, and
 * applies to every reactive step here that declares none of its own.
 */
class DefaultReactiveStep<T, O> implements ReactiveStep<T, O> {

    final NioStep<T, O> delegate;
    final ReactiveConfig config;

    DefaultReactiveStep(NioStep<T, O> delegate, ReactiveConfig config) {
        this.delegate = delegate;
        this.config = config;
    }

    private ReactiveStep<T, O> wrap(NioStep<T, O> result) {
        return result == delegate ? this : new DefaultReactiveStep<>(result, config);
    }

    private <R> ReactiveStep<R, O> retyped(NioStep<R, O> result) {
        return new DefaultReactiveStep<>(result, config);
    }

    // ── the reactive steps ──

    @Override
    public ReactiveStep<T, O> handleMono(String name, Function<T, Mono<T>> call) {
        return handleMono(name, call, config.budget());
    }

    @Override
    public ReactiveStep<T, O> handleMono(String name, Function<T, Mono<T>> call, Duration budget) {
        if (config.preferAsync()) {
            return handleMonoAsync(name, call, budget);
        }
        return wrap(delegate.handle(name, value -> Blocking.await(Blocking.budgeted(call.apply(value), budget))));
    }

    @Override
    public ReactiveStep<T, O> handleMono(String name, Function<T, Mono<T>> call, Retry retry) {
        return handleMono(name, call, config.budget(), retry);
    }

    @Override
    public ReactiveStep<T, O> handleMono(String name, Function<T, Mono<T>> call, Duration budget, Retry retry) {
        if (config.preferAsync()) {
            return wrap(delegate.handleAsync(name, value -> call.apply(value).toFuture(), budget, retry));
        }
        return wrap(delegate.handle(name,
                value -> Blocking.await(Blocking.budgeted(call.apply(value), budget)), retry));
    }

    @Override
    public ReactiveStep<T, O> handleMonoAsync(String name, Function<T, Mono<T>> call) {
        return handleMonoAsync(name, call, config.budget());
    }

    @Override
    public ReactiveStep<T, O> handleMonoAsync(String name, Function<T, Mono<T>> call, Duration budget) {
        return wrap(delegate.handleAsync(name, value -> call.apply(value).toFuture(), budget));
    }

    @Override
    public <R> ReactiveStep<R, O> adaptMonoAsync(Function<T, Mono<R>> call) {
        return adaptMonoAsync(call, config.budget());
    }

    @Override
    public <R> ReactiveStep<R, O> adaptMonoAsync(Function<T, Mono<R>> call, Duration budget) {
        return retyped(delegate.adaptAsync(value -> call.apply(value).toFuture(), budget));
    }

    @Override
    public ReactiveStep<T, O> handleAsync(String name, Function<T, CompletionStage<T>> call) {
        return wrap(delegate.handleAsync(name, call));
    }

    @Override
    public ReactiveStep<T, O> handleAsync(String name, Function<T, CompletionStage<T>> call, Duration timeout) {
        return wrap(delegate.handleAsync(name, call, timeout));
    }

    @Override
    public ReactiveStep<T, O> handleAsync(String name, Function<T, CompletionStage<T>> call, Retry retry) {
        return wrap(delegate.handleAsync(name, call, retry));
    }

    @Override
    public ReactiveStep<T, O> handleAsync(String name, Function<T, CompletionStage<T>> call,
                                          Duration timeout, Retry retry) {
        return wrap(delegate.handleAsync(name, call, timeout, retry));
    }

    @Override
    public <R> ReactiveStep<R, O> adaptAsync(Function<T, CompletionStage<R>> call) {
        return retyped(delegate.adaptAsync(call));
    }

    @Override
    public <R> ReactiveStep<R, O> adaptAsync(Function<T, CompletionStage<R>> call, Duration timeout) {
        return retyped(delegate.adaptAsync(call, timeout));
    }

    @Override
    public <R> ReactiveStep<R, O> adaptMono(Function<T, Mono<R>> call) {
        return adaptMono(call, config.budget());
    }

    @Override
    public <R> ReactiveStep<R, O> adaptMono(Function<T, Mono<R>> call, Duration budget) {
        if (config.preferAsync()) {
            return adaptMonoAsync(call, budget);
        }
        return retyped(delegate.adapt(value -> Blocking.await(Blocking.budgeted(call.apply(value), budget))));
    }

    /** @deprecated see {@link ReactiveStep#adaptFlux(Function)} — prefer the bounded overload. */
    @Override
    @Deprecated(forRemoval = false)
    public <R> ReactiveStep<List<R>, O> adaptFlux(Function<T, Flux<R>> call) {
        return retyped(delegate.adapt(
                value -> Blocking.await(Blocking.budgeted(call.apply(value).collectList(), config.budget()))));
    }

    @Override
    public <R> ReactiveStep<List<R>, O> adaptFlux(Function<T, Flux<R>> call, int maxItems) {
        Blocking.checkMaxItems(maxItems);
        return retyped(delegate.adapt(value -> Blocking.awaitBounded(call.apply(value), maxItems, config.budget())));
    }

    @Override
    public <R, C> ReactiveStep<C, O> fanOutMono(String name, List<Function<T, Mono<R>>> branches,
                                                Function<List<R>, C> join) {
        return retyped(delegate.fanOutAsync(name, Blocking.asyncBranches(branches, config.budget()), join));
    }

    /**
     * Lazy on purpose: the execution starts inside a defer, so nothing runs
     * until somebody subscribes, and every subscription is a fresh execution
     * (which is what makes .retry()/.repeat() on the Mono re-run the pipeline).
     * A filter() cut completes with null, which Reactor turns into an empty
     * Mono — the two notions of "no value" line up.
     *
     * <p><b>A disposed subscription now cancels the execution behind it.</b>
     * It used to cancel a DEPENDENT future (fromFuture cancels what it was
     * handed, and thenApply's future is not the engine's), so the pipeline ran
     * on and charged the card for a client who had already hung up. The handle
     * from executeCancellable() reaches the execution itself: the chain stops
     * at the next link and the in-flight async call is cancelled.
     *
     * <p>Cooperative, as everywhere: a blocking handleMono parked on a worker
     * is not interrupted. That is the difference handleMonoAsync buys.
     *
     * <p>With keys declared ({@code propagate}), the subscriber context is read
     * INSIDE the defer — per subscription, never at assembly, which would share
     * one caller's trace id with every subscription of this Mono.
     */
    @Override
    public Mono<T> executeMono() {
        if (config.keys().isEmpty()) {
            return Mono.defer(() -> Monos.fromCancellable(delegate.executeCancellable()));
        }
        // The entries travel as the run's context, not as a write into the
        // pipeline (with() would leak this subscription's values into the next
        // one), and they are read per subscription inside the defer.
        return Mono.deferContextual(view ->
                Monos.fromCancellable(delegate.executeCancellable(Monos.seed(view, config.keys()))));
    }

    /**
     * The engine's one value, then Reactor's many: flatMapMany subscribes the
     * tail only once the pipeline has produced a value, so a failure is onError
     * before the tail runs and a filter() cut is an empty Flux. Laziness comes
     * from executeMono() — assembling this subscribes nothing.
     */
    @Override
    public <R> Flux<R> executeFlux(Function<T, Flux<R>> tail) {
        return executeMono().flatMapMany(tail);
    }

    // ── everything else ──

    @Override
    public ReactiveStep<T, O> handle(UnaryOperator<T> function) {
        return wrap(delegate.handle(function));
    }

    @Override
    public ReactiveStep<T, O> handle(String name, UnaryOperator<T> function) {
        return wrap(delegate.handle(name, function));
    }

    @Override
    public ReactiveStep<T, O> handle(String name, UnaryOperator<T> function, Duration timeout) {
        return wrap(delegate.handle(name, function, timeout));
    }

    @Override
    public ReactiveStep<T, O> handle(String name, UnaryOperator<T> function, Retry retry) {
        return wrap(delegate.handle(name, function, retry));
    }

    @Override
    public ReactiveStep<T, O> handle(String name, UnaryOperator<T> function, Duration timeout, Retry retry) {
        return wrap(delegate.handle(name, function, timeout, retry));
    }

    @Override
    public ReactiveStep<T, O> handle(String name, UnaryOperator<T> function, RateLimit rateLimit) {
        return wrap(delegate.handle(name, function, rateLimit));
    }

    @Override
    public ReactiveStep<T, O> handleContextual(BiFunction<T, Context, T> function) {
        return wrap(delegate.handleContextual(function));
    }

    @Override
    public ReactiveStep<T, O> handleContextual(String name, BiFunction<T, Context, T> function) {
        return wrap(delegate.handleContextual(name, function));
    }

    @Override
    public ReactiveStep<T, O> handleSync(UnaryOperator<T> function) {
        return wrap(delegate.handleSync(function));
    }

    @Override
    public ReactiveStep<T, O> handleSync(String name, UnaryOperator<T> function) {
        return wrap(delegate.handleSync(name, function));
    }

    @Override
    public ReactiveStep<T, O> background(Consumer<T> effect) {
        return wrap(delegate.background(effect));
    }

    @Override
    public ReactiveStep<T, O> background(String name, Consumer<T> effect) {
        return wrap(delegate.background(name, effect));
    }

    @Override
    public <R> ReactiveStep<R, O> adapt(Function<T, R> function) {
        return retyped(delegate.adapt(function));
    }

    @Override
    public ReactiveStep<T, O> filter(Predicate<T> predicate) {
        return wrap(delegate.filter(predicate));
    }

    @Override
    public <R, C> ReactiveStep<C, O> fanOut(List<Function<T, R>> branches, Function<List<R>, C> join) {
        return retyped(delegate.fanOut(branches, join));
    }

    @Override
    public <R, C> ReactiveStep<C, O> fanOut(String name, List<Function<T, R>> branches,
                                            Function<List<R>, C> join) {
        return retyped(delegate.fanOut(name, branches, join));
    }

    @Override
    public <R, C> ReactiveStep<C, O> fanOutAsync(List<Function<T, CompletionStage<R>>> branches,
                                                 Function<List<R>, C> join) {
        return retyped(delegate.fanOutAsync(branches, join));
    }

    @Override
    public <R, C> ReactiveStep<C, O> fanOutAsync(String name, List<Function<T, CompletionStage<R>>> branches,
                                                 Function<List<R>, C> join) {
        return retyped(delegate.fanOutAsync(name, branches, join));
    }

    @Override
    public <R> ReactiveStep<R, O> batch(int size, Duration window, Function<List<T>, List<R>> bulk) {
        return retyped(delegate.batch(size, window, bulk));
    }

    @Override
    public <R> ReactiveStep<R, O> batch(String name, int size, Duration window,
                                        Function<List<T>, List<R>> bulk) {
        return retyped(delegate.batch(name, size, window, bulk));
    }

    @Override
    public <R> ReactiveStep<T, O> fork(Segment<T, R> sub) {
        return wrap(delegate.fork(Lanes.budgeted(sub, config.budget(), config.preferAsync())));
    }

    @Override
    public <R> ReactiveStep<T, O> fork(String name, Segment<T, R> sub) {
        return wrap(delegate.fork(name, Lanes.budgeted(sub, config.budget(), config.preferAsync())));
    }

    @Override
    public <R> ReactiveStep<R, O> use(Segment<T, R> segment) {
        return retyped(delegate.use(Lanes.budgeted(segment, config.budget(), config.preferAsync())));
    }

    @Override
    public ReactiveStep<T, O> recover(Function<Throwable, T> function) {
        return wrap(delegate.recover(function));
    }

    @Override
    public ReactiveStep<T, O> recover(String name, Function<Throwable, T> function) {
        return wrap(delegate.recover(name, function));
    }

    @Override
    public ReactiveStep<T, O> onComplete(Consumer<T> callback) {
        return wrap(delegate.onComplete(callback));
    }

    @Override
    public ReactiveStep<T, O> onError(Consumer<Throwable> callback) {
        return wrap(delegate.onError(callback));
    }

    @Override
    public ReactiveStep<T, O> key(Object key) {
        return wrap(delegate.key(key));
    }

    @Override
    public <V> ReactiveStep<T, O> with(Context.Key<V> key, V value) {
        return wrap(delegate.with(key, value));
    }

    @Override
    public ReactiveStepCondition<T, O> when(Predicate<T> predicate) {
        return new DefaultReactiveStepCondition<>(delegate.when(predicate), config);
    }

    @Override
    public ReactiveStepCases<T, O> match() {
        return new DefaultReactiveStepCases<>(delegate.match(), config);
    }

    @Override
    public T execute() {
        return delegate.execute();
    }

    @Override
    public CompletableFuture<T> executeAsync() {
        return delegate.executeAsync();
    }

    @Override
    public CompletableFuture<T> executeAsync(Map<String, Object> context) {
        return delegate.executeAsync(context);
    }

    @Override
    public FlowResult<T> executeResult() {
        return delegate.executeResult();
    }

    @Override
    public Cancellable<T> executeCancellable() {
        return delegate.executeCancellable();
    }

    @Override
    public Cancellable<T> executeCancellable(Map<String, Object> context) {
        return delegate.executeCancellable(context);
    }
}
