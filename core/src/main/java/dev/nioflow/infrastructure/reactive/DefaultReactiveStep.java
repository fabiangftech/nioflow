package dev.nioflow.infrastructure.reactive;

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
import java.util.concurrent.CompletableFuture;
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
    final Duration budget;

    DefaultReactiveStep(NioStep<T, O> delegate) {
        this(delegate, null);
    }

    DefaultReactiveStep(NioStep<T, O> delegate, Duration budget) {
        this.delegate = delegate;
        this.budget = budget;
    }

    private ReactiveStep<T, O> wrap(NioStep<T, O> result) {
        return result == delegate ? this : new DefaultReactiveStep<>(result, budget);
    }

    private <R> ReactiveStep<R, O> retyped(NioStep<R, O> result) {
        return new DefaultReactiveStep<>(result, budget);
    }

    // ── the reactive steps ──

    @Override
    public ReactiveStep<T, O> handleMono(String name, Function<T, Mono<T>> call) {
        return handleMono(name, call, budget);
    }

    @Override
    public ReactiveStep<T, O> handleMono(String name, Function<T, Mono<T>> call, Duration budget) {
        return wrap(delegate.handle(name, value -> Blocking.await(Blocking.budgeted(call.apply(value), budget))));
    }

    @Override
    public ReactiveStep<T, O> handleMono(String name, Function<T, Mono<T>> call, Retry retry) {
        return handleMono(name, call, budget, retry);
    }

    @Override
    public ReactiveStep<T, O> handleMono(String name, Function<T, Mono<T>> call, Duration budget, Retry retry) {
        return wrap(delegate.handle(name,
                value -> Blocking.await(Blocking.budgeted(call.apply(value), budget)), retry));
    }

    @Override
    public <R> ReactiveStep<R, O> adaptMono(Function<T, Mono<R>> call) {
        return adaptMono(call, budget);
    }

    @Override
    public <R> ReactiveStep<R, O> adaptMono(Function<T, Mono<R>> call, Duration budget) {
        return retyped(delegate.adapt(value -> Blocking.await(Blocking.budgeted(call.apply(value), budget))));
    }

    @Override
    public <R> ReactiveStep<List<R>, O> adaptFlux(Function<T, Flux<R>> call) {
        return retyped(delegate.adapt(
                value -> Blocking.await(Blocking.budgeted(call.apply(value).collectList(), budget))));
    }

    @Override
    public <R> ReactiveStep<List<R>, O> adaptFlux(Function<T, Flux<R>> call, int maxItems) {
        Blocking.checkMaxItems(maxItems);
        return retyped(delegate.adapt(value -> Blocking.awaitBounded(call.apply(value), maxItems, budget)));
    }

    @Override
    public <R, C> ReactiveStep<C, O> fanOutMono(String name, List<Function<T, Mono<R>>> branches,
                                                Function<List<R>, C> join) {
        return retyped(delegate.fanOut(name, Blocking.branches(branches, budget), join));
    }

    /**
     * Lazy on purpose: fromFuture takes a SUPPLIER, so nothing runs until
     * somebody subscribes, and every subscription is a fresh execution (which
     * is what makes .retry()/.repeat() on the Mono re-run the pipeline).
     * A filter() cut completes with null, which Reactor turns into an empty
     * Mono — the two notions of "no value" line up.
     */
    @Override
    public Mono<T> executeMono() {
        return Mono.fromFuture(delegate::executeAsync);
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
        return wrap(delegate.fork(Lanes.budgeted(sub, budget)));
    }

    @Override
    public <R> ReactiveStep<T, O> fork(String name, Segment<T, R> sub) {
        return wrap(delegate.fork(name, Lanes.budgeted(sub, budget)));
    }

    @Override
    public <R> ReactiveStep<R, O> use(Segment<T, R> segment) {
        return retyped(delegate.use(Lanes.budgeted(segment, budget)));
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
        return new DefaultReactiveStepCondition<>(delegate.when(predicate), budget);
    }

    @Override
    public ReactiveStepCases<T, O> match() {
        return new DefaultReactiveStepCases<>(delegate.match(), budget);
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
    public FlowResult<T> executeResult() {
        return delegate.executeResult();
    }
}
