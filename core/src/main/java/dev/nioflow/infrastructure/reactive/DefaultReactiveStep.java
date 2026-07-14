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

/** Delegating mirror of a per-request pipeline. See DefaultReactiveFlow. */
class DefaultReactiveStep<T, O> implements ReactiveStep<T, O> {

    final NioStep<T, O> delegate;

    DefaultReactiveStep(NioStep<T, O> delegate) {
        this.delegate = delegate;
    }

    private ReactiveStep<T, O> wrap(NioStep<T, O> result) {
        return result == delegate ? this : new DefaultReactiveStep<>(result);
    }

    // ── the reactive steps ──

    @Override
    public ReactiveStep<T, O> handleMono(String name, Function<T, Mono<T>> call) {
        return wrap(delegate.handle(name, value -> Blocking.await(call.apply(value))));
    }

    @Override
    public ReactiveStep<T, O> handleMono(String name, Function<T, Mono<T>> call, Duration budget) {
        return wrap(delegate.handle(name, value -> Blocking.await(call.apply(value).timeout(budget))));
    }

    @Override
    public ReactiveStep<T, O> handleMono(String name, Function<T, Mono<T>> call, Retry retry) {
        return wrap(delegate.handle(name, value -> Blocking.await(call.apply(value)), retry));
    }

    @Override
    public ReactiveStep<T, O> handleMono(String name, Function<T, Mono<T>> call, Duration budget, Retry retry) {
        return wrap(delegate.handle(name, value -> Blocking.await(call.apply(value).timeout(budget)), retry));
    }

    @Override
    public <R> ReactiveStep<R, O> adaptMono(Function<T, Mono<R>> call) {
        return new DefaultReactiveStep<>(delegate.adapt(value -> Blocking.await(call.apply(value))));
    }

    @Override
    public <R> ReactiveStep<R, O> adaptMono(Function<T, Mono<R>> call, Duration budget) {
        return new DefaultReactiveStep<>(delegate.adapt(value -> Blocking.await(call.apply(value).timeout(budget))));
    }

    @Override
    public <R> ReactiveStep<List<R>, O> adaptFlux(Function<T, Flux<R>> call) {
        return new DefaultReactiveStep<>(delegate.adapt(value -> Blocking.await(call.apply(value).collectList())));
    }

    @Override
    public <R, C> ReactiveStep<C, O> fanOutMono(String name, List<Function<T, Mono<R>>> branches,
                                                Function<List<R>, C> join) {
        return new DefaultReactiveStep<>(delegate.fanOut(name, Blocking.branches(branches), join));
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
        return new DefaultReactiveStep<>(delegate.adapt(function));
    }

    @Override
    public ReactiveStep<T, O> filter(Predicate<T> predicate) {
        return wrap(delegate.filter(predicate));
    }

    @Override
    public <R, C> ReactiveStep<C, O> fanOut(List<Function<T, R>> branches, Function<List<R>, C> join) {
        return new DefaultReactiveStep<>(delegate.fanOut(branches, join));
    }

    @Override
    public <R, C> ReactiveStep<C, O> fanOut(String name, List<Function<T, R>> branches,
                                            Function<List<R>, C> join) {
        return new DefaultReactiveStep<>(delegate.fanOut(name, branches, join));
    }

    @Override
    public <R> ReactiveStep<R, O> batch(int size, Duration window, Function<List<T>, List<R>> bulk) {
        return new DefaultReactiveStep<>(delegate.batch(size, window, bulk));
    }

    @Override
    public <R> ReactiveStep<R, O> batch(String name, int size, Duration window,
                                        Function<List<T>, List<R>> bulk) {
        return new DefaultReactiveStep<>(delegate.batch(name, size, window, bulk));
    }

    @Override
    public <R> ReactiveStep<T, O> fork(Segment<T, R> sub) {
        return wrap(delegate.fork(sub));
    }

    @Override
    public <R> ReactiveStep<T, O> fork(String name, Segment<T, R> sub) {
        return wrap(delegate.fork(name, sub));
    }

    @Override
    public <R> ReactiveStep<R, O> use(Segment<T, R> segment) {
        return new DefaultReactiveStep<>(delegate.use(segment));
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
        return new DefaultReactiveStepCondition<>(delegate.when(predicate));
    }

    @Override
    public ReactiveStepCases<T, O> match() {
        return new DefaultReactiveStepCases<>(delegate.match());
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
