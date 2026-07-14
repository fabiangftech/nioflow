package dev.nioflow.infrastructure.reactive;

import dev.nioflow.core.facade.Context;
import dev.nioflow.core.facade.Lane;
import dev.nioflow.core.facade.Segment;
import dev.nioflow.core.model.RateLimit;
import dev.nioflow.core.model.Retry;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

/** Delegating mirror of a fork lane. See DefaultReactiveFlow. */
class DefaultReactiveLane<T> implements ReactiveLane<T> {

    final Lane<T> delegate;

    DefaultReactiveLane(Lane<T> delegate) {
        this.delegate = delegate;
    }

    private ReactiveLane<T> wrap(Lane<T> result) {
        return result == delegate ? this : new DefaultReactiveLane<>(result);
    }

    // ── the reactive steps ──

    @Override
    public ReactiveLane<T> handleMono(String name, Function<T, Mono<T>> call) {
        return wrap(delegate.handle(name, value -> Blocking.await(call.apply(value))));
    }

    @Override
    public ReactiveLane<T> handleMono(String name, Function<T, Mono<T>> call, Duration budget) {
        return wrap(delegate.handle(name, value -> Blocking.await(call.apply(value).timeout(budget))));
    }

    @Override
    public ReactiveLane<T> handleMono(String name, Function<T, Mono<T>> call, Retry retry) {
        return wrap(delegate.handle(name, value -> Blocking.await(call.apply(value)), retry));
    }

    @Override
    public ReactiveLane<T> handleMono(String name, Function<T, Mono<T>> call, Duration budget, Retry retry) {
        return wrap(delegate.handle(name, value -> Blocking.await(call.apply(value).timeout(budget)), retry));
    }

    @Override
    public <R> ReactiveLane<R> adaptMono(Function<T, Mono<R>> call) {
        return new DefaultReactiveLane<>(delegate.adapt(value -> Blocking.await(call.apply(value))));
    }

    @Override
    public <R> ReactiveLane<List<R>> adaptFlux(Function<T, Flux<R>> call) {
        return new DefaultReactiveLane<>(delegate.adapt(value -> Blocking.await(call.apply(value).collectList())));
    }

    @Override
    public <R, C> ReactiveLane<C> fanOutMono(String name, List<Function<T, Mono<R>>> branches,
                                             Function<List<R>, C> join) {
        return new DefaultReactiveLane<>(delegate.fanOut(name, Blocking.branches(branches), join));
    }

    // ── everything else ──

    @Override
    public ReactiveLane<T> handle(UnaryOperator<T> function) {
        return wrap(delegate.handle(function));
    }

    @Override
    public ReactiveLane<T> handle(String name, UnaryOperator<T> function) {
        return wrap(delegate.handle(name, function));
    }

    @Override
    public ReactiveLane<T> handle(String name, UnaryOperator<T> function, Duration timeout) {
        return wrap(delegate.handle(name, function, timeout));
    }

    @Override
    public ReactiveLane<T> handle(String name, UnaryOperator<T> function, Retry retry) {
        return wrap(delegate.handle(name, function, retry));
    }

    @Override
    public ReactiveLane<T> handle(String name, UnaryOperator<T> function, Duration timeout, Retry retry) {
        return wrap(delegate.handle(name, function, timeout, retry));
    }

    @Override
    public ReactiveLane<T> handle(String name, UnaryOperator<T> function, RateLimit rateLimit) {
        return wrap(delegate.handle(name, function, rateLimit));
    }

    @Override
    public ReactiveLane<T> handleContextual(BiFunction<T, Context, T> function) {
        return wrap(delegate.handleContextual(function));
    }

    @Override
    public ReactiveLane<T> handleContextual(String name, BiFunction<T, Context, T> function) {
        return wrap(delegate.handleContextual(name, function));
    }

    @Override
    public ReactiveLane<T> handleSync(UnaryOperator<T> function) {
        return wrap(delegate.handleSync(function));
    }

    @Override
    public ReactiveLane<T> handleSync(String name, UnaryOperator<T> function) {
        return wrap(delegate.handleSync(name, function));
    }

    @Override
    public ReactiveLane<T> background(Consumer<T> effect) {
        return wrap(delegate.background(effect));
    }

    @Override
    public ReactiveLane<T> background(String name, Consumer<T> effect) {
        return wrap(delegate.background(name, effect));
    }

    @Override
    public <R> ReactiveLane<R> adapt(Function<T, R> function) {
        return new DefaultReactiveLane<>(delegate.adapt(function));
    }

    @Override
    public <R, C> ReactiveLane<C> fanOut(List<Function<T, R>> branches, Function<List<R>, C> join) {
        return new DefaultReactiveLane<>(delegate.fanOut(branches, join));
    }

    @Override
    public <R, C> ReactiveLane<C> fanOut(String name, List<Function<T, R>> branches,
                                         Function<List<R>, C> join) {
        return new DefaultReactiveLane<>(delegate.fanOut(name, branches, join));
    }

    @Override
    public <R> ReactiveLane<R> batch(int size, Duration window, Function<List<T>, List<R>> bulk) {
        return new DefaultReactiveLane<>(delegate.batch(size, window, bulk));
    }

    @Override
    public <R> ReactiveLane<R> batch(String name, int size, Duration window, Function<List<T>, List<R>> bulk) {
        return new DefaultReactiveLane<>(delegate.batch(name, size, window, bulk));
    }

    @Override
    public <R> ReactiveLane<R> use(Segment<T, R> segment) {
        return new DefaultReactiveLane<>(delegate.use(segment));
    }

    @Override
    public <R> ReactiveLane<R> use(String region, Segment<T, R> segment) {
        return new DefaultReactiveLane<>(delegate.use(region, segment));
    }

    @Override
    public <R> ReactiveLane<T> fork(Segment<T, R> sub) {
        return wrap(delegate.fork(sub));
    }

    @Override
    public <R> ReactiveLane<T> fork(String name, Segment<T, R> sub) {
        return wrap(delegate.fork(name, sub));
    }

    @Override
    public ReactiveLane<T> filter(Predicate<T> predicate) {
        return wrap(delegate.filter(predicate));
    }

    @Override
    public ReactiveLane<T> recover(Function<Throwable, T> function) {
        return wrap(delegate.recover(function));
    }

    @Override
    public ReactiveLane<T> recover(String name, Function<Throwable, T> function) {
        return wrap(delegate.recover(name, function));
    }

    @Override
    public ReactiveLaneCondition<T> when(Predicate<T> predicate) {
        return new DefaultReactiveLaneCondition<>(delegate.when(predicate));
    }

    @Override
    public ReactiveLaneCases<T> match() {
        return new DefaultReactiveLaneCases<>(delegate.match());
    }
}
