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
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

/**
 * Delegating mirror of a fork lane. See DefaultReactiveFlow — the default budget
 * it carries is the one the flow declared, handed down by the branch or segment
 * that opened this lane.
 */
class DefaultReactiveLane<T> implements ReactiveLane<T> {

    final Lane<T> delegate;
    final Duration budget;
    // Inside a pipe pipeline: route handleMono/adaptMono to the async
    // (future-holding) path instead of parking a worker on the Mono. See
    // ReactiveConfig#withPreferAsync.
    final boolean preferAsync;

    DefaultReactiveLane(Lane<T> delegate) {
        this(delegate, null, false);
    }

    DefaultReactiveLane(Lane<T> delegate, Duration budget) {
        this(delegate, budget, false);
    }

    DefaultReactiveLane(Lane<T> delegate, Duration budget, boolean preferAsync) {
        this.delegate = delegate;
        this.budget = budget;
        this.preferAsync = preferAsync;
    }

    private ReactiveLane<T> wrap(Lane<T> result) {
        return result == delegate ? this : new DefaultReactiveLane<>(result, budget, preferAsync);
    }

    private <R> ReactiveLane<R> retyped(Lane<R> result) {
        return new DefaultReactiveLane<>(result, budget, preferAsync);
    }

    // ── the reactive steps ──

    @Override
    public ReactiveLane<T> handleMono(String name, Function<T, Mono<T>> call) {
        return handleMono(name, call, budget);
    }

    @Override
    public ReactiveLane<T> handleMono(String name, Function<T, Mono<T>> call, Duration budget) {
        if (preferAsync) {
            return handleMonoAsync(name, call, budget);
        }
        return wrap(delegate.handle(name, value -> Blocking.await(Blocking.budgeted(call.apply(value), budget))));
    }

    @Override
    public ReactiveLane<T> handleMono(String name, Function<T, Mono<T>> call, Retry retry) {
        return handleMono(name, call, budget, retry);
    }

    @Override
    public ReactiveLane<T> handleMono(String name, Function<T, Mono<T>> call, Duration budget, Retry retry) {
        if (preferAsync) {
            return wrap(delegate.handleAsync(name, value -> call.apply(value).toFuture(), budget, retry));
        }
        return wrap(delegate.handle(name,
                value -> Blocking.await(Blocking.budgeted(call.apply(value), budget)), retry));
    }

    @Override
    public ReactiveLane<T> handleMonoAsync(String name, Function<T, Mono<T>> call) {
        return handleMonoAsync(name, call, budget);
    }

    @Override
    public ReactiveLane<T> handleMonoAsync(String name, Function<T, Mono<T>> call, Duration budget) {
        return wrap(delegate.handleAsync(name, value -> call.apply(value).toFuture(), budget));
    }

    @Override
    public <R> ReactiveLane<R> adaptMonoAsync(Function<T, Mono<R>> call) {
        return adaptMonoAsync(call, budget);
    }

    @Override
    public <R> ReactiveLane<R> adaptMonoAsync(Function<T, Mono<R>> call, Duration budget) {
        return retyped(delegate.adaptAsync(value -> call.apply(value).toFuture(), budget));
    }

    @Override
    public ReactiveLane<T> handleAsync(String name, Function<T, CompletionStage<T>> call) {
        return wrap(delegate.handleAsync(name, call));
    }

    @Override
    public ReactiveLane<T> handleAsync(String name, Function<T, CompletionStage<T>> call, Duration timeout) {
        return wrap(delegate.handleAsync(name, call, timeout));
    }

    @Override
    public ReactiveLane<T> handleAsync(String name, Function<T, CompletionStage<T>> call, Retry retry) {
        return wrap(delegate.handleAsync(name, call, retry));
    }

    @Override
    public ReactiveLane<T> handleAsync(String name, Function<T, CompletionStage<T>> call,
                                       Duration timeout, Retry retry) {
        return wrap(delegate.handleAsync(name, call, timeout, retry));
    }

    @Override
    public <R> ReactiveLane<R> adaptAsync(Function<T, CompletionStage<R>> call) {
        return retyped(delegate.adaptAsync(call));
    }

    @Override
    public <R> ReactiveLane<R> adaptAsync(Function<T, CompletionStage<R>> call, Duration timeout) {
        return retyped(delegate.adaptAsync(call, timeout));
    }

    @Override
    public <R> ReactiveLane<R> adaptMono(Function<T, Mono<R>> call) {
        return adaptMono(call, budget);
    }

    @Override
    public <R> ReactiveLane<R> adaptMono(Function<T, Mono<R>> call, Duration budget) {
        if (preferAsync) {
            return adaptMonoAsync(call, budget);
        }
        return retyped(delegate.adapt(value -> Blocking.await(Blocking.budgeted(call.apply(value), budget))));
    }

    /** @deprecated see {@link ReactiveLane#adaptFlux(Function)} — prefer the bounded overload. */
    @Override
    @Deprecated(forRemoval = false)
    public <R> ReactiveLane<List<R>> adaptFlux(Function<T, Flux<R>> call) {
        return retyped(delegate.adapt(
                value -> Blocking.await(Blocking.budgeted(call.apply(value).collectList(), budget))));
    }

    @Override
    public <R> ReactiveLane<List<R>> adaptFlux(Function<T, Flux<R>> call, int maxItems) {
        Blocking.checkMaxItems(maxItems);
        return retyped(delegate.adapt(value -> Blocking.awaitBounded(call.apply(value), maxItems, budget)));
    }

    @Override
    public <R, C> ReactiveLane<C> fanOutMono(String name, List<Function<T, Mono<R>>> branches,
                                             Function<List<R>, C> join) {
        return retyped(delegate.fanOutAsync(name, Blocking.asyncBranches(branches, budget), join));
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
        return retyped(delegate.adapt(function));
    }

    @Override
    public <R, C> ReactiveLane<C> fanOut(List<Function<T, R>> branches, Function<List<R>, C> join) {
        return retyped(delegate.fanOut(branches, join));
    }

    @Override
    public <R, C> ReactiveLane<C> fanOut(String name, List<Function<T, R>> branches,
                                         Function<List<R>, C> join) {
        return retyped(delegate.fanOut(name, branches, join));
    }

    @Override
    public <R, C> ReactiveLane<C> fanOutAsync(List<Function<T, CompletionStage<R>>> branches,
                                              Function<List<R>, C> join) {
        return retyped(delegate.fanOutAsync(branches, join));
    }

    @Override
    public <R, C> ReactiveLane<C> fanOutAsync(String name, List<Function<T, CompletionStage<R>>> branches,
                                              Function<List<R>, C> join) {
        return retyped(delegate.fanOutAsync(name, branches, join));
    }

    @Override
    public <R> ReactiveLane<R> batch(int size, Duration window, Function<List<T>, List<R>> bulk) {
        return retyped(delegate.batch(size, window, bulk));
    }

    @Override
    public <R> ReactiveLane<R> batch(String name, int size, Duration window, Function<List<T>, List<R>> bulk) {
        return retyped(delegate.batch(name, size, window, bulk));
    }

    @Override
    public <R> ReactiveLane<R> use(Segment<T, R> segment) {
        return retyped(delegate.use(Lanes.budgeted(segment, budget, preferAsync)));
    }

    @Override
    public <R> ReactiveLane<R> use(String region, Segment<T, R> segment) {
        return retyped(delegate.use(region, Lanes.budgeted(segment, budget, preferAsync)));
    }

    @Override
    public <R> ReactiveLane<T> fork(Segment<T, R> sub) {
        return wrap(delegate.fork(Lanes.budgeted(sub, budget, preferAsync)));
    }

    @Override
    public <R> ReactiveLane<T> fork(String name, Segment<T, R> sub) {
        return wrap(delegate.fork(name, Lanes.budgeted(sub, budget, preferAsync)));
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
        return new DefaultReactiveLaneCondition<>(delegate.when(predicate), budget, preferAsync);
    }

    @Override
    public ReactiveLaneCases<T> match() {
        return new DefaultReactiveLaneCases<>(delegate.match(), budget, preferAsync);
    }
}
