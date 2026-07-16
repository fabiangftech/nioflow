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

    // The step label adaptMono/adaptMonoAsync report under (they carry no name).
    private static final String ADAPT_MONO = "adaptMono";

    final Lane<T> delegate;
    // The flow's config, handed down by the branch or segment that opened this
    // lane: its default budget, its preferAsync choice and its requireBudget
    // guard all reach the reactive steps declared inside a branch or fork.
    final ReactiveConfig config;

    DefaultReactiveLane(Lane<T> delegate) {
        this(delegate, ReactiveConfig.NONE);
    }

    DefaultReactiveLane(Lane<T> delegate, ReactiveConfig config) {
        this.delegate = delegate;
        this.config = config;
    }

    private ReactiveLane<T> wrap(Lane<T> result) {
        return result == delegate ? this : new DefaultReactiveLane<>(result, config);
    }

    private <R> ReactiveLane<R> retyped(Lane<R> result) {
        return new DefaultReactiveLane<>(result, config);
    }

    // ── the reactive steps ──

    @Override
    public ReactiveLane<T> handleMono(String name, Function<T, Mono<T>> call) {
        return handleMono(name, call, config.budget());
    }

    @Override
    public ReactiveLane<T> handleMono(String name, Function<T, Mono<T>> call, Duration budget) {
        Duration effective = config.budgetFor(name, budget);
        if (config.preferAsync()) {
            return handleMonoAsync(name, call, effective);
        }
        return wrap(delegate.handle(name, value -> Blocking.awaitValue(call.apply(value), effective, name)));
    }

    @Override
    public ReactiveLane<T> handleMono(String name, Function<T, Mono<T>> call, Retry retry) {
        return handleMono(name, call, config.budget(), retry);
    }

    @Override
    public ReactiveLane<T> handleMono(String name, Function<T, Mono<T>> call, Duration budget, Retry retry) {
        Duration effective = config.budgetFor(name, budget);
        if (config.preferAsync()) {
            return wrap(delegate.handleAsync(name, value -> Blocking.requiredFuture(call.apply(value), name), effective, retry));
        }
        return wrap(delegate.handle(name,
                value -> Blocking.awaitValue(call.apply(value), effective, name), retry));
    }

    @Override
    public ReactiveLane<T> handleMonoAsync(String name, Function<T, Mono<T>> call) {
        return handleMonoAsync(name, call, config.budget());
    }

    @Override
    public ReactiveLane<T> handleMonoAsync(String name, Function<T, Mono<T>> call, Duration budget) {
        return wrap(delegate.handleAsync(name, value -> Blocking.requiredFuture(call.apply(value), name), config.budgetFor(name, budget)));
    }

    @Override
    public <R> ReactiveLane<R> adaptMonoAsync(Function<T, Mono<R>> call) {
        return adaptMonoAsync(call, config.budget());
    }

    @Override
    public <R> ReactiveLane<R> adaptMonoAsync(Function<T, Mono<R>> call, Duration budget) {
        return retyped(delegate.adaptAsync(value -> Blocking.requiredFuture(call.apply(value), ADAPT_MONO),
                config.budgetFor("adaptMonoAsync", budget)));
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
        return adaptMono(call, config.budget());
    }

    @Override
    public <R> ReactiveLane<R> adaptMono(Function<T, Mono<R>> call, Duration budget) {
        Duration effective = config.budgetFor(ADAPT_MONO, budget);
        if (config.preferAsync()) {
            return adaptMonoAsync(call, effective);
        }
        return retyped(delegate.adapt(value -> Blocking.awaitValue(call.apply(value), effective, ADAPT_MONO)));
    }

    /** @deprecated see {@link ReactiveLane#adaptFlux(Function)} — prefer the bounded overload. */
    @Override
    @Deprecated(forRemoval = false)
    public <R> ReactiveLane<List<R>> adaptFlux(Function<T, Flux<R>> call) {
        Duration effective = config.budgetFor("adaptFlux", null);
        return retyped(delegate.adapt(
                value -> Blocking.await(Blocking.budgeted(call.apply(value).collectList(), effective))));
    }

    @Override
    public <R> ReactiveLane<List<R>> adaptFlux(Function<T, Flux<R>> call, int maxItems) {
        Blocking.checkMaxItems(maxItems);
        Duration effective = config.budgetFor("adaptFlux", null);
        return retyped(delegate.adapt(value -> Blocking.awaitBounded(call.apply(value), maxItems, effective)));
    }

    @Override
    public <R, C> ReactiveLane<C> fanOutMono(String name, List<Function<T, Mono<R>>> branches,
                                             Function<List<R>, C> join) {
        return retyped(delegate.fanOutAsync(name,
                Blocking.asyncBranches(branches, config.budgetFor(name, null)), join));
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
        return retyped(delegate.use(Lanes.budgeted(segment, config)));
    }

    @Override
    public <R> ReactiveLane<R> use(String region, Segment<T, R> segment) {
        return retyped(delegate.use(region, Lanes.budgeted(segment, config)));
    }

    @Override
    public <R> ReactiveLane<T> fork(Segment<T, R> sub) {
        return wrap(delegate.fork(Lanes.budgeted(sub, config)));
    }

    @Override
    public <R> ReactiveLane<T> fork(String name, Segment<T, R> sub) {
        return wrap(delegate.fork(name, Lanes.budgeted(sub, config)));
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
        return new DefaultReactiveLaneCondition<>(delegate.when(predicate), config);
    }

    @Override
    public ReactiveLaneCases<T> match() {
        return new DefaultReactiveLaneCases<>(delegate.match(), config);
    }
}
