package dev.nioflow.application.facade;

import dev.nioflow.core.facade.Branch;
import dev.nioflow.core.facade.Cases;
import dev.nioflow.core.facade.Context;
import dev.nioflow.core.facade.FlowResult;
import dev.nioflow.core.facade.Condition;
import dev.nioflow.core.facade.Lane;
import dev.nioflow.core.facade.NioFlow;
import dev.nioflow.core.facade.Segment;
import dev.nioflow.core.model.Guard;
import dev.nioflow.core.model.RateLimit;
import dev.nioflow.core.model.Retry;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

/**
 * Branch opened by when().then(): otherwise() declares the opposite lane and
 * the rest of the API delegates to the original flow — chaining after a fork
 * returns to the main line (without the fork's guards).
 */
final class DefaultBranch<I, T> implements Branch<I, T> {

    private final AbstractNioFlow<I, T> flow;
    private final int decision;

    DefaultBranch(AbstractNioFlow<I, T> flow, int decision) {
        this.flow = flow;
        this.decision = decision;
    }

    @Override
    public NioFlow<I, T> otherwise(UnaryOperator<Lane<T>> lane) {
        lane.apply(new DefaultLane<>(flow.withGuards(
                AbstractNioFlow.withGuard(flow.guards(), new Guard(decision, false)))));
        return flow;
    }

    @Override
    public NioFlow<I, T> just(I input) {
        return flow.just(input);
    }

    @Override
    public NioFlow<I, T> justAll(Iterable<I> inputs) {
        return flow.justAll(inputs);
    }

    @Override
    public NioFlow<I, T> handle(Function<T, T> function) {
        return flow.handle(function);
    }

    @Override
    public NioFlow<I, T> handle(String name, Function<T, T> function) {
        return flow.handle(name, function);
    }

    @Override
    public NioFlow<I, T> handle(String name, Function<T, T> function, Duration timeout) {
        return flow.handle(name, function, timeout);
    }

    @Override
    public NioFlow<I, T> handle(String name, Function<T, T> function, Retry retry) {
        return flow.handle(name, function, retry);
    }

    @Override
    public NioFlow<I, T> handle(String name, Function<T, T> function, Duration timeout, Retry retry) {
        return flow.handle(name, function, timeout, retry);
    }

    @Override
    public NioFlow<I, T> handle(String name, Function<T, T> function, RateLimit rateLimit) {
        return flow.handle(name, function, rateLimit);
    }

    @Override
    public NioFlow<I, T> handleContextual(BiFunction<T, Context, T> function) {
        return flow.handleContextual(function);
    }

    @Override
    public NioFlow<I, T> handleContextual(String name, BiFunction<T, Context, T> function) {
        return flow.handleContextual(name, function);
    }

    @Override
    public NioFlow<I, T> onComplete(Consumer<T> callback) {
        return flow.onComplete(callback);
    }

    @Override
    public NioFlow<I, T> onError(Consumer<Throwable> callback) {
        return flow.onError(callback);
    }

    @Override
    public NioFlow<I, T> handleSync(Function<T, T> function) {
        return flow.handleSync(function);
    }

    @Override
    public NioFlow<I, T> handleSync(String name, Function<T, T> function) {
        return flow.handleSync(name, function);
    }

    @Override
    public NioFlow<I, T> background(Consumer<T> effect) {
        return flow.background(effect);
    }

    @Override
    public NioFlow<I, T> background(String name, Consumer<T> effect) {
        return flow.background(name, effect);
    }

    @Override
    public <R> NioFlow<I, R> adapt(Function<T, R> function) {
        return flow.adapt(function);
    }

    @Override
    public <R, C> NioFlow<I, C> fanOut(List<Function<T, R>> branches,
                                       Function<List<R>, C> join) {
        return flow.fanOut(branches, join);
    }

    @Override
    public <R, C> NioFlow<I, C> fanOut(String name, List<Function<T, R>> branches,
                                       Function<List<R>, C> join) {
        return flow.fanOut(name, branches, join);
    }

    @Override
    public NioFlow<I, T> filter(Predicate<T> predicate) {
        return flow.filter(predicate);
    }

    @Override
    public <R> NioFlow<I, R> batch(int size, Duration window, Function<List<T>, List<R>> bulk) {
        return flow.batch(size, window, bulk);
    }

    @Override
    public <R> NioFlow<I, R> batch(String name, int size, Duration window, Function<List<T>, List<R>> bulk) {
        return flow.batch(name, size, window, bulk);
    }

    @Override
    public <R> NioFlow<I, R> use(Segment<T, R> segment) {
        return flow.use(segment);
    }

    @Override
    public NioFlow<I, T> recover(Function<Throwable, T> function) {
        return flow.recover(function);
    }

    @Override
    public NioFlow<I, T> recover(String name, Function<Throwable, T> function) {
        return flow.recover(name, function);
    }

    @Override
    public Condition<I, T> when(Predicate<T> predicate) {
        return flow.when(predicate);
    }

    @Override
    public Cases<I, T> match() {
        return flow.match();
    }

    @Override
    public T execute() {
        return flow.execute();
    }

    @Override
    public CompletableFuture<T> executeAsync() {
        return flow.executeAsync();
    }

    @Override
    public FlowResult<T> executeResult() {
        return flow.executeResult();
    }
}
