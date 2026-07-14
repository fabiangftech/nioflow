package dev.nioflow.application.facade;

import dev.nioflow.core.facade.Context;
import dev.nioflow.core.facade.FlowResult;
import dev.nioflow.core.facade.NioStep;
import dev.nioflow.core.facade.Segment;
import dev.nioflow.core.facade.StepCases;
import dev.nioflow.core.facade.StepCondition;
import dev.nioflow.core.model.RateLimit;
import dev.nioflow.core.model.Retry;

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
 * Same idea as NioFlowDelegate, for the per-request side: chaining after a
 * fork returns to the main line of the execution.
 */
abstract class NioStepDelegate<T, O> implements NioStep<T, O> {

    abstract ExecutionNioFlow<T, O> step();

    @Override
    public NioStep<T, O> handle(UnaryOperator<T> function) {
        return step().handle(function);
    }

    @Override
    public NioStep<T, O> handle(String name, UnaryOperator<T> function) {
        return step().handle(name, function);
    }

    @Override
    public NioStep<T, O> handle(String name, UnaryOperator<T> function, Duration timeout) {
        return step().handle(name, function, timeout);
    }

    @Override
    public NioStep<T, O> handle(String name, UnaryOperator<T> function, Retry retry) {
        return step().handle(name, function, retry);
    }

    @Override
    public NioStep<T, O> handle(String name, UnaryOperator<T> function, Duration timeout, Retry retry) {
        return step().handle(name, function, timeout, retry);
    }

    @Override
    public NioStep<T, O> handle(String name, UnaryOperator<T> function, RateLimit rateLimit) {
        return step().handle(name, function, rateLimit);
    }

    @Override
    public NioStep<T, O> handleAsync(String name, Function<T, CompletionStage<T>> call) {
        return step().handleAsync(name, call);
    }

    @Override
    public NioStep<T, O> handleAsync(String name, Function<T, CompletionStage<T>> call, Duration timeout) {
        return step().handleAsync(name, call, timeout);
    }

    @Override
    public NioStep<T, O> handleAsync(String name, Function<T, CompletionStage<T>> call, Retry retry) {
        return step().handleAsync(name, call, retry);
    }

    @Override
    public NioStep<T, O> handleAsync(String name, Function<T, CompletionStage<T>> call,
                                     Duration timeout, Retry retry) {
        return step().handleAsync(name, call, timeout, retry);
    }

    @Override
    public <R> NioStep<R, O> adaptAsync(Function<T, CompletionStage<R>> call) {
        return step().adaptAsync(call);
    }

    @Override
    public <R> NioStep<R, O> adaptAsync(Function<T, CompletionStage<R>> call, Duration timeout) {
        return step().adaptAsync(call, timeout);
    }

    @Override
    public NioStep<T, O> handleContextual(BiFunction<T, Context, T> function) {
        return step().handleContextual(function);
    }

    @Override
    public NioStep<T, O> handleContextual(String name, BiFunction<T, Context, T> function) {
        return step().handleContextual(name, function);
    }

    @Override
    public NioStep<T, O> handleSync(UnaryOperator<T> function) {
        return step().handleSync(function);
    }

    @Override
    public NioStep<T, O> handleSync(String name, UnaryOperator<T> function) {
        return step().handleSync(name, function);
    }

    @Override
    public NioStep<T, O> background(Consumer<T> effect) {
        return step().background(effect);
    }

    @Override
    public NioStep<T, O> background(String name, Consumer<T> effect) {
        return step().background(name, effect);
    }

    @Override
    public <R> NioStep<R, O> adapt(Function<T, R> function) {
        return step().adapt(function);
    }

    @Override
    public NioStep<T, O> filter(Predicate<T> predicate) {
        return step().filter(predicate);
    }

    @Override
    public <R, C> NioStep<C, O> fanOut(List<Function<T, R>> branches, Function<List<R>, C> join) {
        return step().fanOut(branches, join);
    }

    @Override
    public <R, C> NioStep<C, O> fanOut(String name, List<Function<T, R>> branches, Function<List<R>, C> join) {
        return step().fanOut(name, branches, join);
    }

    @Override
    public <R> NioStep<R, O> batch(int size, Duration window, Function<List<T>, List<R>> bulk) {
        return step().batch(size, window, bulk);
    }

    @Override
    public <R> NioStep<R, O> batch(String name, int size, Duration window, Function<List<T>, List<R>> bulk) {
        return step().batch(name, size, window, bulk);
    }

    @Override
    public <R> NioStep<R, O> use(Segment<T, R> segment) {
        return step().use(segment);
    }

    @Override
    public <R> NioStep<T, O> fork(Segment<T, R> sub) {
        return step().fork(sub);
    }

    @Override
    public <R> NioStep<T, O> fork(String name, Segment<T, R> sub) {
        return step().fork(name, sub);
    }

    @Override
    public NioStep<T, O> recover(Function<Throwable, T> function) {
        return step().recover(function);
    }

    @Override
    public NioStep<T, O> recover(String name, Function<Throwable, T> function) {
        return step().recover(name, function);
    }

    @Override
    public NioStep<T, O> onComplete(Consumer<T> callback) {
        return step().onComplete(callback);
    }

    @Override
    public NioStep<T, O> onError(Consumer<Throwable> callback) {
        return step().onError(callback);
    }

    @Override
    public NioStep<T, O> key(Object key) {
        return step().key(key);
    }

    @Override
    public <V> NioStep<T, O> with(Context.Key<V> key, V value) {
        return step().with(key, value);
    }

    @Override
    public StepCondition<T, O> when(Predicate<T> predicate) {
        return step().when(predicate);
    }

    @Override
    public StepCases<T, O> match() {
        return step().match();
    }

    @Override
    public T execute() {
        return step().execute();
    }

    @Override
    public CompletableFuture<T> executeAsync() {
        return step().executeAsync();
    }

    @Override
    public CompletableFuture<T> executeAsync(Map<String, Object> context) {
        return step().executeAsync(context);
    }

    @Override
    public FlowResult<T> executeResult() {
        return step().executeResult();
    }
}
