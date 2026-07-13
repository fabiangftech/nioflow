package dev.nioflow.application.facade;

import dev.nioflow.core.facade.Cases;
import dev.nioflow.core.facade.Condition;
import dev.nioflow.core.facade.Context;
import dev.nioflow.core.facade.NioFlow;
import dev.nioflow.core.facade.NioStep;
import dev.nioflow.core.facade.Segment;
import dev.nioflow.core.model.RateLimit;
import dev.nioflow.core.model.Retry;

import java.time.Duration;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

/**
 * Chaining after a fork returns to the MAIN LINE (without the fork's guards),
 * so Branch and Cases behave like the flow they came from. This is the plumbing
 * that lets them do that: everything delegates to the flow, and each fork type
 * only adds its own step (otherwise / is).
 */
abstract class NioFlowDelegate<I, O> implements NioFlow<I, O> {

    abstract DefaultNioFlow<I, O> flow();

    @Override
    public NioStep<I, O> just(I input) {
        return flow().just(input);
    }

    @Override
    public NioFlow<I, O> justAll(Iterable<I> inputs) {
        return flow().justAll(inputs);
    }

    @Override
    public NioFlow<I, O> handle(UnaryOperator<I> function) {
        return flow().handle(function);
    }

    @Override
    public NioFlow<I, O> handle(String name, UnaryOperator<I> function) {
        return flow().handle(name, function);
    }

    @Override
    public NioFlow<I, O> handle(String name, UnaryOperator<I> function, Duration timeout) {
        return flow().handle(name, function, timeout);
    }

    @Override
    public NioFlow<I, O> handle(String name, UnaryOperator<I> function, Retry retry) {
        return flow().handle(name, function, retry);
    }

    @Override
    public NioFlow<I, O> handle(String name, UnaryOperator<I> function, Duration timeout, Retry retry) {
        return flow().handle(name, function, timeout, retry);
    }

    @Override
    public NioFlow<I, O> handle(String name, UnaryOperator<I> function, RateLimit rateLimit) {
        return flow().handle(name, function, rateLimit);
    }

    @Override
    public NioFlow<I, O> handleContextual(BiFunction<I, Context, I> function) {
        return flow().handleContextual(function);
    }

    @Override
    public NioFlow<I, O> handleContextual(String name, BiFunction<I, Context, I> function) {
        return flow().handleContextual(name, function);
    }

    @Override
    public NioFlow<I, O> handleSync(UnaryOperator<I> function) {
        return flow().handleSync(function);
    }

    @Override
    public NioFlow<I, O> handleSync(String name, UnaryOperator<I> function) {
        return flow().handleSync(name, function);
    }

    @Override
    public NioFlow<I, O> background(Consumer<I> effect) {
        return flow().background(effect);
    }

    @Override
    public NioFlow<I, O> background(String name, Consumer<I> effect) {
        return flow().background(name, effect);
    }

    @Override
    public NioFlow<I, O> filter(Predicate<I> predicate) {
        return flow().filter(predicate);
    }

    @Override
    public <R> NioFlow<I, O> fanOut(List<Function<I, R>> branches, Function<List<R>, I> join) {
        return flow().fanOut(branches, join);
    }

    @Override
    public <R> NioFlow<I, O> fanOut(String name, List<Function<I, R>> branches, Function<List<R>, I> join) {
        return flow().fanOut(name, branches, join);
    }

    @Override
    public NioFlow<I, O> batch(int size, Duration window, UnaryOperator<List<I>> bulk) {
        return flow().batch(size, window, bulk);
    }

    @Override
    public NioFlow<I, O> batch(String name, int size, Duration window, UnaryOperator<List<I>> bulk) {
        return flow().batch(name, size, window, bulk);
    }

    @Override
    public NioFlow<I, O> use(Segment<I, I> segment) {
        return flow().use(segment);
    }

    @Override
    public NioFlow<I, O> use(String region, Segment<I, I> segment) {
        return flow().use(region, segment);
    }

    @Override
    public NioFlow<I, O> recover(Function<Throwable, I> function) {
        return flow().recover(function);
    }

    @Override
    public NioFlow<I, O> recover(String name, Function<Throwable, I> function) {
        return flow().recover(name, function);
    }

    @Override
    public NioFlow<I, O> onComplete(Consumer<O> callback) {
        return flow().onComplete(callback);
    }

    @Override
    public NioFlow<I, O> onError(Consumer<Throwable> callback) {
        return flow().onError(callback);
    }

    @Override
    public Condition<I, O> when(Predicate<I> predicate) {
        return flow().when(predicate);
    }

    @Override
    public Cases<I, O> match() {
        return flow().match();
    }
}
