package dev.nioflow.application.facade;

import dev.nioflow.core.facade.Cases;
import dev.nioflow.core.facade.Condition;
import dev.nioflow.core.facade.Lane;
import dev.nioflow.core.facade.NioFlow;
import dev.nioflow.core.facade.Segment;
import dev.nioflow.core.model.Guard;

import java.util.ArrayList;
import java.util.List;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

/**
 * match() with first-match-wins semantics: case k's Decision only evaluates
 * if previous cases were false (it carries their guards in false), its lane
 * additionally requires its own decision true, and otherwise() requires all
 * false. Chaining after (with or without otherwise) returns to the main line.
 */
final class DefaultCases<I, T> implements Cases<I, T> {

    private final AbstractNioFlow<I, T> flow;
    private final List<Guard> priorCasesFalse = new ArrayList<>();

    DefaultCases(AbstractNioFlow<I, T> flow) {
        this.flow = flow;
    }

    @Override
    public Cases<I, T> is(Predicate<T> predicate, UnaryOperator<Lane<T>> lane) {
        AbstractNioFlow<I, T> evaluation = flow.withGuards(
                AbstractNioFlow.withGuards(flow.guards(), priorCasesFalse));
        int decision = evaluation.appendDecision(predicate);
        lane.apply(new DefaultLane<>(evaluation.withGuards(
                AbstractNioFlow.withGuard(evaluation.guards(), new Guard(decision, true)))));
        priorCasesFalse.add(new Guard(decision, false));
        return this;
    }

    @Override
    public NioFlow<I, T> otherwise(UnaryOperator<Lane<T>> lane) {
        lane.apply(new DefaultLane<>(flow.withGuards(
                AbstractNioFlow.withGuards(flow.guards(), priorCasesFalse))));
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
    public <R, C> NioFlow<I, C> fanOut(List<Function<T, R>> branches, Function<List<R>, C> join) {
        return flow.fanOut(branches, join);
    }

    @Override
    public <R, C> NioFlow<I, C> fanOut(String name, List<Function<T, R>> branches, Function<List<R>, C> join) {
        return flow.fanOut(name, branches, join);
    }

    @Override
    public NioFlow<I, T> filter(Predicate<T> predicate) {
        return flow.filter(predicate);
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
}
