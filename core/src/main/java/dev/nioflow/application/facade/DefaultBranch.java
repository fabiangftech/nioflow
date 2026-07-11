package dev.nioflow.application.facade;

import dev.nioflow.core.facade.Branch;
import dev.nioflow.core.facade.Cases;
import dev.nioflow.core.facade.Condition;
import dev.nioflow.core.facade.Lane;
import dev.nioflow.core.facade.NioFlow;
import dev.nioflow.core.model.Guard;

import java.util.concurrent.CompletableFuture;
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
    public NioFlow<I, T> filter(Predicate<T> predicate) {
        return flow.filter(predicate);
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
