package dev.nioflow.application.facade;

import dev.nioflow.core.facade.Branch;
import dev.nioflow.core.facade.Cases;
import dev.nioflow.core.facade.Condition;
import dev.nioflow.core.facade.NioFlow;
import dev.nioflow.core.model.Guard;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

/**
 * Rama abierta por when().then(): otherwise() declara el lane contrario y el
 * resto de la API delega en el flow original — encadenar después de un fork
 * vuelve a la línea principal (sin guards del fork).
 */
final class DefaultBranch<I, T> implements Branch<I, T> {

    private final AbstractNioFlow<I, T> flow;
    private final int decision;

    DefaultBranch(AbstractNioFlow<I, T> flow, int decision) {
        this.flow = flow;
        this.decision = decision;
    }

    @Override
    public NioFlow<I, T> otherwise(UnaryOperator<NioFlow<I, T>> lane) {
        lane.apply(flow.withGuards(AbstractNioFlow.withGuard(flow.guards(), new Guard(decision, false))));
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
    public void close() throws Exception {
        flow.close();
    }
}
