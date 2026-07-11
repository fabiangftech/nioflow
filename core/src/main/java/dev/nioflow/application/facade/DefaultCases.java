package dev.nioflow.application.facade;

import dev.nioflow.core.facade.Cases;
import dev.nioflow.core.facade.Condition;
import dev.nioflow.core.facade.NioFlow;
import dev.nioflow.core.model.Decision;
import dev.nioflow.core.model.Guard;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

/**
 * match() con semántica first-match-wins: el Decision del caso k solo se
 * evalúa si los anteriores fueron false (lleva sus guards en false), su lane
 * exige además su propio decision en true, y otherwise() exige todos en false.
 * Encadenar después (con o sin otherwise) vuelve a la línea principal.
 */
final class DefaultCases<I, T> implements Cases<I, T> {

    private final AbstractNioFlow<I, T> flow;
    private final List<Guard> priorCasesFalse = new ArrayList<>();

    DefaultCases(AbstractNioFlow<I, T> flow) {
        this.flow = flow;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Cases<I, T> is(Predicate<T> predicate, UnaryOperator<NioFlow<I, T>> lane) {
        int decision = flow.engine().nextDecision();
        List<Guard> evaluationGuards = AbstractNioFlow.withGuards(flow.guards(), priorCasesFalse);
        flow.appendLink(new Decision((Predicate<Object>) predicate, decision, evaluationGuards));
        lane.apply(flow.withGuards(AbstractNioFlow.withGuard(evaluationGuards, new Guard(decision, true))));
        priorCasesFalse.add(new Guard(decision, false));
        return this;
    }

    @Override
    public NioFlow<I, T> otherwise(UnaryOperator<NioFlow<I, T>> lane) {
        lane.apply(flow.withGuards(AbstractNioFlow.withGuards(flow.guards(), priorCasesFalse)));
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
