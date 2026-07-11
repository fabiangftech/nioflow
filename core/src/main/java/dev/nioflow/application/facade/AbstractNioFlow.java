package dev.nioflow.application.facade;

import dev.nioflow.core.facade.Cases;
import dev.nioflow.core.facade.Condition;
import dev.nioflow.core.facade.NioEngine;
import dev.nioflow.core.facade.NioFlow;
import dev.nioflow.core.model.Background;
import dev.nioflow.core.model.Decision;
import dev.nioflow.core.model.Filter;
import dev.nioflow.core.model.Guard;
import dev.nioflow.core.model.Link;
import dev.nioflow.core.model.Recovery;
import dev.nioflow.core.model.Stage;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Shared chain-building logic for the shared definition (DefaultNioFlow) and
 * per-request executions (ExecutionNioFlow). Forks (when/match) work through
 * views: withGuards() returns the same flow with active Guards, and lanes wrap
 * those views behind the restricted Lane interface, so every link declared
 * inside a lane is conditioned on its Decision and nested forks compose
 * guards automatically.
 */
abstract class AbstractNioFlow<I, T> implements NioFlow<I, T> {

    abstract NioEngine engine();

    abstract void appendLink(Link link);

    abstract List<Guard> guards();

    abstract AbstractNioFlow<I, T> withGuards(List<Guard> guards);

    abstract String anonymousName(String prefix);

    @Override
    public NioFlow<I, T> handle(Function<T, T> function) {
        return handle(anonymousName("stage"), function);
    }

    @Override
    public NioFlow<I, T> handle(String name, Function<T, T> function) {
        appendLink(new Stage(name, asObjectFunction(function), false, null, guards()));
        return this;
    }

    @Override
    public NioFlow<I, T> background(Consumer<T> effect) {
        return background(anonymousName("background"), effect);
    }

    @Override
    @SuppressWarnings("unchecked")
    public NioFlow<I, T> background(String name, Consumer<T> effect) {
        appendLink(new Background(name, (Consumer<Object>) effect, guards()));
        return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <R> NioFlow<I, R> adapt(Function<T, R> function) {
        appendLink(new Stage(anonymousName("adapt"), asObjectFunction(function), false, null, guards()));
        return (NioFlow<I, R>) this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public NioFlow<I, T> filter(Predicate<T> predicate) {
        appendLink(new Filter((Predicate<Object>) predicate, guards()));
        return this;
    }

    @Override
    public NioFlow<I, T> recover(Function<Throwable, T> function) {
        return recover(anonymousName("recovery"), function);
    }

    @Override
    @SuppressWarnings("unchecked")
    public NioFlow<I, T> recover(String name, Function<Throwable, T> function) {
        appendLink(new Recovery(name, (Function<Throwable, Object>) function, guards()));
        return this;
    }

    @Override
    public Condition<I, T> when(Predicate<T> predicate) {
        return new DefaultCondition<>(this, appendDecision(predicate));
    }

    @Override
    public Cases<I, T> match() {
        return new DefaultCases<>(this);
    }

    @SuppressWarnings("unchecked")
    int appendDecision(Predicate<T> predicate) {
        int decision = engine().nextDecision();
        appendLink(new Decision((Predicate<Object>) predicate, decision, guards()));
        return decision;
    }

    static List<Guard> withGuard(List<Guard> guards, Guard extra) {
        List<Guard> next = new ArrayList<>(guards);
        next.add(extra);
        return List.copyOf(next);
    }

    static List<Guard> withGuards(List<Guard> guards, List<Guard> extras) {
        List<Guard> next = new ArrayList<>(guards);
        next.addAll(extras);
        return List.copyOf(next);
    }

    @SuppressWarnings("unchecked")
    static Function<Object, Object> asObjectFunction(Function<?, ?> function) {
        return (Function<Object, Object>) function;
    }
}
