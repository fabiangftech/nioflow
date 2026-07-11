package dev.nioflow.application.facade;

import dev.nioflow.core.facade.Lane;
import dev.nioflow.core.facade.LaneCases;
import dev.nioflow.core.facade.LaneCondition;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Lane implementation: wraps a guarded view of the flow and exposes only
 * step-building operations. Every link declared here inherits the lane's
 * guards; nested when/match compose guards automatically.
 */
class DefaultLane<T> implements Lane<T> {

    final AbstractNioFlow<?, T> view;

    DefaultLane(AbstractNioFlow<?, T> view) {
        this.view = view;
    }

    @Override
    public Lane<T> handle(Function<T, T> function) {
        view.handle(function);
        return this;
    }

    @Override
    public Lane<T> handle(String name, Function<T, T> function) {
        view.handle(name, function);
        return this;
    }

    @Override
    public Lane<T> background(Consumer<T> effect) {
        view.background(effect);
        return this;
    }

    @Override
    public Lane<T> background(String name, Consumer<T> effect) {
        view.background(name, effect);
        return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <R> Lane<R> adapt(Function<T, R> function) {
        view.adapt(function);
        return (Lane<R>) this;
    }

    @Override
    public Lane<T> filter(Predicate<T> predicate) {
        view.filter(predicate);
        return this;
    }

    @Override
    public Lane<T> recover(Function<Throwable, T> function) {
        view.recover(function);
        return this;
    }

    @Override
    public Lane<T> recover(String name, Function<Throwable, T> function) {
        view.recover(name, function);
        return this;
    }

    @Override
    public LaneCondition<T> when(Predicate<T> predicate) {
        return new DefaultLaneCondition<>(view, view.appendDecision(predicate));
    }

    @Override
    public LaneCases<T> match() {
        return new DefaultLaneCases<>(view);
    }
}
