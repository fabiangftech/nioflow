package dev.nioflow.application.facade;

import dev.nioflow.core.facade.Context;
import dev.nioflow.core.facade.Lane;
import dev.nioflow.core.facade.LaneCases;
import dev.nioflow.core.facade.LaneCondition;
import dev.nioflow.core.facade.Segment;
import dev.nioflow.core.model.RateLimit;
import dev.nioflow.core.model.Retry;

import java.time.Duration;
import java.util.List;
import java.util.function.BiFunction;
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
    public Lane<T> handle(String name, Function<T, T> function, Duration timeout) {
        view.handle(name, function, timeout);
        return this;
    }

    @Override
    public Lane<T> handle(String name, Function<T, T> function, Retry retry) {
        view.handle(name, function, retry);
        return this;
    }

    @Override
    public Lane<T> handle(String name, Function<T, T> function, Duration timeout, Retry retry) {
        view.handle(name, function, timeout, retry);
        return this;
    }

    @Override
    public Lane<T> handle(String name, Function<T, T> function, RateLimit rateLimit) {
        view.handle(name, function, rateLimit);
        return this;
    }

    @Override
    public Lane<T> handleContextual(BiFunction<T, Context, T> function) {
        view.handleContextual(function);
        return this;
    }

    @Override
    public Lane<T> handleContextual(String name, BiFunction<T, Context, T> function) {
        view.handleContextual(name, function);
        return this;
    }

    @Override
    public Lane<T> handleSync(Function<T, T> function) {
        view.handleSync(function);
        return this;
    }

    @Override
    public Lane<T> handleSync(String name, Function<T, T> function) {
        view.handleSync(name, function);
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
    @SuppressWarnings("unchecked")
    public <R, C> Lane<C> fanOut(List<Function<T, R>> branches, Function<List<R>, C> join) {
        view.fanOut(branches, join);
        return (Lane<C>) this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <R, C> Lane<C> fanOut(String name, List<Function<T, R>> branches, Function<List<R>, C> join) {
        view.fanOut(name, branches, join);
        return (Lane<C>) this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <R> Lane<R> batch(int size, Duration window, Function<List<T>, List<R>> bulk) {
        view.batch(size, window, bulk);
        return (Lane<R>) this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <R> Lane<R> batch(String name, int size, Duration window, Function<List<T>, List<R>> bulk) {
        view.batch(name, size, window, bulk);
        return (Lane<R>) this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <R> Lane<R> use(Segment<T, R> segment) {
        segment.define(new DefaultLane<>(view));
        return (Lane<R>) this;
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
