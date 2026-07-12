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
 * Lane implementation: wraps a guarded view of the chain and exposes only
 * step-building operations. Every link declared here inherits the lane's
 * guards; nested when/match compose guards automatically. Lanes work the same
 * on the shared definition and inside a per-request pipeline — both are just
 * chains over the value's current type.
 */
class DefaultLane<T> implements Lane<T> {

    final AbstractChain<T> view;

    DefaultLane(AbstractChain<T> view) {
        this.view = view;
    }

    @Override
    public Lane<T> handle(Function<T, T> function) {
        return handle(view.anonymousName("stage"), function);
    }

    @Override
    public Lane<T> handle(String name, Function<T, T> function) {
        view.stage(name, function);
        return this;
    }

    @Override
    public Lane<T> handle(String name, Function<T, T> function, Duration timeout) {
        view.stage(name, function, timeout);
        return this;
    }

    @Override
    public Lane<T> handle(String name, Function<T, T> function, Retry retry) {
        view.stage(name, function, retry);
        return this;
    }

    @Override
    public Lane<T> handle(String name, Function<T, T> function, Duration timeout, Retry retry) {
        view.stage(name, function, timeout, retry);
        return this;
    }

    @Override
    public Lane<T> handle(String name, Function<T, T> function, RateLimit rateLimit) {
        view.rateLimitedStage(name, function, rateLimit);
        return this;
    }

    @Override
    public Lane<T> handleContextual(BiFunction<T, Context, T> function) {
        return handleContextual(view.anonymousName("stage"), function);
    }

    @Override
    public Lane<T> handleContextual(String name, BiFunction<T, Context, T> function) {
        view.contextualStage(name, function);
        return this;
    }

    @Override
    public Lane<T> handleSync(Function<T, T> function) {
        return handleSync(view.anonymousName("sync"), function);
    }

    @Override
    public Lane<T> handleSync(String name, Function<T, T> function) {
        view.syncStage(name, function);
        return this;
    }

    @Override
    public Lane<T> background(Consumer<T> effect) {
        return background(view.anonymousName("background"), effect);
    }

    @Override
    public Lane<T> background(String name, Consumer<T> effect) {
        view.backgroundEffect(name, effect);
        return this;
    }

    @Override
    public <R> Lane<R> adapt(Function<T, R> function) {
        view.adaptValue(function);
        return retyped();
    }

    @Override
    public Lane<T> filter(Predicate<T> predicate) {
        view.filterValues(predicate);
        return this;
    }

    @Override
    public <R, C> Lane<C> fanOut(List<Function<T, R>> branches, Function<List<R>, C> join) {
        return fanOut(view.anonymousName("fanout"), branches, join);
    }

    @Override
    public <R, C> Lane<C> fanOut(String name, List<Function<T, R>> branches, Function<List<R>, C> join) {
        view.fanOutBranches(name, branches, join);
        return retyped();
    }

    @Override
    public <R> Lane<R> batch(int size, Duration window, Function<List<T>, List<R>> bulk) {
        return batch(view.anonymousName("batch"), size, window, bulk);
    }

    @Override
    public <R> Lane<R> batch(String name, int size, Duration window, Function<List<T>, List<R>> bulk) {
        view.batchValues(name, size, window, bulk);
        return retyped();
    }

    @Override
    public <R> Lane<R> use(Segment<T, R> segment) {
        view.embed(segment);
        return retyped();
    }

    @Override
    public <R> Lane<R> use(String region, Segment<T, R> segment) {
        view.embed(region, segment);
        return retyped();
    }

    // The chain keeps moving Objects: only the static type changes.
    @SuppressWarnings("unchecked")
    private <R> Lane<R> retyped() {
        return (Lane<R>) this;
    }

    @Override
    public Lane<T> recover(Function<Throwable, T> function) {
        return recover(view.anonymousName("recovery"), function);
    }

    @Override
    public Lane<T> recover(String name, Function<Throwable, T> function) {
        view.recovery(name, function);
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
