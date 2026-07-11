package dev.nioflow.application.facade;

import dev.nioflow.core.facade.NioFlow;
import dev.nioflow.core.facade.NioFlowMetrics;
import dev.nioflow.core.facade.NioFlowTracer;
import dev.nioflow.core.facade.Resilience;
import dev.nioflow.core.model.Diagnostics;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

/**
 * A {@code when} fork whose true lane is already built. {@link #otherwise} builds
 * the false lane; every other call falls through to the fork's parent view — the
 * main line — whose stages run for both lanes. That fall-through is what makes
 * {@code otherwise} optional: chaining any stage directly on the branch simply
 * resumes the main line, and false values skip the true lane unchanged.
 *
 * <p>All delegating overrides inherit their contract from
 * {@link NioFlow}; only the fork mechanics live here.
 *
 * @param <T> the type of the values flowing through the fork
 */
final class ForkBranch<T> implements NioFlow.Branch<T> {

    private final DefaultNioFlow<T> mainLine;
    private final int decision;

    /**
     * @param mainLine the view the fork was declared on, resumed after the lanes
     * @param decision the fork's decision id, guarding the links of both lanes
     */
    ForkBranch(DefaultNioFlow<T> mainLine, int decision) {
        this.mainLine = mainLine;
        this.decision = decision;
    }

    /**
     * Builds the false lane: the builder receives a view whose links are guarded by
     * this fork's decision being false, so they only run for values the predicate
     * rejected. Returns the main line — stages declared after this run for every
     * value again.
     */
    @Override
    public NioFlow<T> otherwise(UnaryOperator<NioFlow<T>> lane) {
        lane.apply(mainLine.lane(decision, false));
        return mainLine;
    }

    @Override
    public NioFlow<T> just(T input) {
        return mainLine.just(input);
    }

    @Override
    public NioFlow<T> just(T input, Map<String, Object> context) {
        return mainLine.just(input, context);
    }

    @Override
    public NioFlow<T> justAll(Iterable<T> inputs) {
        return mainLine.justAll(inputs);
    }

    @Override
    public <R> CompletableFuture<R> call(T input) {
        return mainLine.call(input);
    }

    @Override
    public <R> CompletableFuture<R> call(T input, Map<String, Object> context) {
        return mainLine.call(input, context);
    }

    @Override
    public <R> CompletableFuture<R> call(T input, Duration timeout) {
        return mainLine.call(input, timeout);
    }

    @Override
    public <R> CompletableFuture<R> call(T input, Map<String, Object> context, Duration timeout) {
        return mainLine.call(input, context, timeout);
    }

    @Override
    public NioFlow<T> filter(Predicate<T> predicate) {
        return mainLine.filter(predicate);
    }

    @Override
    public NioFlow<T> handle(Function<T, T> function) {
        return mainLine.handle(function);
    }

    @Override
    public NioFlow<T> handle(String name, Function<T, T> function) {
        return mainLine.handle(name, function);
    }

    @Override
    public NioFlow<T> handle(Function<T, T> function, Resilience<T> resilience) {
        return mainLine.handle(function, resilience);
    }

    @Override
    public NioFlow<T> submit(Function<T, T> function) {
        return mainLine.submit(function);
    }

    @Override
    public NioFlow<T> submit(String name, Function<T, T> function) {
        return mainLine.submit(name, function);
    }

    @Override
    public NioFlow<T> submit(Function<T, T> function, Duration timeout) {
        return mainLine.submit(function, timeout);
    }

    @Override
    public NioFlow<T> submit(Function<T, T> function, Resilience<T> resilience) {
        return mainLine.submit(function, resilience);
    }

    @Override
    public NioFlow<T> background(Consumer<T> effect) {
        return mainLine.background(effect);
    }

    @Override
    public NioFlow<T> background(String name, Consumer<T> effect) {
        return mainLine.background(name, effect);
    }

    @Override
    public NioFlow<T> batch(int size, Duration maxWait, Function<List<T>, List<T>> function) {
        return mainLine.batch(size, maxWait, function);
    }

    @Override
    public <N> NioFlow<N> adapt(Function<T, N> function) {
        return mainLine.adapt(function);
    }

    @Override
    public <N> NioFlow<N> fanOut(Function<T, List<N>> function) {
        return mainLine.fanOut(function);
    }

    @Override
    public Condition<T> when(Predicate<T> predicate) {
        return mainLine.when(predicate);
    }

    @Override
    public Cases<T> match() {
        return mainLine.match();
    }

    @Override
    public NioFlow<T> onError(Consumer<Throwable> handler) {
        return mainLine.onError(handler);
    }

    @Override
    public NioFlow<T> onComplete(Consumer<T> handler) {
        return mainLine.onComplete(handler);
    }

    @Override
    public NioFlow<T> onErrorResume(Function<Throwable, T> fallback) {
        return mainLine.onErrorResume(fallback);
    }

    @Override
    public NioFlow<T> metrics(NioFlowMetrics metrics) {
        return mainLine.metrics(metrics);
    }

    @Override
    public NioFlow<T> trace(NioFlowTracer tracer) {
        return mainLine.trace(tracer);
    }

    @Override
    public Diagnostics diagnostics() {
        return mainLine.diagnostics();
    }

    @Override
    public NioFlow<T> scoped() {
        return mainLine.scoped();
    }

    @Override
    public NioFlow<T> release() {
        return mainLine.release();
    }

    @Override
    public NioFlow<T> remove(String name) {
        return mainLine.remove(name);
    }

    @Override
    public NioFlow<T> replace(String name, UnaryOperator<NioFlow<T>> segment) {
        return mainLine.replace(name, segment);
    }

    @Override
    public NioFlow<T> insertBefore(String anchor, UnaryOperator<NioFlow<T>> segment) {
        return mainLine.insertBefore(anchor, segment);
    }

    @Override
    public NioFlow<T> insertAfter(String anchor, UnaryOperator<NioFlow<T>> segment) {
        return mainLine.insertAfter(anchor, segment);
    }

    @Override
    public NioFlow<T> seal() {
        return mainLine.seal();
    }

    @Override
    public T join() {
        return mainLine.join();
    }

    @Override
    public T join(Duration timeout) {
        return mainLine.join(timeout);
    }

    @Override
    public void close() {
        mainLine.close();
    }

    @Override
    public void close(Duration gracePeriod) {
        mainLine.close(gracePeriod);
    }
}
