package dev.nioflow.application.facade;

import dev.nioflow.core.facade.NioFlowMetrics;
import dev.nioflow.core.facade.NioFlowTracer;
import dev.nioflow.core.facade.Resilience;
import dev.nioflow.core.model.Diagnostics;

import java.time.Duration;
import java.util.List;
import java.util.Map;
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
 * {@link dev.nioflow.core.facade.NioFlow}; only the fork mechanics live here.
 *
 * @param <T> the type of the values flowing through the fork
 */
final class ForkBranch<T> implements dev.nioflow.core.facade.NioFlow.Branch<T> {

    private final NioFlow<T> mainLine;
    private final int decision;

    /**
     * @param mainLine the view the fork was declared on, resumed after the lanes
     * @param decision the fork's decision id, guarding the links of both lanes
     */
    ForkBranch(NioFlow<T> mainLine, int decision) {
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
    public dev.nioflow.core.facade.NioFlow<T> otherwise(UnaryOperator<dev.nioflow.core.facade.NioFlow<T>> lane) {
        lane.apply(mainLine.lane(decision, false));
        return mainLine;
    }

    @Override
    public dev.nioflow.core.facade.NioFlow<T> just(T input) {
        return mainLine.just(input);
    }

    @Override
    public dev.nioflow.core.facade.NioFlow<T> just(T input, Map<String, Object> context) {
        return mainLine.just(input, context);
    }

    @Override
    public dev.nioflow.core.facade.NioFlow<T> justAll(Iterable<T> inputs) {
        return mainLine.justAll(inputs);
    }

    @Override
    public dev.nioflow.core.facade.NioFlow<T> filter(Predicate<T> predicate) {
        return mainLine.filter(predicate);
    }

    @Override
    public dev.nioflow.core.facade.NioFlow<T> handle(Function<T, T> function) {
        return mainLine.handle(function);
    }

    @Override
    public dev.nioflow.core.facade.NioFlow<T> handle(String name, Function<T, T> function) {
        return mainLine.handle(name, function);
    }

    @Override
    public dev.nioflow.core.facade.NioFlow<T> handle(Function<T, T> function, Resilience<T> resilience) {
        return mainLine.handle(function, resilience);
    }

    @Override
    public dev.nioflow.core.facade.NioFlow<T> submit(Function<T, T> function) {
        return mainLine.submit(function);
    }

    @Override
    public dev.nioflow.core.facade.NioFlow<T> submit(String name, Function<T, T> function) {
        return mainLine.submit(name, function);
    }

    @Override
    public dev.nioflow.core.facade.NioFlow<T> submit(Function<T, T> function, Duration timeout) {
        return mainLine.submit(function, timeout);
    }

    @Override
    public dev.nioflow.core.facade.NioFlow<T> submit(Function<T, T> function, Resilience<T> resilience) {
        return mainLine.submit(function, resilience);
    }

    @Override
    public dev.nioflow.core.facade.NioFlow<T> batch(int size, Duration maxWait, Function<List<T>, List<T>> function) {
        return mainLine.batch(size, maxWait, function);
    }

    @Override
    public <N> dev.nioflow.core.facade.NioFlow<N> adapt(Function<T, N> function) {
        return mainLine.adapt(function);
    }

    @Override
    public <N> dev.nioflow.core.facade.NioFlow<N> fanOut(Function<T, List<N>> function) {
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
    public dev.nioflow.core.facade.NioFlow<T> onError(Consumer<Throwable> handler) {
        return mainLine.onError(handler);
    }

    @Override
    public dev.nioflow.core.facade.NioFlow<T> onComplete(Consumer<T> handler) {
        return mainLine.onComplete(handler);
    }

    @Override
    public dev.nioflow.core.facade.NioFlow<T> onErrorResume(Function<Throwable, T> fallback) {
        return mainLine.onErrorResume(fallback);
    }

    @Override
    public dev.nioflow.core.facade.NioFlow<T> metrics(NioFlowMetrics metrics) {
        return mainLine.metrics(metrics);
    }

    @Override
    public dev.nioflow.core.facade.NioFlow<T> trace(NioFlowTracer tracer) {
        return mainLine.trace(tracer);
    }

    @Override
    public Diagnostics diagnostics() {
        return mainLine.diagnostics();
    }

    @Override
    public dev.nioflow.core.facade.NioFlow<T> seal() {
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
}
