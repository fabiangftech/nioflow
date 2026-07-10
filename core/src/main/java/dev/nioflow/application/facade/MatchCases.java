package dev.nioflow.application.facade;

import dev.nioflow.core.facade.NioFlow;
import dev.nioflow.core.facade. NioFlowMetrics;
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
 * A {@code match} fork under construction. Every {@link #is} appends a decision
 * guarded by "all previous cases were false", so cases short-circuit in declaration
 * order like a switch; {@code remaining} always views the values no case has taken
 * yet. Any other call falls through to the fork's parent view — the main line —
 * which is what lets unmatched values pass through unchanged when no
 * {@code otherwise} is declared.
 *
 * <p>All delegating overrides inherit their contract from
 * {@link NioFlow}; only the fork mechanics live here.
 *
 * @param <T> the type of the values flowing through the fork
 */
final class MatchCases<T> implements NioFlow.Cases<T> {

    private final DefaultNioFlow<T> mainLine;
    private DefaultNioFlow<T> remaining;

    /**
     * @param mainLine the view the fork was declared on, resumed after the lanes;
     *                 also the initial {@code remaining} — before any case, every
     *                 value is still unmatched
     */
    MatchCases(DefaultNioFlow<T> mainLine) {
        this.mainLine = mainLine;
        this.remaining = mainLine;
    }

    /**
     * Declares the next case: its decision link is appended on the {@code remaining}
     * view, so only values no earlier case took even evaluate the predicate. The
     * lane builds on the decision's true side; {@code remaining} narrows to its
     * false side, ready for the next case.
     */
    @Override
    public NioFlow.Cases<T> is(Predicate<T> predicate, UnaryOperator<NioFlow<T>> lane) {
        int decision = remaining.decision(predicate);
        lane.apply(remaining.lane(decision, true));
        remaining = remaining.lane(decision, false);
        return this;
    }

    /**
     * Builds the default lane directly on the {@code remaining} view — the values
     * no case matched — and returns the main line, whose stages run for every
     * value again.
     */
    @Override
    public NioFlow<T> otherwise(UnaryOperator<NioFlow<T>> lane) {
        lane.apply(remaining);
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
    public NioFlow<T> filter(Predicate<T> predicate) {
        return mainLine.filter(predicate);
    }

    @Override
    public NioFlow.Condition<T> when(Predicate<T> predicate) {
        return mainLine.when(predicate);
    }

    @Override
    public NioFlow.Cases<T> match() {
        return mainLine.match();
    }

    @Override
    public NioFlow<T> onError(Consumer<Throwable> handler) {
        return mainLine.onError(handler);
    }

    @Override
    public NioFlow<T> onErrorResume(Function<Throwable, T> fallback) {
        return mainLine.onErrorResume(fallback);
    }

    @Override
    public NioFlow<T> onComplete(Consumer<T> handler) {
        return mainLine.onComplete(handler);
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
}
