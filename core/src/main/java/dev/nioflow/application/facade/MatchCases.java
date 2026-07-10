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
 * A multi-way fork under construction. Every {@code is} appends a decision guarded
 * by "all previous cases were false", so cases short-circuit in declaration order
 * like a switch; {@code remaining} always views the values no case has taken yet.
 * Any other call falls through to the fork's parent view — the main line.
 */
final class MatchCases<T> implements dev.nioflow.core.facade.NioFlow.Cases<T> {

    private final NioFlow<T> mainLine;
    private NioFlow<T> remaining;

    MatchCases(NioFlow<T> mainLine) {
        this.mainLine = mainLine;
        this.remaining = mainLine;
    }

    @Override
    public dev.nioflow.core.facade.NioFlow.Cases<T> is(Predicate<T> predicate, UnaryOperator<dev.nioflow.core.facade.NioFlow<T>> lane) {
        int decision = remaining.decision(predicate);
        lane.apply(remaining.lane(decision, true));
        remaining = remaining.lane(decision, false);
        return this;
    }

    @Override
    public dev.nioflow.core.facade.NioFlow<T> otherwise(UnaryOperator<dev.nioflow.core.facade.NioFlow<T>> lane) {
        lane.apply(remaining);
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
    public dev.nioflow.core.facade.NioFlow<T> filter(Predicate<T> predicate) {
        return mainLine.filter(predicate);
    }

    @Override
    public dev.nioflow.core.facade.NioFlow.Condition<T> when(Predicate<T> predicate) {
        return mainLine.when(predicate);
    }

    @Override
    public dev.nioflow.core.facade.NioFlow.Cases<T> match() {
        return mainLine.match();
    }

    @Override
    public dev.nioflow.core.facade.NioFlow<T> onError(Consumer<Throwable> handler) {
        return mainLine.onError(handler);
    }

    @Override
    public dev.nioflow.core.facade.NioFlow<T> onErrorResume(Function<Throwable, T> fallback) {
        return mainLine.onErrorResume(fallback);
    }

    @Override
    public dev.nioflow.core.facade.NioFlow<T> onComplete(Consumer<T> handler) {
        return mainLine.onComplete(handler);
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
