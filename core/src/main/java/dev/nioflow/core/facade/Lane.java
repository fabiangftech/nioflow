package dev.nioflow.core.facade;

import dev.nioflow.core.model.Retry;

import java.time.Duration;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Restricted builder handed to fork lanes (when/match): only step-declaring
 * operations, indented under their branch. Execution (just/execute) and
 * lifecycle (close) belong to the flow, never to a lane.
 */
public interface Lane<T> {

    Lane<T> handle(Function<T, T> function);

    Lane<T> handle(String name, Function<T, T> function);

    Lane<T> handle(String name, Function<T, T> function, Duration timeout);

    Lane<T> handle(String name, Function<T, T> function, Retry retry);

    Lane<T> handle(String name, Function<T, T> function, Duration timeout, Retry retry);

    /**
     * Context-aware stage; see NioFlow#handleContextual for the contract.
     */
    Lane<T> handleContextual(BiFunction<T, Context, T> function);

    Lane<T> handleContextual(String name, BiFunction<T, Context, T> function);

    /**
     * Boss-inlined stage for pure-CPU, sub-microsecond functions; see
     * NioFlow#handleSync for the contract.
     */
    Lane<T> handleSync(Function<T, T> function);

    Lane<T> handleSync(String name, Function<T, T> function);

    Lane<T> background(Consumer<T> effect);

    Lane<T> background(String name, Consumer<T> effect);

    <R> Lane<R> adapt(Function<T, R> function);

    <R, C> Lane<C> fanOut(List<Function<T, R>> branches, Function<List<R>, C> join);

    <R, C> Lane<C> fanOut(String name, List<Function<T, R>> branches, Function<List<R>, C> join);

    <R> Lane<R> use(Segment<T, R> segment);

    Lane<T> filter(Predicate<T> predicate);

    /**
     * Positional error handling scoped to this lane: it inherits the lane's
     * guards, so it only catches failures of values routed through this branch.
     */
    Lane<T> recover(Function<Throwable, T> function);

    Lane<T> recover(String name, Function<Throwable, T> function);

    LaneCondition<T> when(Predicate<T> predicate);

    LaneCases<T> match();
}
