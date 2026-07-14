package dev.nioflow.core.facade;

import dev.nioflow.core.model.RateLimit;
import dev.nioflow.core.model.Retry;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

/**
 * Restricted builder handed to fork lanes (when/match): only step-declaring
 * operations, indented under their branch. Execution (just/execute) and
 * lifecycle (close) belong to the flow, never to a lane.
 */
public interface Lane<T> {

    Lane<T> handle(UnaryOperator<T> function);

    Lane<T> handle(String name, UnaryOperator<T> function);

    Lane<T> handle(String name, UnaryOperator<T> function, Duration timeout);

    Lane<T> handle(String name, UnaryOperator<T> function, Retry retry);

    Lane<T> handle(String name, UnaryOperator<T> function, Duration timeout, Retry retry);

    /**
     * Rate-limited stage; see NioFlow#handle(String, Function, RateLimit).
     */
    Lane<T> handle(String name, UnaryOperator<T> function, RateLimit rateLimit);

    /**
     * The stage that does not park; see NioFlow#handleAsync. Type-preserving
     * over the lane's T, like every other lane step.
     */
    Lane<T> handleAsync(String name, Function<T, CompletionStage<T>> call);

    Lane<T> handleAsync(String name, Function<T, CompletionStage<T>> call, Duration timeout);

    Lane<T> handleAsync(String name, Function<T, CompletionStage<T>> call, Retry retry);

    Lane<T> handleAsync(String name, Function<T, CompletionStage<T>> call, Duration timeout, Retry retry);

    /** The re-typing async stage; see NioStep#adaptAsync. */
    <R> Lane<R> adaptAsync(Function<T, CompletionStage<R>> call);

    <R> Lane<R> adaptAsync(Function<T, CompletionStage<R>> call, Duration timeout);

    /**
     * Context-aware stage; see NioFlow#handleContextual for the contract.
     */
    Lane<T> handleContextual(BiFunction<T, Context, T> function);

    Lane<T> handleContextual(String name, BiFunction<T, Context, T> function);

    /**
     * Boss-inlined stage for pure-CPU, sub-microsecond functions; see
     * NioFlow#handleSync for the contract.
     */
    Lane<T> handleSync(UnaryOperator<T> function);

    Lane<T> handleSync(String name, UnaryOperator<T> function);

    Lane<T> background(Consumer<T> effect);

    Lane<T> background(String name, Consumer<T> effect);

    <R> Lane<R> adapt(Function<T, R> function);

    <R, C> Lane<C> fanOut(List<Function<T, R>> branches, Function<List<R>, C> join);

    <R, C> Lane<C> fanOut(String name, List<Function<T, R>> branches, Function<List<R>, C> join);

    /**
     * Coalescing point; see NioFlow#batch. Lane-scoped: only values routed
     * through this branch pool together.
     */
    <R> Lane<R> batch(int size, Duration window, Function<List<T>, List<R>> bulk);

    <R> Lane<R> batch(String name, int size, Duration window, Function<List<T>, List<R>> bulk);

    /**
     * Detached sub-flow; see NioFlow#fork. Declared inside a lane it inherits
     * the lane's guards: only values routed down this branch spawn it.
     */
    <R> Lane<T> fork(Segment<T, R> sub);

    <R> Lane<T> fork(String name, Segment<T, R> sub);

    <R> Lane<R> use(Segment<T, R> segment);

    /**
     * Embeds and remembers the span as a named region; see NioFlow#use.
     */
    <R> Lane<R> use(String region, Segment<T, R> segment);

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
