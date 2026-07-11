package dev.nioflow.core.facade;

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

    Lane<T> background(Consumer<T> effect);

    Lane<T> background(String name, Consumer<T> effect);

    <R> Lane<R> adapt(Function<T, R> function);

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
