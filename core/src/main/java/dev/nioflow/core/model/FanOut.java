package dev.nioflow.core.model;

import java.util.List;
import java.util.function.Function;

/**
 * Parallel split-join: every branch receives the same input value and runs
 * concurrently on the workers; join combines the branch results (in branch
 * declaration order) into the value that continues down the chain. Any branch
 * failure fails the whole fan-out, recoverable downstream like a stage failure.
 *
 * <p>{@code async} distinguishes the two flavours the engine dispatches
 * differently: a sync branch is a {@code Function<Object, Object>} whose worker
 * parks on any blocking it does; an async branch is a
 * {@code Function<Object, CompletionStage<Object>>} (stored erased to
 * {@code Object}) — a worker only INVOKES it and is released, the countdown
 * fires when the stage completes, so a fan-out over remote calls parks no
 * worker. The branch list, the join and the guard semantics are otherwise
 * identical; only the completion mechanism differs.
 */
public record FanOut(String name, List<Function<Object, Object>> branches,
                     Function<List<Object>, Object> join, boolean async, List<Guard> guards)
        implements Link {
}
