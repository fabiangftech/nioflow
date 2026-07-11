package dev.nioflow.core.model;

import java.util.List;
import java.util.function.Function;

/**
 * Parallel split-join: every branch receives the same input value and runs
 * concurrently on the workers; join combines the branch results (in branch
 * declaration order) into the value that continues down the chain. Any branch
 * failure fails the whole fan-out, recoverable downstream like a stage failure.
 */
public record FanOut(String name, List<Function<Object, Object>> branches,
                     Function<List<Object>, Object> join, List<Guard> guards) implements Link {
}
