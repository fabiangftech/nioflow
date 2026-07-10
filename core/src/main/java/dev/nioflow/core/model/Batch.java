package dev.nioflow.core.model;

import java.time.Duration;
import java.util.List;
import java.util.function.Function;

/**
 * A grouping link: values wait here until {@code size} of them arrived or the
 * oldest waited {@code maxWait}, then one async call processes the whole group —
 * ideal for bulk IO. The function must return exactly one result per input,
 * matched by index; each result resumes its own value's flow. A failure fails
 * every value in the group, each one individually recoverable.
 *
 * @param function the bulk call; receives the group in arrival order and must
 *                 return one result per input, matched by index
 * @param size     the group size that triggers an immediate flush; must be positive
 * @param maxWait  how long the oldest value of a partial group may wait before the
 *                 group flushes anyway; must be positive
 * @param guards   the lane markers deciding which values reach this link
 */
public record Batch(Function<List<Object>, List<Object>> function, int size, Duration maxWait,
                    List<Guard> guards) implements Link {

    /**
     * Validates the grouping bounds.
     *
     * @throws IllegalArgumentException when {@code size} or {@code maxWait} is not positive
     */
    public Batch {
        if (size <= 0) {
            throw new IllegalArgumentException("batch size must be positive: " + size);
        }
        if (maxWait.isNegative() || maxWait.isZero()) {
            throw new IllegalArgumentException("batch maxWait must be positive: " + maxWait);
        }
    }
}
