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
 */
public record Batch(Function<List<Object>, List<Object>> function, int size, Duration maxWait,
                    List<Guard> guards) implements Link {

    public Batch {
        if (size <= 0) {
            throw new IllegalArgumentException("batch size must be positive: " + size);
        }
        if (maxWait.isNegative() || maxWait.isZero()) {
            throw new IllegalArgumentException("batch maxWait must be positive: " + maxWait);
        }
    }
}
