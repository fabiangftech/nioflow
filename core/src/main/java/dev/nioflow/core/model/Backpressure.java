package dev.nioflow.core.model;

/**
 * Admission control for {@code just}: at most {@code capacity} values in flight,
 * resolved by {@code policy} when full. Only injection is bounded — values already
 * flowing always re-enter the engine's queues, so nothing in flight is ever lost
 * to backpressure.
 *
 * <p>Careful with {@link OverflowPolicy#BLOCK} and stages that inject new values:
 * a worker blocked inside {@code just} cannot finish its value, so a full nio-flow
 * where every worker is injecting deadlocks. Prefer {@code DROP}/{@code FAIL} or
 * capacity headroom for feedback loops.
 */
public record Backpressure(int capacity, OverflowPolicy policy) {

    public Backpressure {
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity must be positive: " + capacity);
        }
    }

    public static Backpressure unbounded() {
        return new Backpressure(Integer.MAX_VALUE, OverflowPolicy.BLOCK);
    }

    public static Backpressure blocking(int capacity) {
        return new Backpressure(capacity, OverflowPolicy.BLOCK);
    }

    public static Backpressure dropping(int capacity) {
        return new Backpressure(capacity, OverflowPolicy.DROP);
    }

    public static Backpressure failing(int capacity) {
        return new Backpressure(capacity, OverflowPolicy.FAIL);
    }
}
