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
 *
 * @param capacity the maximum number of values in flight before the policy applies;
 *                 must be positive
 * @param policy   what {@code just} does with a new value once at capacity
 */
public record Backpressure(int capacity, OverflowPolicy policy) {

    /**
     * Validates the policy.
     *
     * @throws IllegalArgumentException when {@code capacity} is not positive
     */
    public Backpressure {
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity must be positive: " + capacity);
        }
    }

    /**
     * No admission control: every {@code just} is admitted immediately.
     *
     * @return a policy that never limits injection
     */
    public static Backpressure unbounded() {
        return new Backpressure(Integer.MAX_VALUE, OverflowPolicy.BLOCK);
    }

    /**
     * At capacity, {@code just} blocks the producer thread until a slot frees up —
     * the natural choice for producers that should slow down to the nio-flow's pace.
     *
     * @param capacity the maximum number of values in flight; must be positive
     * @return a bounded, blocking admission policy
     */
    public static Backpressure blocking(int capacity) {
        return new Backpressure(capacity, OverflowPolicy.BLOCK);
    }

    /**
     * At capacity, {@code just} silently discards the new value — the choice for
     * lossy streams where staying current beats completeness.
     *
     * @param capacity the maximum number of values in flight; must be positive
     * @return a bounded, dropping admission policy
     */
    public static Backpressure dropping(int capacity) {
        return new Backpressure(capacity, OverflowPolicy.DROP);
    }

    /**
     * At capacity, {@code just} throws {@code RejectedExecutionException} — the
     * choice when the producer must notice overload and react itself.
     *
     * @param capacity the maximum number of values in flight; must be positive
     * @return a bounded, failing admission policy
     */
    public static Backpressure failing(int capacity) {
        return new Backpressure(capacity, OverflowPolicy.FAIL);
    }
}
