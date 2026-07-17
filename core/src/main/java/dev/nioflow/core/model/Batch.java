package dev.nioflow.core.model;

import java.time.Duration;
import java.util.List;
import java.util.function.Function;

/**
 * Coalescing point: executions reaching this link park until `size` of
 * them accumulated or `window` elapsed since the first, then ONE bulk
 * call processes all their values (positionally: element i of the result
 * belongs to the i-th parked execution) and each execution continues its
 * own chain with its own element. Callers never see the batch — their
 * futures complete with their individual results; a bulk failure (or a
 * result of the wrong size) fails every batched execution, recoverable
 * downstream per execution.
 *
 * Deliberately a class with identity equality, not a record: the engine
 * keys the shared in-flight accumulator by {@link #groupKey()}, and two
 * structurally identical batch links must never pool their values.
 *
 * <p>The group key exists because a batch link is REBUILT whenever the chain
 * around it is rebuilt with different guards — entering a lane, and (since RFC
 * 0038) on every per-request pipeline whose decision ids get compacted. A
 * rebuilt copy is a different instance but the SAME batch point, so keying the
 * group by instance silently split one pool into one group per request: no
 * coalescing, and a leaked group each time. {@link #withGuards} carries the key
 * across the rebuild; only a genuinely new {@code Batch(...)} mints a new one.
 */
public final class Batch implements Link {

    private final String name;
    private final int size;
    private final Duration window;
    private final Function<List<Object>, List<Object>> bulk;
    private final List<Guard> guards;
    // Identity of the batch POINT, stable across guard rebuilds. Never equal
    // between two separately declared batches, however identical they look.
    private final Object groupKey;

    public Batch(String name, int size, Duration window,
                 Function<List<Object>, List<Object>> bulk, List<Guard> guards) {
        this(name, size, window, bulk, guards, new Object());
    }

    private Batch(String name, int size, Duration window,
                  Function<List<Object>, List<Object>> bulk, List<Guard> guards, Object groupKey) {
        if (size < 1) {
            throw new IllegalArgumentException("batch size must be >= 1");
        }
        if (window == null || window.isZero() || window.isNegative()) {
            throw new IllegalArgumentException("batch window must be positive");
        }
        this.name = name;
        this.size = size;
        this.window = window;
        this.bulk = bulk;
        this.guards = guards;
        this.groupKey = groupKey;
    }

    /**
     * The same batch point under different guards: same group, new routing.
     * This is what keeps a lane-scoped or per-request batch pooling with the
     * definition it was declared on.
     */
    public Batch withGuards(List<Guard> newGuards) {
        return new Batch(name, size, window, bulk, newGuards, groupKey);
    }

    /** Identity of the batch point; the engine keys in-flight groups by this. */
    public Object groupKey() {
        return groupKey;
    }

    public String name() {
        return name;
    }

    public int size() {
        return size;
    }

    public Duration window() {
        return window;
    }

    public Function<List<Object>, List<Object>> bulk() {
        return bulk;
    }

    @Override
    public List<Guard> guards() {
        return guards;
    }
}
