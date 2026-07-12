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
 * keys the shared in-flight accumulator by link instance, and two
 * structurally identical batch links must never pool their values.
 */
public final class Batch implements Link {

    private final String name;
    private final int size;
    private final Duration window;
    private final Function<List<Object>, List<Object>> bulk;
    private final List<Guard> guards;

    public Batch(String name, int size, Duration window,
                 Function<List<Object>, List<Object>> bulk, List<Guard> guards) {
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
