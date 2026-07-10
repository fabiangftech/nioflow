package dev.nioflow.application.facade;


import dev.nioflow.core.model.FlowValue;

import java.util.ArrayList;
import java.util.List;

/**
 * Values waiting at a {@code Batch} link — the one extra place a {@code FlowValue}
 * may live besides the queues, a worker and the parked list. All access is guarded
 * by the engine's lock.
 */
final class BatchBuffer {

    private final List<FlowValue> flows = new ArrayList<>();
    private long deadline;

    /**
     * Adds a value; the first arrival of a group arms the flush deadline, so
     * {@code maxWait} is measured from the group's oldest value.
     */
    void add(FlowValue flow, long deadlineIfFirst) {
        if (flows.isEmpty()) {
            deadline = deadlineIfFirst;
        }
        flows.add(flow);
    }

    /** Whether the group reached the batch size and must flush now. */
    boolean isFull(int size) {
        return flows.size() >= size;
    }

    /** How many values are waiting — reported by diagnostics as {@code batched}. */
    int size() {
        return flows.size();
    }

    /** Whether a non-empty group's oldest value waited past its deadline. */
    boolean expired(long now) {
        return !flows.isEmpty() && now >= deadline;
    }

    /** Empties the buffer and returns the group, ready for its single async call. */
    List<FlowValue> drain() {
        List<FlowValue> drained = List.copyOf(flows);
        flows.clear();
        return drained;
    }
}
