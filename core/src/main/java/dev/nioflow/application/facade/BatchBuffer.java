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

    /** Adds a value; the first arrival of a group arms the flush deadline. */
    void add(FlowValue flow, long deadlineIfFirst) {
        if (flows.isEmpty()) {
            deadline = deadlineIfFirst;
        }
        flows.add(flow);
    }

    boolean isFull(int size) {
        return flows.size() >= size;
    }

    int size() {
        return flows.size();
    }

    boolean expired(long now) {
        return !flows.isEmpty() && now >= deadline;
    }

    List<FlowValue> drain() {
        List<FlowValue> drained = List.copyOf(flows);
        flows.clear();
        return drained;
    }
}
