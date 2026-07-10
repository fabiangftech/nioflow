package dev.nioflow.core.model;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A value flowing through the nio-flow: its current payload, the index of the next
 * link to run, and the outcome of every {@link Decision} it has crossed — which
 * determines the lanes it takes. An instance lives in exactly one place at a time
 * (submission queue, a worker, the completion queue or the parked set), so its state
 * needs no synchronization of its own — hand-offs between threads happen through
 * the queues.
 */
public final class FlowValue {

    private final Map<Integer, Boolean> decisions = new HashMap<>();
    private final Map<String, Object> context = new HashMap<>();
    private final long sequence;
    private Object value;
    private int cursor;

    public FlowValue(Object value, long sequence) {
        this.value = value;
        this.sequence = sequence;
    }

    /** Injection order; the engine's last result is the newest sequence to finish. */
    public long sequence() {
        return sequence;
    }

    public Object value() {
        return value;
    }

    public void value(Object value) {
        this.value = value;
    }

    public int cursor() {
        return cursor;
    }

    public void cursor(int cursor) {
        this.cursor = cursor;
    }

    public void advance() {
        cursor++;
    }

    public void decide(int decision, boolean outcome) {
        decisions.put(decision, outcome);
    }

    /** The value's metadata; owned by whoever owns the value, like the rest of it. */
    public Map<String, Object> context() {
        return context;
    }

    /**
     * A fan-out child: carries its own payload but inherits the parent's injection
     * sequence, lane decisions and a copy of the context (siblings run concurrently,
     * so they must not share a mutable map), and starts right after the parent's
     * current link.
     */
    public FlowValue child(Object value) {
        FlowValue child = new FlowValue(value, sequence);
        child.decisions.putAll(decisions);
        child.context.putAll(context);
        child.cursor = cursor + 1;
        return child;
    }

    public boolean satisfies(List<Guard> guards) {
        for (Guard guard : guards) {
            if (!Objects.equals(decisions.get(guard.decision()), guard.expected())) {
                return false;
            }
        }
        return true;
    }
}
