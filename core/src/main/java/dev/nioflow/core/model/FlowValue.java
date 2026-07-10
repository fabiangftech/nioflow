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

    /**
     * A freshly injected value, starting at the first link of the chain.
     *
     * @param value    the injected payload the flow starts with
     * @param sequence the value's injection order, fixed for its whole life
     */
    public FlowValue(Object value, long sequence) {
        this.value = value;
        this.sequence = sequence;
    }

    /**
     * Injection order; the engine's last result is the newest sequence to finish.
     *
     * @return the value's position in injection order, shared by fan-out children
     */
    public long sequence() {
        return sequence;
    }

    /**
     * The current payload — replaced by every stage the value crosses.
     *
     * @return the payload as of the last stage that ran
     */
    public Object value() {
        return value;
    }

    /**
     * Replaces the payload with a stage's output.
     *
     * @param value the payload the value continues with
     */
    public void value(Object value) {
        this.value = value;
    }

    /**
     * The index into the shared chain of the next link this value runs.
     *
     * @return the cursor position; the value parked when it reaches the chain's size
     */
    public int cursor() {
        return cursor;
    }

    /**
     * Moves the cursor to an arbitrary link — used when a recovery jumps the value
     * forward to the fallback that caught its error.
     *
     * @param cursor the index of the link to run next
     */
    public void cursor(int cursor) {
        this.cursor = cursor;
    }

    /** Moves the cursor to the next link of the chain. */
    public void advance() {
        cursor++;
    }

    /**
     * Records the outcome of a fork's predicate, deciding once and for all which
     * lane of that fork this value takes.
     *
     * @param decision the fork's decision id
     * @param outcome  the predicate's result for this value
     */
    public void decide(int decision, boolean outcome) {
        decisions.put(decision, outcome);
    }

    /**
     * The value's metadata; owned by whoever owns the value, like the rest of it.
     *
     * @return the mutable metadata map bound as {@code FlowContext} around user code
     */
    public Map<String, Object> context() {
        return context;
    }

    /**
     * A fan-out child: carries its own payload but inherits the parent's injection
     * sequence, lane decisions and a copy of the context (siblings run concurrently,
     * so they must not share a mutable map), and starts right after the parent's
     * current link.
     *
     * @param value the child's own payload
     * @return the child, ready to be offered to the submission queue
     */
    public FlowValue child(Object value) {
        FlowValue child = new FlowValue(value, sequence);
        child.decisions.putAll(decisions);
        child.context.putAll(context);
        child.cursor = cursor + 1;
        return child;
    }

    /**
     * Whether this value's recorded decisions match every guard — that is, whether
     * a link marked with these guards belongs to a lane this value took.
     *
     * @param guards the link's lane markers, one per enclosing fork
     * @return true when every guard matches; a fork this value never crossed
     *         matches nothing
     */
    public boolean satisfies(List<Guard> guards) {
        for (Guard guard : guards) {
            if (!Objects.equals(decisions.get(guard.decision()), guard.expected())) {
                return false;
            }
        }
        return true;
    }
}
