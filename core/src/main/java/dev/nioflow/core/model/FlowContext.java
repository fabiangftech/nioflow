package dev.nioflow.core.model;

import java.util.Map;
import java.util.function.Supplier;

/**
 * Per-value metadata (trace id, tenant, ...) that travels with the value instead of
 * the thread. The engine binds it around every execution of user code for a value —
 * stages, predicates, fan-outs, recoveries and {@code onComplete} handlers — so
 * {@link #get} and {@link #put} work no matter which worker or virtual thread runs
 * the code. Seed it with {@code just(input, context)}; fan-out children inherit a
 * copy. Batch functions process many values at once and run unbound.
 */
public final class FlowContext {

    private static final ScopedValue<Map<String, Object>> CURRENT = ScopedValue.newInstance();

    private FlowContext() {
    }

    /** The value's metadata for {@code key}, or null when absent or unbound. */
    public static Object get(String key) {
        return CURRENT.isBound() ? CURRENT.get().get(key) : null;
    }

    /** Adds metadata visible to every later stage of the current value. */
    public static void put(String key, Object value) {
        if (!CURRENT.isBound()) {
            throw new IllegalStateException("no nio-flow value is bound to this thread");
        }
        CURRENT.get().put(key, value);
    }

    /** Engine use: runs {@code call} with the given value's context bound. */
    public static <R> R bound(Map<String, Object> context, Supplier<R> call) {
        return ScopedValue.where(CURRENT, context).call(() -> call.get());
    }
}
