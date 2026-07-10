package dev.nioflow.core.model;

import java.util.Map;
import java.util.function.Supplier;

/**
 * Per-value metadata (trace id, tenant, ...) that travels with the value instead of
 * the thread. The engine binds it around every execution of user code for a value —
 * stages, predicates, fan-outs, recoveries, {@code onComplete} and {@code onError}
 * handlers — so
 * {@link #get} and {@link #put} work no matter which worker or virtual thread runs
 * the code. Seed it with {@code just(input, context)}; fan-out children inherit a
 * copy. Batch functions process many values at once and run unbound.
 */
public final class FlowContext {

    private static final ScopedValue<Map<String, Object>> CURRENT = ScopedValue.newInstance();

    private FlowContext() {
    }

    /**
     * Reads the current value's metadata. Safe to call anywhere: outside a bound
     * execution it simply reports the metadata as absent.
     *
     * @param key the metadata key to look up
     * @return the value's metadata for {@code key}, or null when absent or when no
     *         nio-flow value is bound to this thread
     */
    public static Object get(String key) {
        return CURRENT.isBound() ? CURRENT.get().get(key) : null;
    }

    /**
     * Adds metadata visible to every later stage of the current value.
     *
     * @param key   the metadata key to write
     * @param value the metadata value, replacing any previous one under {@code key}
     * @throws IllegalStateException when called outside a bound execution — that is,
     *                               from code the engine is not running for a value
     */
    public static void put(String key, Object value) {
        if (!CURRENT.isBound()) {
            throw new IllegalStateException("no nio-flow value is bound to this thread");
        }
        CURRENT.get().put(key, value);
    }

    /**
     * Engine use: runs {@code call} with the given value's context bound, so the
     * user code it wraps can reach {@link #get} and {@link #put}.
     *
     * @param <R>     the type the wrapped code returns
     * @param context the value's metadata map to bind for the duration of the call
     * @param call    the user code to run under the binding
     * @return whatever {@code call} returns
     */
    public static <R> R bound(Map<String, Object> context, Supplier<R> call) {
        return ScopedValue.where(CURRENT, context).call(() -> call.get());
    }
}
