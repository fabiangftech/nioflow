package dev.nioflow.infrastructure.reactive;

import dev.nioflow.core.facade.Context;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * What a reactive flow declares ONCE and everything it hands out inherits: the
 * default budget of its reactive steps, and the context keys it bridges from the
 * Reactor subscriber context.
 *
 * <p>It rides along every re-wrap — into {@code just()}'s pipeline, into a
 * branch's lane, into a fork's segment — which is what makes "declared once, on
 * the flow" true rather than aspirational. Immutable, and {@link #NONE} is the
 * shared empty one: a flow that declares neither pays for neither.
 */
record ReactiveConfig(Duration budget, List<Context.Key<?>> keys) {

    static final ReactiveConfig NONE = new ReactiveConfig(null, List.of());

    ReactiveConfig withBudget(Duration budget) {
        if (budget == null || budget.isZero() || budget.isNegative()) {
            throw new IllegalArgumentException("defaultBudget must be a positive duration, was " + budget);
        }
        return new ReactiveConfig(budget, keys);
    }

    /**
     * The propagated keys, replacing whatever was declared before — the flow
     * states its whitelist, it does not accumulate one across call sites.
     *
     * <p>Nothing to propagate is a mistake, not a no-op: it is a
     * {@code propagate()} somebody wrote expecting keys to cross, and silence is
     * the one outcome that would never tell them otherwise. Rejected here, at
     * build time, where the line that wrote it still exists.
     */
    ReactiveConfig withKeys(Context.Key<?>... declared) {
        if (declared == null || declared.length == 0) {
            throw new IllegalArgumentException("propagate() needs at least one key:"
                    + " it declares WHAT crosses from the subscriber context, and nothing crosses that is not named");
        }
        List<Context.Key<?>> copy = new ArrayList<>(declared.length);
        for (Context.Key<?> key : declared) {
            if (key == null) {
                throw new IllegalArgumentException("propagate() was handed a null key");
            }
            copy.add(key);
        }
        return new ReactiveConfig(budget, List.copyOf(copy));
    }
}
