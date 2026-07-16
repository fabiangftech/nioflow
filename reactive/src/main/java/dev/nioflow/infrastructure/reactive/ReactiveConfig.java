package dev.nioflow.infrastructure.reactive;

import dev.nioflow.core.facade.Context;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * What a reactive flow declares ONCE and everything it hands out inherits: the
 * default budget of its reactive steps, the context keys it bridges from the
 * Reactor subscriber context, whether reactive steps prefer the async path, and
 * whether an unbudgeted reactive step is a build-time error.
 *
 * <p>It rides along every re-wrap — into {@code just()}'s pipeline, into a
 * branch's lane, into a fork's segment — which is what makes "declared once, on
 * the flow" true rather than aspirational. Immutable, and {@link #NONE} is the
 * shared empty one: a flow that declares neither pays for neither.
 */
record ReactiveConfig(Duration budget, List<Context.Key<?>> keys, boolean preferAsync, boolean requireBudget) {

    // requireBudget is ON by default (RFC 0034): an unbudgeted reactive step is
    // the documented forever-leak (a parked worker on a hung upstream), so the
    // safe thing is mandatory unless the flow opts out with allowUnbudgeted() for
    // the Mono.just/cache chains that genuinely need none. A defaultBudget or a
    // per-step budget satisfies it; allowUnbudgeted() waives it.
    static final ReactiveConfig NONE = new ReactiveConfig(null, List.of(), false, true);

    ReactiveConfig withBudget(Duration budget) {
        if (budget == null || budget.isZero() || budget.isNegative()) {
            throw new IllegalArgumentException("defaultBudget must be a positive duration, was " + budget);
        }
        return new ReactiveConfig(budget, keys, preferAsync, requireBudget);
    }

    /**
     * Routes {@code handleMono}/{@code adaptMono} to their async (future-holding)
     * equivalents instead of parking a worker on the Mono. Set on the pipelines a
     * {@code pipe} runs — the ingestion loop at high concurrency, where a parked
     * worker per element is 3 KB of stack each. A direct request/response leaves
     * it false: one parked worker for one request is the trade RFC 0006 judged
     * fine at low concurrency.
     */
    ReactiveConfig withPreferAsync() {
        return preferAsync ? this : new ReactiveConfig(budget, keys, true, requireBudget);
    }

    /**
     * Turns an unbudgeted reactive step into a BUILD-TIME error: with this on,
     * every {@code handleMono}/{@code adaptMono}/{@code handleMonoAsync}/
     * {@code adaptMonoAsync}/{@code adaptFlux}/{@code fanOutMono} that resolves to
     * a null budget (none of its own and no {@link #withBudget defaultBudget}) is
     * rejected where the caller's line still exists. Off by default so a
     * {@code Mono.just(...)} chain stays frictionless; on, it is the mechanical
     * guarantee that a network-facing flow cannot leak on a hung call. See RFC 0028.
     */
    ReactiveConfig withRequireBudget() {
        return requireBudget ? this : new ReactiveConfig(budget, keys, preferAsync, true);
    }

    /**
     * Waives the default budget requirement (RFC 0034): with this on, an
     * unbudgeted reactive step is allowed instead of a build-time error. It is the
     * opt-out for the {@code Mono.just(...)}/in-memory-cache chains that genuinely
     * need no budget — a conscious "these steps park on nothing, I accept it",
     * where {@code requireBudget} is on by default because a network-facing step
     * that forgets one leaks a worker forever.
     */
    ReactiveConfig withAllowUnbudgeted() {
        return requireBudget ? new ReactiveConfig(budget, keys, preferAsync, false) : this;
    }

    /**
     * The effective budget for a reactive step: the step's own if it declared
     * one, otherwise the flow default. When {@link #withRequireBudget
     * requireBudget} is on and neither exists, this is a BUILD-TIME error — a
     * reactive step with no budget can hang forever (a parked worker on the
     * blocking path; a pinned execution and a leaked connection on the async
     * path), and requireBudget() makes that a build failure instead of a
     * production leak. See RFC 0028.
     */
    Duration budgetFor(String step, Duration stepBudget) {
        Duration effective = stepBudget != null ? stepBudget : budget;
        if (effective == null && requireBudget) {
            throw new IllegalStateException("Reactive step '" + step + "' has no budget and requireBudget() is on:"
                    + " give it one (e.g. handleMono(name, call, budget)) or declare a defaultBudget(...) on the flow."
                    + " A reactive step with no budget can hang forever — a parked worker on the blocking path,"
                    + " a pinned execution and connection on the async path.");
        }
        return effective;
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
        return new ReactiveConfig(budget, List.copyOf(copy), preferAsync, requireBudget);
    }
}
