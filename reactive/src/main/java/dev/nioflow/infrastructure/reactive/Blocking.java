package dev.nioflow.infrastructure.reactive;

import reactor.core.Exceptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

/**
 * The one thing the whole facade does: turn a Mono-returning call into a plain
 * function the chain can hold. It parks a VIRTUAL worker — never a boss, never
 * a Netty event loop — so the "block" here is a thread that unmounts, not a
 * thread that is lost.
 *
 * <p>Lost is exactly what it becomes without a budget, though: a Mono that never
 * completes parks its worker for the life of the JVM (the engine has no
 * cancellation). Hence {@link #budgeted}: every reactive step routes its Mono
 * through it, so a flow that declared a {@code defaultBudget} cannot leak a
 * worker on a hung socket.
 */
final class Blocking {

    private Blocking() {
    }

    /**
     * Puts the budget ON THE MONO — {@code mono.timeout(d)} cancels the
     * subscription (reactor-netty releases the connection), where a stage
     * timeout would only abandon the parked worker.
     *
     * <p>A null budget means "none declared": neither on the step nor as the
     * flow's default. The Mono travels through untouched, which is the right
     * thing for {@code Mono.just(...)} or a cache lookup — and a permanently
     * parked worker for anything that talks to the network.
     */
    static <T> Mono<T> budgeted(Mono<T> mono, Duration budget) {
        return budget == null ? mono : mono.timeout(budget);
    }

    /**
     * Turns an empty value-carrying Mono into an {@link EmptyMonoException}
     * BEFORE it becomes a silent {@code null} (RFC 0027). The error is deferred
     * (a supplier), so a Mono that DOES emit builds no exception. Every
     * value-carrying reactive step routes through here — {@code handleMono},
     * {@code adaptMono} and each {@code fanOutMono} BRANCH (a repository lookup
     * that misses is exactly the empty Mono this exists for, and the join would
     * otherwise read a null out of its results list). {@code adaptFlux} awaits a
     * {@code collectList}, which emits an empty list, never an empty Mono, so it
     * is unaffected.
     */
    static <T> Mono<T> required(Mono<T> mono, String step) {
        return mono.switchIfEmpty(Mono.error(() -> new EmptyMonoException(step)));
    }

    /** The blocking value path: require exactly one value, budget it, await it. */
    static <T> T awaitValue(Mono<T> mono, Duration budget, String step) {
        return await(budgeted(required(mono, step), budget));
    }

    /**
     * The async value path: require exactly one value, then to a
     * {@link CompletionStage} — an empty Mono completes the future exceptionally
     * with {@link EmptyMonoException}, exactly as the blocking path throws it, so
     * {@code preferAsync} and the block path agree.
     */
    static <T> CompletionStage<T> requiredFuture(Mono<T> mono, String step) {
        return required(mono, step).toFuture();
    }

    /**
     * Awaits the Mono and re-throws its failure AS ITSELF.
     *
     * <p>{@code Mono.block()} wraps whatever it throws in Reactor's
     * ReactiveException, so a {@code mono.timeout(d)} would reach
     * {@code recover()} as a Reactor wrapper instead of the
     * {@link java.util.concurrent.TimeoutException} it really is — and a
     * reactive stage would stop looking like an ordinary stage, which is the one
     * thing this facade promises. Unwrapping here is what keeps the promise: a
     * timed-out Mono and a timed-out {@code handle(name, fn, timeout)} hand
     * {@code recover()} the same exception.
     *
     * <p>A checked cause travels in a CompletionException, which the engine
     * unwraps on the recovery path exactly as it does for its own timeouts.
     */
    static <T> T await(Mono<T> mono) {
        try {
            return mono.block();
        } catch (RuntimeException error) {
            Throwable cause = Exceptions.unwrap(error);
            if (cause instanceof RuntimeException runtime) {
                throw runtime;
            }
            throw new CompletionException(cause);
        }
    }

    /**
     * Collects a Flux into a List that CANNOT be bigger than the cap.
     *
     * <p>{@code take(maxItems + 1)} cancels the source the moment the bound is
     * exceeded, so an overrun costs one extra element — not the rest of a stream
     * that may have ten million of them. The overflow is thrown here, on the
     * virtual worker, as a plain Java exception: it travels the recovery path as
     * itself, exactly like any other stage failure.
     */
    static <R> List<R> awaitBounded(Flux<R> flux, int maxItems, Duration budget) {
        List<R> items = await(budgeted(flux.take(maxItems + 1L).collectList(), budget));
        if (items.size() > maxItems) {
            throw new FlowOverflowException(
                    "adaptFlux(" + maxItems + ") — the stream produced more than " + maxItems + " items");
        }
        return items;
    }

    /**
     * A cap of zero (or less) is not a bound, it is a mistake: rejected at BUILD
     * time, where the caller's line number still exists.
     */
    static void checkMaxItems(int maxItems) {
        if (maxItems < 1) {
            throw new IllegalArgumentException("adaptFlux maxItems must be at least 1, was " + maxItems
                    + ": it is the largest List the chain agrees to carry");
        }
    }

    /**
     * Async fan-out branches: each Mono becomes a {@link CompletionStage}
     * ({@code mono.timeout(budget).toFuture()}), so core's {@code fanOutAsync}
     * runs them concurrently while PARKING NOTHING — a worker only invokes each
     * branch and is released, where the blocking form parked one worker per
     * branch for the duration of the slowest call (RFC 0016). The budget rides on
     * the Mono, so a hung branch is cancelled (subscription disposed) and reaches
     * {@code recover()} as a {@link java.util.concurrent.TimeoutException},
     * exactly as on the async main line.
     */
    static <T, R> List<Function<T, CompletionStage<R>>> asyncBranches(
            List<Function<T, Mono<R>>> reactive, Duration budget, String step) {
        List<Function<T, CompletionStage<R>>> async = new ArrayList<>(reactive.size());
        for (Function<T, Mono<R>> branch : reactive) {
            // required INSIDE, budgeted OUTSIDE — the same order awaitValue uses,
            // so the timeout still wraps the switchIfEmpty. Without required, an
            // empty branch Mono completed its future with null and the join read
            // a null out of its results list: RFC 0027's silent null, in the one
            // value-carrying step that skipped the guard.
            async.add(value -> budgeted(required(branch.apply(value), step), budget).toFuture());
        }
        return async;
    }
}
