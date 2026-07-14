package dev.nioflow.infrastructure.reactive;

import reactor.core.Exceptions;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionException;
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

    /** Fan-out branches: each Mono awaits on its own worker, concurrently. */
    static <T, R> List<Function<T, R>> branches(List<Function<T, Mono<R>>> reactive, Duration budget) {
        List<Function<T, R>> blocking = new ArrayList<>(reactive.size());
        for (Function<T, Mono<R>> branch : reactive) {
            blocking.add(value -> await(budgeted(branch.apply(value), budget)));
        }
        return blocking;
    }
}
