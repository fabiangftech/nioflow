package dev.nioflow.core.facade;

import java.util.function.Function;

/**
 * Decorates a stage function with a resilience concern — retry, circuit breaking,
 * rate limiting, bulkheading. The port is library-agnostic: implement it with a
 * plain lambda or plug an adapter from {@code dev.nioflow.infrastructure}; the
 * engine only ever sees the decorated function.
 *
 * <p>Any policy works on {@code submit} stages and on {@code handle} stages with the
 * default virtual workers — blocking ties up only that value's thread. Only when the
 * nio-flow uses a fixed handle-worker pool should {@code handle} policies stay
 * non-blocking (e.g. a circuit breaker): there a waiting policy ties up a shared
 * worker.
 *
 * @param <T> the type of the values the decorated stage transforms
 */
@FunctionalInterface
public interface Resilience<T> {

    /**
     * Wraps the stage function with this policy. Called once, when the stage is
     * declared — the returned function is what the engine runs for every value, so
     * any policy state (retry counters, breaker state) lives inside it and is shared
     * by all values crossing the stage.
     *
     * @param function the raw stage function to protect
     * @return the decorated function the engine runs instead
     */
    Function<T, T> decorate(Function<T, T> function);

    /**
     * Composes decorators: {@code outer} ends up wrapping this one.
     *
     * @param outer the policy applied around this one
     * @return a policy that applies this one first, then {@code outer}
     */
    default Resilience<T> andThen(Resilience<T> outer) {
        return function -> outer.decorate(decorate(function));
    }
}
