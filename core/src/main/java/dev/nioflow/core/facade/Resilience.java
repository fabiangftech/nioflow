package dev.nioflow.core.facade;

import java.util.function.Function;

/**
 * Decorates a stage function with a resilience concern — retry, circuit breaking,
 * rate limiting, bulkheading. The port is library-agnostic: implement it with a
 * plain lambda or plug an adapter from {@code io.nio-flow.infrastructure}; the
 * engine only ever sees the decorated function.
 *
 * <p>Any policy works on {@code submit} stages and on {@code handle} stages with the
 * default virtual workers — blocking ties up only that value's thread. Only when the
 * nio-flow uses a fixed handle-worker pool should {@code handle} policies stay
 * non-blocking (e.g. a circuit breaker): there a waiting policy ties up a shared
 * worker.
 */
@FunctionalInterface
public interface Resilience<T> {

    Function<T, T> decorate(Function<T, T> function);

    /** Composes decorators: {@code outer} ends up wrapping this one. */
    default Resilience<T> andThen(Resilience<T> outer) {
        return function -> outer.decorate(decorate(function));
    }
}
