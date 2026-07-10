package dev.nioflow.infrastructure.resilience;

import io.github.resilience4j.bulkhead.Bulkhead;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.core.IntervalFunction;
import io.github.resilience4j.ratelimiter.RateLimiter;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import dev.nioflow.core.facade.Resilience;

import java.time.Duration;

/**
 * Resilience4j adapters for the {@link Resilience} port. This is optional
 * infrastructure: the library only compiles against resilience4j ({@code compileOnly}),
 * so using this class requires {@code io.github.resilience4j} on your classpath.
 * Nothing else in the library touches it — replace or drop the adapter and the core
 * stays intact.
 */
public final class Resilience4j {

    private Resilience4j() {
    }

    /**
     * Retries with exponential backoff, starting at {@code initialBackoff} — the
     * common case, preconfigured. Backoff waits block the value's thread, so prefer
     * it on {@code submit} stages or virtual handle workers. Use
     * {@link #retry(Retry)} for full control (predicates, max interval, ...).
     *
     * @param <T>            the type of the values the decorated stage transforms
     * @param maxAttempts    total attempts including the first one
     * @param initialBackoff wait before the first retry, doubled on each further one
     * @return a policy retrying the stage with exponential backoff
     */
    public static <T> Resilience<T> retry(int maxAttempts, Duration initialBackoff) {
        RetryConfig config = RetryConfig.custom()
                .maxAttempts(maxAttempts)
                .intervalFunction(IntervalFunction.ofExponentialBackoff(initialBackoff))
                .build();
        return retry(Retry.of("nio-flow", config));
    }

    /**
     * Adapts a fully configured Resilience4j retry. An exhausted retry rethrows the
     * last failure, failing only that value.
     *
     * @param <T>   the type of the values the decorated stage transforms
     * @param retry the retry instance; shared by every value crossing the stage
     * @return a policy retrying the stage as the instance dictates
     */
    public static <T> Resilience<T> retry(Retry retry) {
        return function -> Retry.decorateFunction(retry, function);
    }

    /**
     * Adapts a Resilience4j circuit breaker. Non-blocking — safe on any handle
     * worker pool. An open circuit fails values immediately with
     * {@code CallNotPermittedException}, each one recoverable downstream.
     *
     * @param <T>            the type of the values the decorated stage transforms
     * @param circuitBreaker the breaker instance; shared by every value crossing
     *                       the stage, which is what lets it trip
     * @return a policy short-circuiting the stage while the breaker is open
     */
    public static <T> Resilience<T> circuitBreaker(CircuitBreaker circuitBreaker) {
        return function -> CircuitBreaker.decorateFunction(circuitBreaker, function);
    }

    /**
     * Adapts a Resilience4j rate limiter. Values beyond the rate wait for a permit —
     * a blocking policy, so prefer it on {@code submit} stages or virtual handle
     * workers.
     *
     * @param <T>         the type of the values the decorated stage transforms
     * @param rateLimiter the limiter instance; shared by every value crossing the stage
     * @return a policy throttling the stage to the limiter's rate
     */
    public static <T> Resilience<T> rateLimiter(RateLimiter rateLimiter) {
        return function -> RateLimiter.decorateFunction(rateLimiter, function);
    }

    /**
     * Adapts a Resilience4j bulkhead, capping how many values run the stage at once.
     * Values beyond the cap wait for a slot — a blocking policy, so prefer it on
     * {@code submit} stages or virtual handle workers.
     *
     * @param <T>      the type of the values the decorated stage transforms
     * @param bulkhead the bulkhead instance; shared by every value crossing the stage
     * @return a policy bounding the stage's concurrency
     */
    public static <T> Resilience<T> bulkhead(Bulkhead bulkhead) {
        return function -> Bulkhead.decorateFunction(bulkhead, function);
    }
}
