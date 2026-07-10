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

    /** Retries with exponential backoff, starting at {@code initialBackoff}. */
    public static <T> Resilience<T> retry(int maxAttempts, Duration initialBackoff) {
        RetryConfig config = RetryConfig.custom()
                .maxAttempts(maxAttempts)
                .intervalFunction(IntervalFunction.ofExponentialBackoff(initialBackoff))
                .build();
        return retry(Retry.of("nio-flow", config));
    }

    public static <T> Resilience<T> retry(Retry retry) {
        return function -> Retry.decorateFunction(retry, function);
    }

    public static <T> Resilience<T> circuitBreaker(CircuitBreaker circuitBreaker) {
        return function -> CircuitBreaker.decorateFunction(circuitBreaker, function);
    }

    public static <T> Resilience<T> rateLimiter(RateLimiter rateLimiter) {
        return function -> RateLimiter.decorateFunction(rateLimiter, function);
    }

    public static <T> Resilience<T> bulkhead(Bulkhead bulkhead) {
        return function -> Bulkhead.decorateFunction(bulkhead, function);
    }
}
