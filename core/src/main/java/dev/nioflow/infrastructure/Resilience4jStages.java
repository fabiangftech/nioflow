package dev.nioflow.infrastructure;

import io.github.resilience4j.bulkhead.Bulkhead;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;

import java.util.function.Function;
import java.util.function.UnaryOperator;

/**
 * Resilience4j decorators for stage functions. r4j is a compileOnly
 * dependency of core: this class only loads if the consumer brings
 * resilience4j to the runtime classpath. Rate limiting and retry are
 * native (RateLimit / Retry) — what stays external is the stateful pair:
 * circuit breaker and bulkhead.
 *
 * Decorated functions are plain Functions, so they fuse like any stage
 * and their short-circuit failures (CallNotPermittedException while the
 * breaker is open, BulkheadFullException when saturated) flow to the
 * recovery path like any stage failure — declare a recover() downstream
 * as the fallback. Composition notes: with a native Retry on the same
 * stage, EACH attempt passes through the decorators (the breaker counts
 * every attempt); a bulkhead wait parks the virtual worker — cheap, and
 * never the boss — and counts against a stage timeout budget.
 *
 * <pre>
 * flow.handle("charge", Resilience4jStages.circuitBreaker(breaker, payments::charge))
 *     .recover("charge-fallback", error -> Order.degraded());
 * </pre>
 */
public final class Resilience4jStages {

    private Resilience4jStages() {
    }

    /**
     * Every application must be permitted by the breaker; outcomes feed its
     * state machine. Share one breaker across stages to trip them together.
     */
    public static <T> UnaryOperator<T> circuitBreaker(CircuitBreaker breaker, UnaryOperator<T> function) {
        // r4j decorates into a Function; a stage is type-preserving, so hand it
        // back as the UnaryOperator that handle() takes.
        Function<T, T> decorated = CircuitBreaker.decorateFunction(breaker, function);
        return decorated::apply;
    }

    /**
     * Caps concurrent applications: above the limit callers wait up to the
     * bulkhead's maxWaitDuration (parking the virtual worker) and then fail
     * with BulkheadFullException.
     */
    public static <T> UnaryOperator<T> bulkhead(Bulkhead bulkhead, UnaryOperator<T> function) {
        Function<T, T> decorated = Bulkhead.decorateFunction(bulkhead, function);
        return decorated::apply;
    }

    /**
     * Both guards, breaker outermost: a saturated bulkhead trips the breaker
     * like any other failure.
     */
    public static <T> UnaryOperator<T> guarded(CircuitBreaker breaker, Bulkhead bulkhead,
                                               UnaryOperator<T> function) {
        return circuitBreaker(breaker, bulkhead(bulkhead, function));
    }
}
