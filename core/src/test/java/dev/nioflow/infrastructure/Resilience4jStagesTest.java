package dev.nioflow.infrastructure;

import dev.nioflow.application.facade.DefaultNioEngine;
import dev.nioflow.application.facade.DefaultNioFlow;
import dev.nioflow.core.facade.NioFlow;
import io.github.resilience4j.bulkhead.Bulkhead;
import io.github.resilience4j.bulkhead.BulkheadConfig;
import io.github.resilience4j.bulkhead.BulkheadFullException;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Resilience4j decorators on stage functions: breaker-open and
 * bulkhead-full short circuits behave like any stage failure — caught by
 * a downstream recover(), never touching the engine. r4j is compileOnly
 * in core; these tests bring it via testImplementation.
 */
class Resilience4jStagesTest {

    private DefaultNioEngine engine;

    @BeforeEach
    void setUp() {
        engine = new DefaultNioEngine();
    }

    @AfterEach
    void tearDown() {
        engine.shutdown(Duration.ofMillis(200));
    }

    @Test
    void openBreakerShortCircuitsWithoutInvokingTheStage() {
        CircuitBreaker breaker = CircuitBreaker.of("charge", CircuitBreakerConfig.custom()
                .slidingWindowSize(4)
                .minimumNumberOfCalls(4)
                .failureRateThreshold(50)
                .build());
        var invocations = new AtomicInteger();
        var lastError = new AtomicReference<Throwable>();
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        flow.handle("charge", Resilience4jStages.circuitBreaker(breaker, value -> {
                    invocations.incrementAndGet();
                    throw new IllegalStateException("downstream down");
                }))
                .recover("fallback", error -> {
                    lastError.set(error);
                    return -1;
                });

        for (int i = 0; i < 4; i++) {
            assertEquals(-1, flow.just(i).execute());
        }
        assertEquals(CircuitBreaker.State.OPEN, breaker.getState());
        assertEquals(4, invocations.get());

        // Open breaker: fails fast through recovery, the stage never runs.
        assertEquals(-1, flow.just(9).execute());
        assertEquals(4, invocations.get(), "an open breaker must not invoke the stage");
        assertInstanceOf(CallNotPermittedException.class, lastError.get());
    }

    @Test
    void saturatedBulkheadRejectsRecoverably() throws Exception {
        Bulkhead bulkhead = Bulkhead.of("db", BulkheadConfig.custom()
                .maxConcurrentCalls(1)
                .maxWaitDuration(Duration.ZERO)
                .build());
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var lastError = new AtomicReference<Throwable>();
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        flow.handle("db", Resilience4jStages.bulkhead(bulkhead, value -> {
                    entered.countDown();
                    try {
                        release.await(10, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    return value * 10;
                }))
                .recover("fallback", error -> {
                    lastError.set(error);
                    return -1;
                });

        CompletableFuture<Integer> holder = flow.just(1).executeAsync();
        assertTrue(entered.await(5, TimeUnit.SECONDS));

        // The single permit is taken: the second caller fails fast, recovers.
        assertEquals(-1, flow.just(2).executeAsync().orTimeout(5, TimeUnit.SECONDS).join());
        assertInstanceOf(BulkheadFullException.class, lastError.get());

        release.countDown();
        assertEquals(10, holder.orTimeout(5, TimeUnit.SECONDS).join());
    }

    @Test
    void guardedComposesBreakerOverBulkhead() {
        CircuitBreaker breaker = CircuitBreaker.ofDefaults("both");
        Bulkhead bulkhead = Bulkhead.ofDefaults("both");
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        flow.handle("guarded", Resilience4jStages.guarded(breaker, bulkhead, value -> value * 2))
                .handle("tail", value -> value + 1);
        engine.seal();

        assertEquals(15, flow.just(7).execute());
        assertEquals(1, breaker.getMetrics().getNumberOfSuccessfulCalls());
    }
}
