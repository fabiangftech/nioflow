package dev.nioflow.unit;

import dev.nioflow.application.facade.DefaultNioFlow;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import dev.nioflow.core.facade.Resilience;
import dev.nioflow.infrastructure.resilience.Resilience4j;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class DefaultNioFlowResilienceTest {

    @Test
    void aFlakyStageIsRetriedUntilItSucceeds() {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            AtomicInteger attempts = new AtomicInteger();
            defaultNioFlow.submit(x -> {
                if (attempts.incrementAndGet() < 3) {
                    throw new IllegalStateException("flaky");
                }
                return x * 10;
            }, Resilience4j.retry(3, Duration.ofMillis(10)));

            defaultNioFlow.just(4);

            assertEquals(40, defaultNioFlow.join());
            assertEquals(3, attempts.get());
        }
    }

    @Test
    void exhaustedRetriesFailOnlyThatValue() {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            AtomicInteger attempts = new AtomicInteger();
            List<Integer> completed = new CopyOnWriteArrayList<>();
            defaultNioFlow.submit(x -> {
                        if (x == 1) {
                            attempts.incrementAndGet();
                            throw new IllegalStateException("always failing");
                        }
                        return x * 10;
                    }, Resilience4j.retry(2, Duration.ofMillis(10)))
                    .onComplete(completed::add);

            defaultNioFlow.just(1);
            defaultNioFlow.just(2);

            assertThrows(CompletionException.class, defaultNioFlow::join);
            assertEquals(2, attempts.get());
            assertEquals(List.of(20), completed);
        }
    }

    @Test
    void anOpenCircuitBreakerFailsFastWithoutCallingTheStage() {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            CircuitBreaker breaker = CircuitBreaker.of("io-nioFlow-test",
                    CircuitBreakerConfig.custom()
                            .slidingWindowSize(2)
                            .minimumNumberOfCalls(2)
                            .failureRateThreshold(50)
                            .waitDurationInOpenState(Duration.ofMinutes(1))
                            .build());
            AtomicInteger calls = new AtomicInteger();
            List<Throwable> errors = new CopyOnWriteArrayList<>();
            defaultNioFlow.submit(x -> {
                        calls.incrementAndGet();
                        throw new IllegalStateException("downstream is down");
                    }, Resilience4j.circuitBreaker(breaker))
                    .onError(errors::add);

            defaultNioFlow.just(1);
            assertThrows(CompletionException.class, defaultNioFlow::join);
            defaultNioFlow.just(2);
            assertThrows(CompletionException.class, defaultNioFlow::join);

            // two failures opened the breaker: the third value never reaches the stage
            defaultNioFlow.just(3);
            assertThrows(CompletionException.class, defaultNioFlow::join);
            assertEquals(2, calls.get());
            assertInstanceOf(CallNotPermittedException.class, errors.getLast());
        }
    }

    @Test
    void aHandleAcceptsNonBlockingPolicies() {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            CircuitBreaker breaker = CircuitBreaker.of("io-nioFlow-handle",
                    CircuitBreakerConfig.custom()
                            .slidingWindowSize(2)
                            .minimumNumberOfCalls(2)
                            .failureRateThreshold(50)
                            .waitDurationInOpenState(Duration.ofMinutes(1))
                            .build());
            AtomicInteger calls = new AtomicInteger();
            List<Throwable> errors = new CopyOnWriteArrayList<>();
            defaultNioFlow.handle(x -> {
                        calls.incrementAndGet();
                        throw new IllegalStateException("bad mapping");
                    }, Resilience4j.circuitBreaker(breaker))
                    .onError(errors::add);

            defaultNioFlow.just(1);
            assertThrows(CompletionException.class, defaultNioFlow::join);
            defaultNioFlow.just(2);
            assertThrows(CompletionException.class, defaultNioFlow::join);

            defaultNioFlow.just(3);
            assertThrows(CompletionException.class, defaultNioFlow::join);
            assertEquals(2, calls.get());
            assertInstanceOf(CallNotPermittedException.class, errors.getLast());
        }
    }

    @Test
    void andThenWrapsThisPolicyWithTheOuterOne() {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            Resilience<Integer> plusOneAfter = function -> x -> function.apply(x) + 1;
            Resilience<Integer> timesTwoAfter = function -> x -> function.apply(x) * 2;

            // timesTwo wraps plusOne: (x + 1) * 2
            defaultNioFlow.submit(x -> x, plusOneAfter.andThen(timesTwoAfter));

            defaultNioFlow.just(5);
            assertEquals(12, defaultNioFlow.join());
        }
    }

    @Test
    void thePortWorksWithoutResilience4j() {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            AtomicInteger attempts = new AtomicInteger();
            // hand-rolled one-retry policy: the port has no library coupling
            Resilience<Integer> retryOnce = function -> x -> {
                try {
                    return function.apply(x);
                } catch (RuntimeException firstFailure) {
                    return function.apply(x);
                }
            };
            defaultNioFlow.submit(x -> {
                if (attempts.incrementAndGet() == 1) {
                    throw new IllegalStateException("first attempt fails");
                }
                return x + 1;
            }, retryOnce);

            defaultNioFlow.just(41);

            assertEquals(42, defaultNioFlow.join());
            assertEquals(2, attempts.get());
        }
    }
}
