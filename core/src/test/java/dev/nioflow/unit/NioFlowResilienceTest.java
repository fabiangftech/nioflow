package dev.nioflow.unit;

import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import dev.nioflow.application.facade.NioFlow;
import dev.nioflow.core.facade.Resilience;
import dev.nioflow.infrastructure.resilience.Resilience4j;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class NioFlowResilienceTest {

    @Test
    void aFlakyStageIsRetriedUntilItSucceeds() {
        try (NioFlow<Integer> pipeline = new NioFlow<>()) {
            AtomicInteger attempts = new AtomicInteger();
            pipeline.submit(x -> {
                if (attempts.incrementAndGet() < 3) {
                    throw new IllegalStateException("flaky");
                }
                return x * 10;
            }, Resilience4j.retry(3, Duration.ofMillis(10)));

            pipeline.just(4);

            assertEquals(40, pipeline.join());
            assertEquals(3, attempts.get());
        }
    }

    @Test
    void exhaustedRetriesFailOnlyThatValue() {
        try (NioFlow<Integer> pipeline = new NioFlow<>()) {
            AtomicInteger attempts = new AtomicInteger();
            List<Integer> completed = new CopyOnWriteArrayList<>();
            pipeline.submit(x -> {
                        if (x == 1) {
                            attempts.incrementAndGet();
                            throw new IllegalStateException("always failing");
                        }
                        return x * 10;
                    }, Resilience4j.retry(2, Duration.ofMillis(10)))
                    .onComplete(completed::add);

            pipeline.just(1);
            pipeline.just(2);

            assertThrows(CompletionException.class, pipeline::join);
            assertEquals(2, attempts.get());
            assertEquals(List.of(20), completed);
        }
    }

    @Test
    void anOpenCircuitBreakerFailsFastWithoutCallingTheStage() {
        try (NioFlow<Integer> pipeline = new NioFlow<>()) {
            CircuitBreaker breaker = CircuitBreaker.of("io-pipeline-test",
                    CircuitBreakerConfig.custom()
                            .slidingWindowSize(2)
                            .minimumNumberOfCalls(2)
                            .failureRateThreshold(50)
                            .waitDurationInOpenState(Duration.ofMinutes(1))
                            .build());
            AtomicInteger calls = new AtomicInteger();
            List<Throwable> errors = new CopyOnWriteArrayList<>();
            pipeline.submit(x -> {
                        calls.incrementAndGet();
                        throw new IllegalStateException("downstream is down");
                    }, Resilience4j.circuitBreaker(breaker))
                    .onError(errors::add);

            pipeline.just(1);
            assertThrows(CompletionException.class, pipeline::join);
            pipeline.just(2);
            assertThrows(CompletionException.class, pipeline::join);

            // two failures opened the breaker: the third value never reaches the stage
            pipeline.just(3);
            assertThrows(CompletionException.class, pipeline::join);
            assertEquals(2, calls.get());
            assertInstanceOf(CallNotPermittedException.class, errors.getLast());
        }
    }

    @Test
    void aHandleAcceptsNonBlockingPolicies() {
        try (NioFlow<Integer> pipeline = new NioFlow<>()) {
            CircuitBreaker breaker = CircuitBreaker.of("io-pipeline-handle",
                    CircuitBreakerConfig.custom()
                            .slidingWindowSize(2)
                            .minimumNumberOfCalls(2)
                            .failureRateThreshold(50)
                            .waitDurationInOpenState(Duration.ofMinutes(1))
                            .build());
            AtomicInteger calls = new AtomicInteger();
            List<Throwable> errors = new CopyOnWriteArrayList<>();
            pipeline.handle(x -> {
                        calls.incrementAndGet();
                        throw new IllegalStateException("bad mapping");
                    }, Resilience4j.circuitBreaker(breaker))
                    .onError(errors::add);

            pipeline.just(1);
            assertThrows(CompletionException.class, pipeline::join);
            pipeline.just(2);
            assertThrows(CompletionException.class, pipeline::join);

            pipeline.just(3);
            assertThrows(CompletionException.class, pipeline::join);
            assertEquals(2, calls.get());
            assertInstanceOf(CallNotPermittedException.class, errors.getLast());
        }
    }

    @Test
    void andThenWrapsThisPolicyWithTheOuterOne() {
        try (NioFlow<Integer> pipeline = new NioFlow<>()) {
            Resilience<Integer> plusOneAfter = function -> x -> function.apply(x) + 1;
            Resilience<Integer> timesTwoAfter = function -> x -> function.apply(x) * 2;

            // timesTwo wraps plusOne: (x + 1) * 2
            pipeline.submit(x -> x, plusOneAfter.andThen(timesTwoAfter));

            pipeline.just(5);
            assertEquals(12, pipeline.join());
        }
    }

    @Test
    void thePortWorksWithoutResilience4j() {
        try (NioFlow<Integer> pipeline = new NioFlow<>()) {
            AtomicInteger attempts = new AtomicInteger();
            // hand-rolled one-retry policy: the port has no library coupling
            Resilience<Integer> retryOnce = function -> x -> {
                try {
                    return function.apply(x);
                } catch (RuntimeException firstFailure) {
                    return function.apply(x);
                }
            };
            pipeline.submit(x -> {
                if (attempts.incrementAndGet() == 1) {
                    throw new IllegalStateException("first attempt fails");
                }
                return x + 1;
            }, retryOnce);

            pipeline.just(41);

            assertEquals(42, pipeline.join());
            assertEquals(2, attempts.get());
        }
    }
}
