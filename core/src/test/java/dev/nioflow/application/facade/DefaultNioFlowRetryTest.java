package dev.nioflow.application.facade;

import dev.nioflow.core.facade.NioFlow;
import dev.nioflow.core.facade.NioFlowMetrics;
import dev.nioflow.core.model.Retry;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DefaultNioFlowRetryTest {

    @Test
    void retriesUntilTheAttemptSucceeds() {
        var attempts = new AtomicInteger();
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class);

        Integer result = flow.just(5)
                .handle("flaky", value -> {
                    if (attempts.incrementAndGet() < 3) {
                        throw new IllegalStateException("transient");
                    }
                    return value * 10;
                }, Retry.of(3, Duration.ofMillis(1)))
                .execute();

        assertEquals(50, result);
        assertEquals(3, attempts.get());
    }

    @Test
    void exhaustedRetriesFlowToRecovery() {
        var attempts = new AtomicInteger();
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class);

        Integer result = flow.just(5)
                .handle("down", value -> {
                    attempts.incrementAndGet();
                    throw new IllegalStateException("still down");
                }, Retry.of(2, Duration.ofMillis(1)))
                .recover("fallback", error -> -1)
                .execute();

        assertEquals(-1, result);
        assertEquals(2, attempts.get());
    }

    @Test
    void exhaustedRetriesWithoutRecoveryFailWithTheLastError() {
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class);

        var failure = assertThrows(CompletionException.class, () -> flow.just(1)
                .handle("down", value -> {
                    throw new IllegalStateException("still down");
                }, Retry.of(2, Duration.ZERO))
                .execute());

        assertEquals("still down", failure.getCause().getMessage());
    }

    @Test
    void retryComposesWithPerAttemptTimeout() {
        var attempts = new AtomicInteger();
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class);

        var failure = assertThrows(CompletionException.class, () -> flow.just(1)
                .handle("sleepy", value -> {
                    attempts.incrementAndGet();
                    try {
                        Thread.sleep(5_000);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    return value;
                }, Duration.ofMillis(50), Retry.of(3, Duration.ofMillis(1)))
                .execute());

        assertInstanceOf(TimeoutException.class, failure.getCause());
        assertEquals(3, attempts.get()); // the budget applied to EACH attempt
    }

    @Test
    void retryInsideAFusedRunKeepsWorking() {
        var attempts = new AtomicInteger();
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class);

        Integer result = flow.just(5)
                .handle("before", value -> value + 1)
                .handle("flaky", value -> {
                    if (attempts.incrementAndGet() < 2) {
                        throw new IllegalStateException("transient");
                    }
                    return value * 10;
                }, Retry.of(2, Duration.ZERO))
                .handle("after", value -> value - 3)
                .execute();

        assertEquals(57, result); // ((5+1) * 10) - 3, retried inside the run
        assertEquals(2, attempts.get());
    }

    @Test
    void retriesAreObservableThroughMetrics() {
        var retried = new AtomicInteger();
        var engine = new DefaultNioEngine();
        engine.metrics(new NioFlowMetrics() {
            @Override
            public void stageRetried(String stage) {
                retried.incrementAndGet();
            }
        });
        var attempts = new AtomicInteger();
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        flow.handle("flaky", value -> {
            if (attempts.incrementAndGet() < 3) {
                throw new IllegalStateException("transient");
            }
            return value;
        }, Retry.of(3, Duration.ZERO));

        assertEquals(7, flow.just(7).execute());
        assertEquals(2, retried.get()); // 3 attempts = 2 retries
        engine.shutdown(Duration.ofMillis(100));
    }

    @Test
    void retryPolicyValidatesItsArguments() {
        assertThrows(IllegalArgumentException.class, () -> Retry.of(0, Duration.ofMillis(1)));
        assertThrows(IllegalArgumentException.class, () -> Retry.of(2, Duration.ofMillis(-1)));
        assertThrows(IllegalArgumentException.class, () -> new Retry(2, Duration.ZERO, 0.5));
        assertEquals(4_000_000L, Retry.exponential(3, Duration.ofMillis(1)).delayNanos(3)); // 1ms * 2^2
    }
}
