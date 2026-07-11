package dev.nioflow.application.facade;

import dev.nioflow.core.facade.NioFlow;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DefaultNioFlowTimeoutTest {

    private static <T> T sleepy(T value) {
        try {
            Thread.sleep(5_000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return value;
    }

    @Test
    void stageWithinItsTimeoutCompletesNormally() {
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class);

        Integer result = flow.just(5)
                .handle("fast", value -> value + 1, Duration.ofSeconds(5))
                .handle(value -> value * 2)
                .execute();

        assertEquals(12, result);
    }

    @Test
    void stageExceedingItsTimeoutFailsWithTimeoutException() {
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class);

        var failure = assertThrows(CompletionException.class, () -> flow.just(5)
                .handle("sleepy", DefaultNioFlowTimeoutTest::sleepy, Duration.ofMillis(50))
                .execute());

        assertInstanceOf(TimeoutException.class, failure.getCause());
    }

    @Test
    void timeoutIsRecoverableDownstream() {
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class);

        Integer result = flow.just(5)
                .handle("sleepy", DefaultNioFlowTimeoutTest::sleepy, Duration.ofMillis(50))
                .recover("fallback", error -> -1)
                .handle(value -> value * 10)
                .execute();

        assertEquals(-10, result);
    }

    @Test
    void timeoutInsideALaneOnlyAppliesToThatLane() {
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class);
        flow.when(value -> value % 2 == 0)
                .then(lane -> lane
                        .handle("even-sleepy", DefaultNioFlowTimeoutTest::sleepy, Duration.ofMillis(50))
                        .recover("even-fallback", error -> 0))
                .otherwise(lane -> lane
                        .handle("odd", value -> -value));

        assertEquals(0, flow.just(4).execute());  // even: times out and recovers in its lane
        assertEquals(-3, flow.just(3).execute()); // odd: untouched by the other lane's timeout
    }
}
