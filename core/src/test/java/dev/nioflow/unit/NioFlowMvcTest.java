package dev.nioflow.unit;

import dev.nioflow.application.facade.NioFlow;
import dev.nioflow.application.facade.NioFlowGateway;
import dev.nioflow.infrastructure.spring.NioFlowMvc;
import org.junit.jupiter.api.Test;
import org.springframework.web.context.request.async.DeferredResult;

import java.time.Duration;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.*;

class NioFlowMvcTest {

    private static final Duration PATIENCE = Duration.ofSeconds(5);

    @Test
    void aDeferredResultResolvesWithTheValuesOwnResult() {
        try (NioFlow<Integer> nioFlow = new NioFlow<>()) {
            var exit = nioFlow
                    .submit(x -> x + 1)
                    .adapt(x -> "value-" + x);
            NioFlowGateway<Integer, String> gateway = NioFlowGateway.of(nioFlow, exit);
            nioFlow.seal();

            DeferredResult<String> deferred = NioFlowMvc.deferred(gateway, 7);

            assertEquals("value-8", settledResult(deferred));
        }
    }

    @Test
    void aFailingValueErrorsTheDeferredResult() {
        try (NioFlow<Integer> nioFlow = new NioFlow<>()) {
            nioFlow.handle(x -> {
                throw new IllegalStateException("boom");
            });
            NioFlowGateway<Integer, Integer> gateway = NioFlowGateway.of(nioFlow);
            nioFlow.seal();

            DeferredResult<Integer> deferred = NioFlowMvc.deferred(gateway, 1);

            Object result = settledResult(deferred);
            assertInstanceOf(IllegalStateException.class, result);
            assertEquals("boom", ((Throwable) result).getMessage());
        }
    }

    @Test
    void aDroppedValueErrorsTheDeferredResultOnTimeout() {
        try (NioFlow<Integer> nioFlow = new NioFlow<>()) {
            nioFlow.filter(x -> x > 0);
            NioFlowGateway<Integer, Integer> gateway = NioFlowGateway.of(nioFlow);
            nioFlow.seal();

            DeferredResult<Integer> deferred =
                    NioFlowMvc.deferred(gateway, -1, Duration.ofMillis(100));

            assertInstanceOf(TimeoutException.class, settledResult(deferred));
            NioFlowGatewayTest.awaitForgotten(gateway);
        }
    }

    /** Polls until the deferred result settles, failing the test after PATIENCE. */
    private static Object settledResult(DeferredResult<?> deferred) {
        long deadline = System.nanoTime() + PATIENCE.toNanos();
        while (!deferred.hasResult()) {
            assertTrue(System.nanoTime() < deadline, "deferred result never settled");
            Thread.onSpinWait();
        }
        return deferred.getResult();
    }
}
