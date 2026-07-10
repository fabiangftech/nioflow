package dev.nioflow.unit;

import dev.nioflow.application.facade.NioFlow;
import dev.nioflow.application.facade.NioFlowGateway;
import dev.nioflow.infrastructure.spring.NioFlowReactive;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.*;

class NioFlowReactiveTest {

    private static final Duration PATIENCE = Duration.ofSeconds(5);

    @Test
    void aMonoEmitsTheValuesOwnResult() {
        try (NioFlow<Integer> nioFlow = new NioFlow<>()) {
            var exit = nioFlow
                    .submit(x -> x + 1)
                    .adapt(x -> "value-" + x);
            NioFlowGateway<Integer, String> gateway = NioFlowGateway.of(nioFlow, exit);
            nioFlow.seal();

            assertEquals("value-8", NioFlowReactive.mono(gateway, 7).block(PATIENCE));
        }
    }

    @Test
    void aFailingValueErrorsTheMono() {
        try (NioFlow<Integer> nioFlow = new NioFlow<>()) {
            nioFlow.handle(x -> {
                throw new IllegalStateException("boom");
            });
            NioFlowGateway<Integer, Integer> gateway = NioFlowGateway.of(nioFlow);
            nioFlow.seal();

            Mono<Integer> mono = NioFlowReactive.mono(gateway, 1);

            IllegalStateException failure =
                    assertThrows(IllegalStateException.class, () -> mono.block(PATIENCE));
            assertEquals("boom", failure.getMessage());
        }
    }

    @Test
    void aDroppedValueErrorsTheMonoOnTimeout() {
        try (NioFlow<Integer> nioFlow = new NioFlow<>()) {
            nioFlow.filter(x -> x > 0);
            NioFlowGateway<Integer, Integer> gateway = NioFlowGateway.of(nioFlow);
            nioFlow.seal();

            Mono<Integer> mono = NioFlowReactive.mono(gateway, -1, Duration.ofMillis(100));

            RuntimeException failure =
                    assertThrows(RuntimeException.class, () -> mono.block(PATIENCE));
            assertInstanceOf(TimeoutException.class, failure.getCause());
            NioFlowGatewayTest.awaitForgotten(gateway);
        }
    }

    @Test
    void theMonoIsColdEachSubscriptionInjectsAFreshValue() {
        try (NioFlow<Integer> nioFlow = new NioFlow<>()) {
            CopyOnWriteArrayList<Integer> seen = new CopyOnWriteArrayList<>();
            nioFlow.handle(x -> {
                seen.add(x);
                return x * 10;
            });
            NioFlowGateway<Integer, Integer> gateway = NioFlowGateway.of(nioFlow);
            nioFlow.seal();

            Mono<Integer> mono = NioFlowReactive.mono(gateway, 3);
            assertTrue(seen.isEmpty()); // nothing injected before subscription

            assertEquals(30, mono.block(PATIENCE));
            assertEquals(30, mono.block(PATIENCE));
            assertEquals(2, seen.size());
        }
    }
}
