package dev.nioflow.unit;

import dev.nioflow.application.facade.DefaultNioFlow;
import dev.nioflow.application.facade.NioFlowGateway;
import dev.nioflow.infrastructure.spring.NioFlowReactive;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.*;

class DefaultNioFlowReactiveTest {

    private static final Duration PATIENCE = Duration.ofSeconds(5);

    @Test
    void aMonoEmitsTheValuesOwnResult() {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            var exit = defaultNioFlow
                    .submit(x -> x + 1)
                    .adapt(x -> "value-" + x);
            NioFlowGateway<Integer, String> gateway = NioFlowGateway.of(defaultNioFlow, exit);
            defaultNioFlow.seal();

            assertEquals("value-8", NioFlowReactive.mono(gateway, 7).block(PATIENCE));
        }
    }

    @Test
    void aFailingValueErrorsTheMono() {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            defaultNioFlow.handle(x -> {
                throw new IllegalStateException("boom");
            });
            NioFlowGateway<Integer, Integer> gateway = NioFlowGateway.of(defaultNioFlow);
            defaultNioFlow.seal();

            Mono<Integer> mono = NioFlowReactive.mono(gateway, 1);

            IllegalStateException failure =
                    assertThrows(IllegalStateException.class, () -> mono.block(PATIENCE));
            assertEquals("boom", failure.getMessage());
        }
    }

    @Test
    void aDroppedValueErrorsTheMonoOnTimeout() {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            defaultNioFlow.filter(x -> x > 0);
            NioFlowGateway<Integer, Integer> gateway = NioFlowGateway.of(defaultNioFlow);
            defaultNioFlow.seal();

            Mono<Integer> mono = NioFlowReactive.mono(gateway, -1, Duration.ofMillis(100));

            RuntimeException failure =
                    assertThrows(RuntimeException.class, () -> mono.block(PATIENCE));
            assertInstanceOf(TimeoutException.class, failure.getCause());
            DefaultNioFlowGatewayTest.awaitForgotten(gateway);
        }
    }

    @Test
    void theMonoIsColdEachSubscriptionInjectsAFreshValue() {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            CopyOnWriteArrayList<Integer> seen = new CopyOnWriteArrayList<>();
            defaultNioFlow.handle(x -> {
                seen.add(x);
                return x * 10;
            });
            NioFlowGateway<Integer, Integer> gateway = NioFlowGateway.of(defaultNioFlow);
            defaultNioFlow.seal();

            Mono<Integer> mono = NioFlowReactive.mono(gateway, 3);
            assertTrue(seen.isEmpty()); // nothing injected before subscription

            assertEquals(30, mono.block(PATIENCE));
            assertEquals(30, mono.block(PATIENCE));
            assertEquals(2, seen.size());
        }
    }
}
