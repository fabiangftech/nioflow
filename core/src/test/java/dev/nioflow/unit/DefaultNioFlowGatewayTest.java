package dev.nioflow.unit;

import dev.nioflow.application.facade.DefaultNioFlow;
import dev.nioflow.application.facade.NioFlowGateway;
import dev.nioflow.core.model.Backpressure;
import dev.nioflow.core.model.FlowContext;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

class DefaultNioFlowGatewayTest {

    private static final Duration PATIENCE = Duration.ofSeconds(5);

    @Test
    void eachCallResolvesToItsOwnResult() throws Exception {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            defaultNioFlow.handle(x -> x * 10);
            NioFlowGateway<Integer, Integer> gateway = NioFlowGateway.of(defaultNioFlow);
            defaultNioFlow.seal();

            CompletableFuture<Integer> first = gateway.call(1);
            CompletableFuture<Integer> second = gateway.call(2);

            assertEquals(10, first.get(PATIENCE.toMillis(), TimeUnit.MILLISECONDS));
            assertEquals(20, second.get(PATIENCE.toMillis(), TimeUnit.MILLISECONDS));
        }
    }

    @Test
    void aSlowCallDoesNotCrossResultsWithAFastOne() throws Exception {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            CountDownLatch release = new CountDownLatch(1);
            defaultNioFlow.submit(x -> {
                if (x == 1) {
                    await(release);
                }
                return x * 10;
            });
            NioFlowGateway<Integer, Integer> gateway = NioFlowGateway.of(defaultNioFlow);
            defaultNioFlow.seal();

            CompletableFuture<Integer> slow = gateway.call(1);
            CompletableFuture<Integer> fast = gateway.call(2);

            assertEquals(20, fast.get(PATIENCE.toMillis(), TimeUnit.MILLISECONDS));
            assertFalse(slow.isDone());
            release.countDown();
            assertEquals(10, slow.get(PATIENCE.toMillis(), TimeUnit.MILLISECONDS));
        }
    }

    @Test
    void aReTypedChainDeliversTheExitViewsType() throws Exception {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            var exit = defaultNioFlow
                    .submit(x -> x + 1)
                    .adapt(x -> "value-" + x);
            NioFlowGateway<Integer, String> gateway = NioFlowGateway.of(defaultNioFlow, exit);
            defaultNioFlow.seal();

            assertEquals("value-8",
                    gateway.call(7).get(PATIENCE.toMillis(), TimeUnit.MILLISECONDS));
        }
    }

    @Test
    void aFailingValueFailsOnlyItsOwnFuture() throws Exception {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            defaultNioFlow.handle(x -> {
                if (x < 0) {
                    throw new IllegalStateException("negative");
                }
                return x;
            });
            NioFlowGateway<Integer, Integer> gateway = NioFlowGateway.of(defaultNioFlow);
            defaultNioFlow.seal();

            CompletableFuture<Integer> bad = gateway.call(-1);
            CompletableFuture<Integer> good = gateway.call(1);

            assertEquals(1, good.get(PATIENCE.toMillis(), TimeUnit.MILLISECONDS));
            ExecutionException failure = assertThrows(ExecutionException.class,
                    () -> bad.get(PATIENCE.toMillis(), TimeUnit.MILLISECONDS));
            assertInstanceOf(IllegalStateException.class, failure.getCause());
            assertEquals("negative", failure.getCause().getMessage());
        }
    }

    @Test
    void aFilteredValueTimesOutAndIsForgotten() {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            defaultNioFlow.filter(x -> x > 0);
            NioFlowGateway<Integer, Integer> gateway = NioFlowGateway.of(defaultNioFlow);
            defaultNioFlow.seal();

            CompletableFuture<Integer> dropped = gateway.call(-1, Duration.ofMillis(100));

            ExecutionException failure = assertThrows(ExecutionException.class,
                    () -> dropped.get(PATIENCE.toMillis(), TimeUnit.MILLISECONDS));
            assertInstanceOf(TimeoutException.class, failure.getCause());
            awaitForgotten(gateway);
        }
    }

    @Test
    void seededContextTravelsWithTheCall() throws Exception {
        try (DefaultNioFlow<String> defaultNioFlow = new DefaultNioFlow<>()) {
            defaultNioFlow.submit(x -> x + "/" + FlowContext.get("tenant"));
            NioFlowGateway<String, String> gateway = NioFlowGateway.of(defaultNioFlow);
            defaultNioFlow.seal();

            assertEquals("payload/acme", gateway.call("payload", Map.of("tenant", "acme"))
                    .get(PATIENCE.toMillis(), TimeUnit.MILLISECONDS));
        }
    }

    @Test
    void rejectedAdmissionFailsTheFutureInsteadOfThrowing() throws Exception {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>(Backpressure.failing(1))) {
            CountDownLatch release = new CountDownLatch(1);
            defaultNioFlow.submit(x -> {
                await(release);
                return x;
            });
            NioFlowGateway<Integer, Integer> gateway = NioFlowGateway.of(defaultNioFlow);
            defaultNioFlow.seal();

            CompletableFuture<Integer> admitted = gateway.call(1);
            CompletableFuture<Integer> rejected = gateway.call(2);

            ExecutionException failure = assertThrows(ExecutionException.class,
                    () -> rejected.get(PATIENCE.toMillis(), TimeUnit.MILLISECONDS));
            assertInstanceOf(RejectedExecutionException.class, failure.getCause());
            release.countDown();
            assertEquals(1, admitted.get(PATIENCE.toMillis(), TimeUnit.MILLISECONDS));
            assertEquals(0, gateway.pending());
        }
    }

    @Test
    void aFanOutCompletesTheCallWithTheFirstResult() throws Exception {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            var exit = defaultNioFlow.fanOut(x -> List.of(x, x + 1));
            NioFlowGateway<Integer, Integer> gateway = NioFlowGateway.of(defaultNioFlow, exit);
            defaultNioFlow.seal();

            int result = gateway.call(1).get(PATIENCE.toMillis(), TimeUnit.MILLISECONDS);

            assertTrue(result == 1 || result == 2);
            defaultNioFlow.join();
            assertEquals(0, gateway.pending());
        }
    }

    @Test
    void anUntypedGatewayAcceptsAnyInputAndRoutesByRuntimeType() throws Exception {
        try (DefaultNioFlow<Object> defaultNioFlow = new DefaultNioFlow<>()) {
            var exit = defaultNioFlow.match()
                    .is(v -> v instanceof Integer, lane -> lane
                            .handle(v -> (Integer) v * 10))
                    .is(v -> v instanceof String, lane -> lane
                            .handle(v -> ((String) v).toUpperCase()))
                    .otherwise(lane -> lane
                            .handle(v -> "unsupported:" + v.getClass().getSimpleName()));
            NioFlowGateway<Object, Object> gateway = NioFlowGateway.of(defaultNioFlow, exit);
            defaultNioFlow.seal();

            assertEquals(70, gateway.call(7).get(PATIENCE.toMillis(), TimeUnit.MILLISECONDS));
            assertEquals("ACME", gateway.call("acme").get(PATIENCE.toMillis(), TimeUnit.MILLISECONDS));
            assertEquals("unsupported:Double",
                    gateway.call(3.14).get(PATIENCE.toMillis(), TimeUnit.MILLISECONDS));
        }
    }

    @Test
    void plainlyInjectedValuesAreIgnoredByTheGateway() throws Exception {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            defaultNioFlow.handle(x -> x * 10);
            NioFlowGateway<Integer, Integer> gateway = NioFlowGateway.of(defaultNioFlow);
            defaultNioFlow.seal();

            defaultNioFlow.just(5); // around the gateway: no correlation id
            CompletableFuture<Integer> called = gateway.call(1);

            assertEquals(10, called.get(PATIENCE.toMillis(), TimeUnit.MILLISECONDS));
            defaultNioFlow.join();
            assertEquals(0, gateway.pending());
        }
    }

    /** The timeout cleanup runs on another thread: waits until the call is forgotten. */
    static void awaitForgotten(dev.nioflow.core.facade.NioFlowGateway<?, ?> gateway) {
        long deadline = System.nanoTime() + PATIENCE.toNanos();
        while (gateway.pending() != 0) {
            assertTrue(System.nanoTime() < deadline, "gateway still has pending calls");
            Thread.onSpinWait();
        }
    }

    private static void await(CountDownLatch latch) {
        try {
            assertTrue(latch.await(PATIENCE.toMillis(), TimeUnit.MILLISECONDS));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }
}
