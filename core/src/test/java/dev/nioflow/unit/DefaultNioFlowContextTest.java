package dev.nioflow.unit;

import dev.nioflow.application.facade.DefaultNioFlow;
import dev.nioflow.core.model.FlowContext;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.*;

class DefaultNioFlowContextTest {

    @Test
    void seededContextIsVisibleInSyncStages() {
        try (DefaultNioFlow<String> defaultNioFlow = new DefaultNioFlow<>()) {
            String result = defaultNioFlow.just("payload", Map.of("traceId", "abc-123"))
                    .handle(x -> x + "/" + FlowContext.get("traceId"))
                    .join();

            assertEquals("payload/abc-123", result);
        }
    }

    @Test
    void contextCrossesAsyncBoundaries() {
        try (DefaultNioFlow<String> defaultNioFlow = new DefaultNioFlow<>()) {
            String result = defaultNioFlow.just("payload", Map.of("tenant", "acme"))
                    .submit(x -> x + "/" + FlowContext.get("tenant")) // runs on a virtual thread
                    .join();

            assertEquals("payload/acme", result);
        }
    }

    @Test
    void stagesCanWriteContextForLaterStages() {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            int result = defaultNioFlow.just(5)
                    .handle(x -> {
                        FlowContext.put("original", x);
                        return x * 100;
                    })
                    .submit(x -> x + (Integer) FlowContext.get("original"))
                    .join();

            assertEquals(505, result);
        }
    }

    @Test
    void eachValueCarriesItsOwnContext() {
        try (DefaultNioFlow<String> defaultNioFlow = new DefaultNioFlow<>()) {
            List<String> completed = new CopyOnWriteArrayList<>();
            defaultNioFlow.submit(x -> x + "/" + FlowContext.get("traceId"))
                    .onComplete(completed::add);

            defaultNioFlow.just("a", Map.of("traceId", "t-1"));
            defaultNioFlow.just("b", Map.of("traceId", "t-2"));
            defaultNioFlow.join();

            assertEquals(2, completed.size());
            assertTrue(completed.containsAll(List.of("a/t-1", "b/t-2")));
        }
    }

    @Test
    void fanOutChildrenInheritTheParentsContext() {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            List<String> completed = new CopyOnWriteArrayList<>();
            defaultNioFlow.fanOut(x -> List.of(x, x + 1))
                    .adapt(x -> x + "/" + FlowContext.get("traceId"))
                    .onComplete(completed::add);

            defaultNioFlow.just(1, Map.of("traceId", "parent"));
            defaultNioFlow.join();

            assertEquals(2, completed.size());
            assertTrue(completed.containsAll(List.of("1/parent", "2/parent")));
        }
    }

    @Test
    void aRecoveryRunsWithTheValuesContext() {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            defaultNioFlow.submit(x -> {
                        throw new IllegalStateException("boom");
                    })
                    .onErrorResume(error -> (Integer) FlowContext.get("fallback"));

            defaultNioFlow.just(1, Map.of("fallback", 42));

            assertEquals(42, defaultNioFlow.join());
        }
    }

    @Test
    void onCompleteSeesTheValuesContext() {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            List<Object> traces = new CopyOnWriteArrayList<>();
            defaultNioFlow.handle(x -> x * 2)
                    .onComplete(value -> traces.add(FlowContext.get("traceId")));

            defaultNioFlow.just(1, Map.of("traceId", "t-9"));
            defaultNioFlow.join();

            assertEquals(List.of("t-9"), traces);
        }
    }

    @Test
    void onErrorSeesTheFailingValuesContext() {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            List<Object> traces = new CopyOnWriteArrayList<>();
            defaultNioFlow.handle(x -> {
                        throw new IllegalStateException("boom");
                    })
                    .onError(error -> traces.add(FlowContext.get("traceId")));

            defaultNioFlow.just(1, Map.of("traceId", "t-err"));

            assertThrows(java.util.concurrent.CompletionException.class, defaultNioFlow::join);
            long deadline = System.nanoTime() + java.time.Duration.ofSeconds(5).toNanos();
            while (traces.isEmpty() && System.nanoTime() < deadline) {
                Thread.onSpinWait(); // delivery happens after the value is released
            }
            assertEquals(List.of("t-err"), traces);
        }
    }

    @Test
    void outsideAPipelineTheContextIsUnbound() {
        assertNull(FlowContext.get("anything"));
        assertThrows(IllegalStateException.class, () -> FlowContext.put("key", "value"));
    }
}
