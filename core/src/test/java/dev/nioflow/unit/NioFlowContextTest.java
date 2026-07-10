package dev.nioflow.unit;

import dev.nioflow.application.facade.NioFlow;
import dev.nioflow.core.model.FlowContext;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.*;

class NioFlowContextTest {

    @Test
    void seededContextIsVisibleInSyncStages() {
        try (NioFlow<String> nioFlow = new NioFlow<>()) {
            String result = nioFlow.just("payload", Map.of("traceId", "abc-123"))
                    .handle(x -> x + "/" + FlowContext.get("traceId"))
                    .join();

            assertEquals("payload/abc-123", result);
        }
    }

    @Test
    void contextCrossesAsyncBoundaries() {
        try (NioFlow<String> nioFlow = new NioFlow<>()) {
            String result = nioFlow.just("payload", Map.of("tenant", "acme"))
                    .submit(x -> x + "/" + FlowContext.get("tenant")) // runs on a virtual thread
                    .join();

            assertEquals("payload/acme", result);
        }
    }

    @Test
    void stagesCanWriteContextForLaterStages() {
        try (NioFlow<Integer> nioFlow = new NioFlow<>()) {
            int result = nioFlow.just(5)
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
        try (NioFlow<String> nioFlow = new NioFlow<>()) {
            List<String> completed = new CopyOnWriteArrayList<>();
            nioFlow.submit(x -> x + "/" + FlowContext.get("traceId"))
                    .onComplete(completed::add);

            nioFlow.just("a", Map.of("traceId", "t-1"));
            nioFlow.just("b", Map.of("traceId", "t-2"));
            nioFlow.join();

            assertEquals(2, completed.size());
            assertTrue(completed.containsAll(List.of("a/t-1", "b/t-2")));
        }
    }

    @Test
    void fanOutChildrenInheritTheParentsContext() {
        try (NioFlow<Integer> nioFlow = new NioFlow<>()) {
            List<String> completed = new CopyOnWriteArrayList<>();
            nioFlow.fanOut(x -> List.of(x, x + 1))
                    .adapt(x -> x + "/" + FlowContext.get("traceId"))
                    .onComplete(completed::add);

            nioFlow.just(1, Map.of("traceId", "parent"));
            nioFlow.join();

            assertEquals(2, completed.size());
            assertTrue(completed.containsAll(List.of("1/parent", "2/parent")));
        }
    }

    @Test
    void aRecoveryRunsWithTheValuesContext() {
        try (NioFlow<Integer> nioFlow = new NioFlow<>()) {
            nioFlow.submit(x -> {
                        throw new IllegalStateException("boom");
                    })
                    .onErrorResume(error -> (Integer) FlowContext.get("fallback"));

            nioFlow.just(1, Map.of("fallback", 42));

            assertEquals(42, nioFlow.join());
        }
    }

    @Test
    void onCompleteSeesTheValuesContext() {
        try (NioFlow<Integer> nioFlow = new NioFlow<>()) {
            List<Object> traces = new CopyOnWriteArrayList<>();
            nioFlow.handle(x -> x * 2)
                    .onComplete(value -> traces.add(FlowContext.get("traceId")));

            nioFlow.just(1, Map.of("traceId", "t-9"));
            nioFlow.join();

            assertEquals(List.of("t-9"), traces);
        }
    }

    @Test
    void outsideAPipelineTheContextIsUnbound() {
        assertNull(FlowContext.get("anything"));
        assertThrows(IllegalStateException.class, () -> FlowContext.put("key", "value"));
    }
}
