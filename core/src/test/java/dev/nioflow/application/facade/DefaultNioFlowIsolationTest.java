package dev.nioflow.application.facade;

import dev.nioflow.core.facade.NioFlow;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DefaultNioFlowIsolationTest {

    @Test
    void executeIsIsolatedPerThread() {
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class);
        flow.handle("double", value -> value * 2);

        var results = new ConcurrentHashMap<Integer, Integer>();
        try (var requests = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int i = 0; i < 50; i++) {
                int input = i;
                requests.execute(() -> results.put(input, flow.just(input).execute()));
            }
        }

        for (int i = 0; i < 50; i++) {
            assertEquals(i * 2, results.get(i));
        }
    }

    @Test
    void repeatedExecutionsAreIndependent() {
        NioFlow<String, String> flow = DefaultNioFlow.from(String.class);

        String first = flow.just("hola")
                .handle(String::toUpperCase)
                .execute();
        String second = flow.just("mundo")
                .handle(String::toUpperCase)
                .execute();

        assertEquals("HOLA", first);
        assertEquals("MUNDO", second);
        // handle() calls from previous executions must not pollute the shared definition.
        assertEquals("intacto", flow.just("intacto").execute());
    }
}
