package dev.nioflow.application.facade;

import dev.nioflow.core.facade.NioFlow;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DefaultNioFlowTest {

    @Test
    void buildsAndExecutesAFullFlow() throws Exception {
        try (NioFlow flow = new DefaultNioFlow()) {
            var effects = new CopyOnWriteArrayList<Object>();
            var backgroundRan = new CountDownLatch(1);

            flow.handle("plus-one", (Integer value) -> value + 1)
                    .background("audit", value -> {
                        effects.add(value);
                        backgroundRan.countDown();
                    })
                    .adapt((Integer value) -> "value:" + value);

            flow.just(5);
            String result = flow.execute();

            assertEquals("value:6", result);
            assertTrue(backgroundRan.await(1, TimeUnit.SECONDS));
            assertEquals(List.of(6), effects);
        }
    }

    @Test
    void executeIsIsolatedPerThread() throws Exception {
        try (NioFlow flow = new DefaultNioFlow()) {
            flow.handle("double", (Integer value) -> value * 2);

            var results = new ConcurrentHashMap<Integer, Integer>();
            try (var requests = Executors.newVirtualThreadPerTaskExecutor()) {
                for (int i = 0; i < 50; i++) {
                    int input = i;
                    requests.execute(() -> {
                        flow.just(input);
                        results.put(input, flow.execute());
                    });
                }
            }

            for (int i = 0; i < 50; i++) {
                assertEquals(i * 2, results.get(i));
            }
        }
    }

    @Test
    void executeSealsTheDefinition() throws Exception {
        try (NioFlow flow = new DefaultNioFlow()) {
            flow.handle("noop", (Object value) -> value);

            flow.just("x");
            flow.execute();

            assertThrows(IllegalStateException.class, () -> flow.handle("late", (Object value) -> value));
        }
    }
}
