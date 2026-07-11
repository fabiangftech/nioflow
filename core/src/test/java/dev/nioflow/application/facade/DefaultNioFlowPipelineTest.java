package dev.nioflow.application.facade;

import dev.nioflow.core.facade.NioFlow;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DefaultNioFlowPipelineTest {

    @Test
    void buildsAndExecutesAFullFlow() throws Exception {
        var effects = new CopyOnWriteArrayList<Object>();
        var backgroundRan = new CountDownLatch(1);
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class);

        String result = flow.just(5)
                .handle("plus-one", value -> value + 1)
                .background("audit", value -> {
                    effects.add(value);
                    backgroundRan.countDown();
                })
                .adapt(value -> "value:" + value)
                .execute();

        assertEquals("value:6", result);
        assertTrue(backgroundRan.await(1, TimeUnit.SECONDS));
        assertEquals(List.of(6), effects);
    }

    @Test
    void adaptChangesThePipelineType() {
        NioFlow<String, String> flow = DefaultNioFlow.from(String.class);

        Integer length = flow.just("hola mundo")
                .handle(String::toUpperCase)
                .adapt(String::length)
                .handle(value -> value * 2)
                .execute();

        assertEquals(20, length);
    }

    @Test
    void sharedAdaptTypesTheStepsAfterJust() {
        NioFlow<String, Integer> flow = DefaultNioFlow.from(String.class)
                .handle(String::trim)
                .adapt(String::length);

        Integer result = flow.just("  hola  ")
                .handle(value -> value * 2)
                .execute();

        assertEquals(8, result);
    }

    @Test
    void sharedDefinitionRunsBeforeExecutionLinks() {
        NioFlow<String, String> flow = DefaultNioFlow.from(String.class);
        flow.handle("trim", String::trim);

        String withLocal = flow.just("  hola  ")
                .handle(String::toUpperCase)
                .execute();
        String sharedOnly = flow.just("  mundo  ").execute();

        assertEquals("HOLA", withLocal);
        assertEquals("mundo", sharedOnly);
    }

    @Test
    void executeWithoutJustIsRejected() {
        NioFlow<String, String> flow = DefaultNioFlow.from(String.class);

        assertThrows(IllegalStateException.class, flow::execute);
    }
}
