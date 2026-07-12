package dev.nioflow.application.facade;

import dev.nioflow.core.facade.NioFlow;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
    void justStartsAtTheInputTypeEvenWithASharedChain() {
        // The shared definition is type-preserving (String in, String out), so
        // the per-request pipeline starts at the INPUT type: the first step
        // chained after just() receives exactly what just() was handed.
        NioFlow<String, Integer> flow = DefaultNioFlow.<String, Integer>from(String.class)
                .handle("trim", String::trim);

        Integer result = flow.just("  hola  ")
                .adapt(String::length)              // String here, Integer from now on
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

    // Executing without just() used to throw at runtime; the split between
    // NioFlow (the definition) and NioStep (the per-request pipeline) makes it
    // a compile error, so there is nothing left to assert here.
}
