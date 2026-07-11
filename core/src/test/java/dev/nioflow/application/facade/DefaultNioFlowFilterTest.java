package dev.nioflow.application.facade;

import dev.nioflow.core.facade.NioFlow;
import org.junit.jupiter.api.Test;

import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class DefaultNioFlowFilterTest {

    @Test
    void filterCutsThePerRequestExecution() {
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class);
        Function<Integer, Integer> route = input -> flow.just(input)
                .handle(value -> value + 1)
                .filter(value -> value > 10)
                .handle(value -> value * 2)
                .execute();

        assertEquals(42, route.apply(20)); // (20 + 1) passes the filter → * 2
        assertNull(route.apply(3));        // (3 + 1) rejected: cuts without running the tail
    }

    @Test
    void filterOnSharedDefinitionAppliesToEveryExecution() {
        NioFlow<String, String> flow = DefaultNioFlow.from(String.class);
        flow.filter(value -> !value.isBlank());

        assertEquals("HOLA", flow.just("hola").handle(String::toUpperCase).execute());
        assertNull(flow.just("   ").handle(String::toUpperCase).execute());
    }

    @Test
    void filterInsideALaneOnlyCutsValuesRoutedThroughThatLane() {
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class);
        flow.when(value -> value % 2 == 0)
                .then(lane -> lane
                        .filter(value -> value > 10)
                        .handle(value -> value * 10))
                .otherwise(lane -> lane
                        .handle(value -> -value));

        assertEquals(200, flow.just(20).execute()); // even and big: passes the lane's filter
        assertNull(flow.just(4).execute());         // even and small: the lane's filter cuts
        assertEquals(-3, flow.just(3).execute());   // odd: the other lane's filter does not apply
    }
}
