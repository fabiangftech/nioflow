package dev.nioflow.application.facade;

import dev.nioflow.core.facade.NioFlow;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DefaultNioFlowTest {

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
    void executeIsIsolatedPerThread() throws Exception {
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
    void whenRoutesEachExecutionThroughItsLane() {
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class);
        Function<Integer, Integer> route = input -> flow.just(input)
                .when(value -> value > 10)
                .then(lane -> lane
                        .handle(value -> value * 2))
                .otherwise(lane -> lane
                        .handle(value -> value - 1))
                .handle(value -> value + 100)
                .execute();

        assertEquals(184, route.apply(42)); // (42 * 2) + 100: only the then lane
        assertEquals(102, route.apply(3));  // (3 - 1) + 100: only the otherwise lane
    }

    @Test
    void whenWithoutOtherwiseSkipsTheLaneAndContinuesTheMainLine() {
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class);

        Integer result = flow.just(3)
                .when(value -> value > 10)
                .then(lane -> lane
                        .handle(value -> value * 1000))
                .handle(value -> value + 1)
                .execute();

        assertEquals(4, result);
    }

    @Test
    void whenOnSharedDefinitionRoutesPerRequest() {
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class);
        flow.when(value -> value % 2 == 0)
                .then(lane -> lane
                        .handle("even", value -> value * 10))
                .otherwise(lane -> lane
                        .handle("odd", value -> value * -1));

        assertEquals(40, flow.just(4).execute());
        assertEquals(-7, flow.just(7).execute());
    }

    @Test
    void matchFirstMatchingCaseWins() {
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class);
        Function<Integer, Integer> route = input -> flow.just(input)
                .match()
                .is(value -> value % 2 == 0, lane -> lane
                        .handle(value -> value * 2))
                .is(value -> value > 10, lane -> lane
                        .handle(value -> value + 1000))
                .otherwise(lane -> lane
                        .handle(value -> -value))
                .execute();

        assertEquals(40, route.apply(20));   // even AND > 10: first case wins, second never runs
        assertEquals(1015, route.apply(15)); // odd and > 10: second case
        assertEquals(-3, route.apply(3));    // no case: otherwise
    }

    @Test
    void matchWithoutOtherwiseFallsThroughToTheMainLine() {
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class);

        Integer result = flow.just(3)
                .match()
                .is(value -> value > 10, lane -> lane
                        .handle(value -> value * 1000))
                .handle(value -> value + 1)
                .execute();

        assertEquals(4, result);
    }

    @Test
    void nestedForksComposeGuards() {
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class);
        Function<Integer, Integer> route = input -> flow.just(input)
                .when(value -> value > 5)
                .then(lane -> lane
                        .when(value -> value % 2 == 0)
                        .then(inner -> inner
                                .handle(value -> value + 10))
                        .otherwise(inner -> inner
                                .handle(value -> value + 20)))
                .otherwise(lane -> lane
                        .handle(value -> value + 30))
                .execute();

        assertEquals(16, route.apply(6)); // big and even
        assertEquals(27, route.apply(7)); // big and odd
        assertEquals(32, route.apply(2)); // small: the inner fork is never evaluated
    }

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

    @Test
    void executeWithoutJustIsRejected() {
        NioFlow<String, String> flow = DefaultNioFlow.from(String.class);

        assertThrows(IllegalStateException.class, flow::execute);
    }
}
