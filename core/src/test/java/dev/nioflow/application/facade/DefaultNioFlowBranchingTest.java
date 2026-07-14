package dev.nioflow.application.facade;

import dev.nioflow.core.facade.NioFlow;
import org.junit.jupiter.api.Test;

import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DefaultNioFlowBranchingTest {

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
    void nestedBranchesComposeGuards() {
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
        assertEquals(32, route.apply(2)); // small: the inner branch is never evaluated
    }
}
