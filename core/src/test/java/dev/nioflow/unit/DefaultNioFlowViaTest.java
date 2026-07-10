package dev.nioflow.unit;

import dev.nioflow.application.facade.DefaultNioFlow;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;
import java.util.function.UnaryOperator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DefaultNioFlowViaTest {

    /** A reusable segment: defined once, spliced anywhere. */
    private static UnaryOperator<dev.nioflow.core.facade.NioFlow<Integer>> doubleThenIncrement() {
        return p -> p
                .handle(x -> x * 2)
                .submit(x -> x + 1);
    }

    private static Function<dev.nioflow.core.facade.NioFlow<Integer>, dev.nioflow.core.facade.NioFlow<String>> stringify() {
        return p -> p
                .handle(x -> x * 10)
                .adapt(x -> "v" + x);
    }

    @Test
    void aSegmentSplicesItsStagesIntoTheChain() {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            int result = defaultNioFlow.just(5)
                    .via(doubleThenIncrement())
                    .handle(x -> x + 100)
                    .join();

            assertEquals(111, result); // (5 * 2 + 1) + 100
        }
    }

    @Test
    void aSegmentMayChangeTheType() {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            String result = defaultNioFlow.just(4)
                    .via(stringify())
                    .handle(String::toUpperCase)
                    .join();

            assertEquals("V40", result);
        }
    }

    @Test
    void theSameSegmentIsReusableAcrossPipelines() {
        try (DefaultNioFlow<Integer> first = new DefaultNioFlow<>();
             DefaultNioFlow<Integer> second = new DefaultNioFlow<>()) {
            UnaryOperator<dev.nioflow.core.facade.NioFlow<Integer>> segment = doubleThenIncrement();

            assertEquals(11, first.just(5).via(segment).join());
            assertEquals(21, second.just(10).via(segment).join());
        }
    }

    @Test
    void segmentsComposeInsideLanes() {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            List<Integer> completed = new CopyOnWriteArrayList<>();
            defaultNioFlow.when(x -> x > 10)
                    .then(lane -> lane
                            .via(doubleThenIncrement()))
                    .otherwise(lane -> lane
                            .handle(x -> x))
                    .onComplete(completed::add);

            defaultNioFlow.just(20); // true lane: through the segment -> 41
            defaultNioFlow.just(5);  // false lane: untouched
            defaultNioFlow.join();

            assertEquals(2, completed.size());
            assertTrue(completed.containsAll(List.of(41, 5)));
        }
    }

    @Test
    void segmentsNest() {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            UnaryOperator<dev.nioflow.core.facade.NioFlow<Integer>> outer = p -> p
                    .via(doubleThenIncrement())
                    .handle(x -> x + 1);

            int result = defaultNioFlow.just(5)
                    .via(outer)
                    .join();

            assertEquals(12, result); // (5 * 2 + 1) + 1
        }
    }
}
