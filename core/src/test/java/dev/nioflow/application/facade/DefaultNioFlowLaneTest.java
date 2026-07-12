package dev.nioflow.application.facade;

import dev.nioflow.core.facade.NioFlow;
import dev.nioflow.core.facade.Segment;
import dev.nioflow.core.model.RateLimit;
import dev.nioflow.core.model.Retry;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A lane is a guarded view of the same chain: every step kind is available
 * inside it, and everything declared there only runs for the values the fork
 * routed in. These tests pin that down for the step kinds and for a fork
 * nested inside a lane.
 */
class DefaultNioFlowLaneTest {

    private static final Duration GENEROUS = Duration.ofSeconds(2);
    private static final Retry TWICE = Retry.of(2, Duration.ofMillis(1));

    @Test
    void everyStepKindInsideALaneOnlyRunsForTheRoutedValues() {
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class);
        List<Integer> effects = new CopyOnWriteArrayList<>();

        List<Function<Integer, Integer>> split = List.of(value -> value, value -> 0);
        Segment<Integer, Integer> plusSegment = lane -> lane.handle(value -> value + 2048);

        flow.when(value -> value > 10)
                .then(lane -> lane
                        .handle(value -> value + 1)
                        .handle("named", value -> value + 2)
                        .handle("timed", value -> value + 4, GENEROUS)
                        .handle("retried", value -> value + 8, TWICE)
                        .handle("timedRetried", value -> value + 16, GENEROUS, TWICE)
                        .handle("limited", value -> value + 32, RateLimit.perSecond(1_000))
                        .handleContextual((value, context) -> value + 64)
                        .handleContextual("contextual", (value, context) -> value + 128)
                        .handleSync(value -> value + 256)
                        .handleSync("sync", value -> value + 512)
                        .background(effects::add)
                        .background("effect", effects::add)
                        .filter(value -> true)
                        .fanOut(split, parts -> parts.get(0) + parts.get(1))
                        .fanOut("fan", split, parts -> parts.get(0) + parts.get(1))
                        .batch(1, Duration.ofMillis(50), values -> values)
                        .batch("bulk", 1, Duration.ofMillis(50), values -> values)
                        .adapt(value -> value + 1024)
                        .use(plusSegment)
                        .use("laneRegion", lane2 -> lane2.handle(value -> value + 4096))
                        .recover(error -> -1)
                        .recover("recovery", error -> -1));

        // 1023 from the handles + 1024 (adapt) + 2048 + 4096 from the segments.
        assertEquals(8211, flow.just(20).execute());
        assertEquals(3, flow.just(3).execute());          // routed away: none of it ran
        assertTrue(effects.contains(1043), () -> "background effects ran: " + effects);
    }

    @Test
    void whenNestedInsideALaneComposesTheGuards() {
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class);

        flow.when(value -> value > 10)
                .then(lane -> lane
                        .handle("double", value -> value * 2)
                        .when(value -> value % 4 == 0)
                        .then(inner -> inner.handle("quarter", value -> value / 4))
                        .otherwise(inner -> inner.handle("tag", value -> value + 1000))
                        .handle("laneTail", value -> value + 1));

        assertEquals(11, flow.just(20).execute());     // 40, divisible by 4: 10, + 1 (lane tail)
        assertEquals(1043, flow.just(21).execute());   // 42, not divisible: + 1000, + 1
        assertEquals(3, flow.just(3).execute());       // never enters the lane
    }

    @Test
    void matchNestedInsideALaneIsFirstMatchWins() {
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class);

        flow.when(value -> value > 10)
                .then(lane -> lane
                        .match()
                        .is(value -> value % 2 == 0, inner -> inner.handle("even", value -> value * 10))
                        .is(value -> value % 3 == 0, inner -> inner.handle("byThree", value -> value * 100))
                        .otherwise(inner -> inner.handle("rest", value -> value - 1))
                        .handle("laneTail", value -> value + 1));

        assertEquals(201, flow.just(20).execute());    // even wins over the multiple-of-3 case
        assertEquals(2101, flow.just(21).execute());   // odd, multiple of 3
        assertEquals(25, flow.just(25).execute());      // neither case: otherwise (25 - 1), + 1 (lane tail)
        assertEquals(6, flow.just(6).execute());       // outside the lane: untouched by all of it
    }

    @Test
    void aLaneRecoveryCatchesTheFailuresOfItsOwnLaneOnly() {
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class);

        flow.when(value -> value > 10)
                .then(lane -> lane
                        .handle("boom", value -> {
                            throw new IllegalStateException("boom");
                        })
                        .recover(error -> -1));

        assertEquals(-1, flow.just(20).execute());   // failed inside the lane, recovered inside the lane
        assertEquals(3, flow.just(3).execute());     // never entered the lane, so nothing failed
    }
}
