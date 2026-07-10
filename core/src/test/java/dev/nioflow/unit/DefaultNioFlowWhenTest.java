package dev.nioflow.unit;

import dev.nioflow.application.facade.DefaultNioFlow;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DefaultNioFlowWhenTest {

    @Test
    void thenLaneRunsWhenThePredicateHolds() {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            int result = defaultNioFlow.just(20)
                    .when(x -> x > 10)
                    .then(lane -> lane
                            .handle(x -> x * 2))
                    .otherwise(lane -> lane
                            .handle(x -> x - 1))
                    .join();

            assertEquals(40, result);
        }
    }

    @Test
    void otherwiseLaneRunsWhenThePredicateFails() {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            int result = defaultNioFlow.just(5)
                    .when(x -> x > 10)
                    .then(lane -> lane
                            .handle(x -> x * 2))
                    .otherwise(lane -> lane
                            .handle(x -> x - 1))
                    .join();

            assertEquals(4, result);
        }
    }

    @Test
    void lanesHoldSeveralStagesAndStayIsolated() {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            List<Integer> thenLane = new CopyOnWriteArrayList<>();
            List<Integer> otherwiseLane = new CopyOnWriteArrayList<>();
            defaultNioFlow.when(x -> x > 10)
                    .then(lane -> lane
                            .handle(x -> x * 2)
                            .handle(x -> x + 1)
                            .submit(x -> x + 1)
                            .handle(x -> {
                                thenLane.add(x);
                                return x;
                            }))
                    .otherwise(lane -> lane
                            .handle(x -> x - 1)
                            .handle(x -> {
                                otherwiseLane.add(x);
                                return x;
                            }));

            defaultNioFlow.just(20);
            defaultNioFlow.just(5);
            defaultNioFlow.join();

            assertEquals(List.of(42), thenLane);
            assertEquals(List.of(4), otherwiseLane);
        }
    }

    @Test
    void mainLineContinuesForBothLanesAfterTheFork() {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            List<Integer> merged = new CopyOnWriteArrayList<>();
            defaultNioFlow.when(x -> x > 10)
                    .then(lane -> lane
                            .handle(x -> x * 2))
                    .otherwise(lane -> lane
                            .handle(x -> x - 1))
                    .handle(x -> {
                        merged.add(x);
                        return x;
                    });

            defaultNioFlow.just(20);
            defaultNioFlow.just(5);
            defaultNioFlow.join();

            assertEquals(2, merged.size());
            assertTrue(merged.containsAll(List.of(40, 4)));
        }
    }

    @Test
    void withoutOtherwiseFalseValuesPassThroughUnchanged() {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            List<Integer> results = new CopyOnWriteArrayList<>();
            defaultNioFlow.when(x -> x > 10)
                    .then(lane -> lane
                            .handle(x -> x * 2)
                            .handle(x -> x + 1))
                    .handle(x -> {
                        results.add(x);
                        return x;
                    });

            defaultNioFlow.just(20);
            defaultNioFlow.just(5);
            defaultNioFlow.join();

            assertEquals(2, results.size());
            assertTrue(results.containsAll(List.of(41, 5)));
        }
    }

    @Test
    void forksCanBeNestedInsideALane() {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            List<Integer> results = new CopyOnWriteArrayList<>();
            defaultNioFlow.when(x -> x > 0)
                    .then(outer -> outer
                            .when(x -> x > 10)
                            .then(inner -> inner
                                    .handle(x -> x * 2))
                            .otherwise(inner -> inner
                                    .handle(x -> x + 1)))
                    .otherwise(lane -> lane
                            .handle(x -> 0))
                    .handle(x -> {
                        results.add(x);
                        return x;
                    });

            defaultNioFlow.just(20);  // outer true, inner true  -> 40
            defaultNioFlow.just(5);   // outer true, inner false -> 6
            defaultNioFlow.just(-3);  // outer false             -> 0
            defaultNioFlow.join();

            assertEquals(3, results.size());
            assertTrue(results.containsAll(List.of(40, 6, 0)));
        }
    }

    @Test
    void sequentialForksDecideIndependently() {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            List<Integer> results = new CopyOnWriteArrayList<>();
            defaultNioFlow.when(x -> x > 0)
                    .then(lane -> lane
                            .handle(x -> x + 10))
                    .when(x -> x % 2 == 0)
                    .then(lane -> lane
                            .handle(x -> x * 2))
                    .handle(x -> {
                        results.add(x);
                        return x;
                    });

            defaultNioFlow.just(5);   // positive -> 15, odd -> stays 15
            defaultNioFlow.just(-4);  // skips +10, even -> -8
            defaultNioFlow.join();

            assertEquals(2, results.size());
            assertTrue(results.containsAll(List.of(15, -8)));
        }
    }

    @Test
    void emptyLanesPassValuesThroughUnchanged() {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            List<Integer> results = new CopyOnWriteArrayList<>();
            defaultNioFlow.when(x -> x > 10)
                    .then(lane -> lane)
                    .otherwise(lane -> lane)
                    .handle(x -> {
                        results.add(x);
                        return x;
                    });

            defaultNioFlow.just(20);
            defaultNioFlow.just(5);
            defaultNioFlow.join();

            assertEquals(2, results.size());
            assertTrue(results.containsAll(List.of(20, 5)));
        }
    }

    @Test
    void aForkDeclaredAfterInjectionStillRoutesParkedValues() {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            List<Integer> results = new CopyOnWriteArrayList<>();
            defaultNioFlow.just(20);
            defaultNioFlow.just(5);

            defaultNioFlow.when(x -> x > 10)
                    .then(lane -> lane
                            .handle(x -> x * 2))
                    .otherwise(lane -> lane
                            .handle(x -> x - 1))
                    .handle(x -> {
                        results.add(x);
                        return x;
                    });
            defaultNioFlow.join();

            assertEquals(2, results.size());
            assertTrue(results.containsAll(List.of(40, 4)));
        }
    }

    @Test
    void thePredicateIsEvaluatedExactlyOncePerValue() {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            AtomicInteger evaluations = new AtomicInteger();
            defaultNioFlow.just(20); // parks and re-parks across the appends below

            defaultNioFlow.when(x -> {
                        evaluations.incrementAndGet();
                        return x > 10;
                    })
                    .then(lane -> lane
                            .handle(x -> x * 2))
                    .otherwise(lane -> lane
                            .handle(x -> x - 1))
                    .handle(x -> x);

            assertEquals(40, defaultNioFlow.join());
            assertEquals(1, evaluations.get(), "the decision must be recorded once, not re-evaluated");
        }
    }

    @Test
    void adaptInsideALaneKeepsItsGuards() {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            List<Integer> laneResults = new CopyOnWriteArrayList<>();
            defaultNioFlow.when(x -> x > 10)
                    .then(lane -> lane
                            .adapt(x -> x + 1000)
                            .handle(x -> {
                                laneResults.add(x);
                                return x;
                            }))
                    .otherwise(lane -> lane
                            .handle(x -> x));

            defaultNioFlow.just(20); // true lane: adapted and collected
            defaultNioFlow.just(5);  // false lane: must never reach the lane collector
            defaultNioFlow.join();

            assertEquals(List.of(1020), laneResults);
        }
    }

    @Test
    void lanesRunTheirOwnSubmitStages() {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            List<String> lanes = new CopyOnWriteArrayList<>();
            defaultNioFlow.when(x -> x > 10)
                    .then(lane -> lane
                            .submit(x -> {
                                lanes.add("then-" + x);
                                return x;
                            }))
                    .otherwise(lane -> lane
                            .submit(x -> {
                                lanes.add("otherwise-" + x);
                                return x;
                            }));

            defaultNioFlow.just(20);
            defaultNioFlow.just(5);
            defaultNioFlow.join();

            assertEquals(2, lanes.size());
            assertTrue(lanes.contains("then-20"));
            assertTrue(lanes.contains("otherwise-5"));
        }
    }
}
