package dev.nioflow.unit;

import dev.nioflow.application.facade.NioFlow;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NioFlowWhenTest {

    @Test
    void thenLaneRunsWhenThePredicateHolds() {
        try (NioFlow<Integer> pipeline = new NioFlow<>()) {
            int result = pipeline.just(20)
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
        try (NioFlow<Integer> pipeline = new NioFlow<>()) {
            int result = pipeline.just(5)
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
        try (NioFlow<Integer> pipeline = new NioFlow<>()) {
            List<Integer> thenLane = new CopyOnWriteArrayList<>();
            List<Integer> otherwiseLane = new CopyOnWriteArrayList<>();
            pipeline.when(x -> x > 10)
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

            pipeline.just(20);
            pipeline.just(5);
            pipeline.join();

            assertEquals(List.of(42), thenLane);
            assertEquals(List.of(4), otherwiseLane);
        }
    }

    @Test
    void mainLineContinuesForBothLanesAfterTheFork() {
        try (NioFlow<Integer> pipeline = new NioFlow<>()) {
            List<Integer> merged = new CopyOnWriteArrayList<>();
            pipeline.when(x -> x > 10)
                    .then(lane -> lane
                            .handle(x -> x * 2))
                    .otherwise(lane -> lane
                            .handle(x -> x - 1))
                    .handle(x -> {
                        merged.add(x);
                        return x;
                    });

            pipeline.just(20);
            pipeline.just(5);
            pipeline.join();

            assertEquals(2, merged.size());
            assertTrue(merged.containsAll(List.of(40, 4)));
        }
    }

    @Test
    void withoutOtherwiseFalseValuesPassThroughUnchanged() {
        try (NioFlow<Integer> pipeline = new NioFlow<>()) {
            List<Integer> results = new CopyOnWriteArrayList<>();
            pipeline.when(x -> x > 10)
                    .then(lane -> lane
                            .handle(x -> x * 2)
                            .handle(x -> x + 1))
                    .handle(x -> {
                        results.add(x);
                        return x;
                    });

            pipeline.just(20);
            pipeline.just(5);
            pipeline.join();

            assertEquals(2, results.size());
            assertTrue(results.containsAll(List.of(41, 5)));
        }
    }

    @Test
    void forksCanBeNestedInsideALane() {
        try (NioFlow<Integer> pipeline = new NioFlow<>()) {
            List<Integer> results = new CopyOnWriteArrayList<>();
            pipeline.when(x -> x > 0)
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

            pipeline.just(20);  // outer true, inner true  -> 40
            pipeline.just(5);   // outer true, inner false -> 6
            pipeline.just(-3);  // outer false             -> 0
            pipeline.join();

            assertEquals(3, results.size());
            assertTrue(results.containsAll(List.of(40, 6, 0)));
        }
    }

    @Test
    void sequentialForksDecideIndependently() {
        try (NioFlow<Integer> pipeline = new NioFlow<>()) {
            List<Integer> results = new CopyOnWriteArrayList<>();
            pipeline.when(x -> x > 0)
                    .then(lane -> lane
                            .handle(x -> x + 10))
                    .when(x -> x % 2 == 0)
                    .then(lane -> lane
                            .handle(x -> x * 2))
                    .handle(x -> {
                        results.add(x);
                        return x;
                    });

            pipeline.just(5);   // positive -> 15, odd -> stays 15
            pipeline.just(-4);  // skips +10, even -> -8
            pipeline.join();

            assertEquals(2, results.size());
            assertTrue(results.containsAll(List.of(15, -8)));
        }
    }

    @Test
    void emptyLanesPassValuesThroughUnchanged() {
        try (NioFlow<Integer> pipeline = new NioFlow<>()) {
            List<Integer> results = new CopyOnWriteArrayList<>();
            pipeline.when(x -> x > 10)
                    .then(lane -> lane)
                    .otherwise(lane -> lane)
                    .handle(x -> {
                        results.add(x);
                        return x;
                    });

            pipeline.just(20);
            pipeline.just(5);
            pipeline.join();

            assertEquals(2, results.size());
            assertTrue(results.containsAll(List.of(20, 5)));
        }
    }

    @Test
    void aForkDeclaredAfterInjectionStillRoutesParkedValues() {
        try (NioFlow<Integer> pipeline = new NioFlow<>()) {
            List<Integer> results = new CopyOnWriteArrayList<>();
            pipeline.just(20);
            pipeline.just(5);

            pipeline.when(x -> x > 10)
                    .then(lane -> lane
                            .handle(x -> x * 2))
                    .otherwise(lane -> lane
                            .handle(x -> x - 1))
                    .handle(x -> {
                        results.add(x);
                        return x;
                    });
            pipeline.join();

            assertEquals(2, results.size());
            assertTrue(results.containsAll(List.of(40, 4)));
        }
    }

    @Test
    void thePredicateIsEvaluatedExactlyOncePerValue() {
        try (NioFlow<Integer> pipeline = new NioFlow<>()) {
            AtomicInteger evaluations = new AtomicInteger();
            pipeline.just(20); // parks and re-parks across the appends below

            pipeline.when(x -> {
                        evaluations.incrementAndGet();
                        return x > 10;
                    })
                    .then(lane -> lane
                            .handle(x -> x * 2))
                    .otherwise(lane -> lane
                            .handle(x -> x - 1))
                    .handle(x -> x);

            assertEquals(40, pipeline.join());
            assertEquals(1, evaluations.get(), "the decision must be recorded once, not re-evaluated");
        }
    }

    @Test
    void adaptInsideALaneKeepsItsGuards() {
        try (NioFlow<Integer> pipeline = new NioFlow<>()) {
            List<Integer> laneResults = new CopyOnWriteArrayList<>();
            pipeline.when(x -> x > 10)
                    .then(lane -> lane
                            .adapt(x -> x + 1000)
                            .handle(x -> {
                                laneResults.add(x);
                                return x;
                            }))
                    .otherwise(lane -> lane
                            .handle(x -> x));

            pipeline.just(20); // true lane: adapted and collected
            pipeline.just(5);  // false lane: must never reach the lane collector
            pipeline.join();

            assertEquals(List.of(1020), laneResults);
        }
    }

    @Test
    void lanesRunTheirOwnSubmitStages() {
        try (NioFlow<Integer> pipeline = new NioFlow<>()) {
            List<String> lanes = new CopyOnWriteArrayList<>();
            pipeline.when(x -> x > 10)
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

            pipeline.just(20);
            pipeline.just(5);
            pipeline.join();

            assertEquals(2, lanes.size());
            assertTrue(lanes.contains("then-20"));
            assertTrue(lanes.contains("otherwise-5"));
        }
    }
}
