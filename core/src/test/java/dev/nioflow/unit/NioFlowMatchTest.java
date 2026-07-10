package dev.nioflow.unit;

import dev.nioflow.application.facade.NioFlow;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NioFlowMatchTest {

    @Test
    void eachValueTakesItsFirstMatchingCase() {
        try (NioFlow<Integer> nioFlow = new NioFlow<>()) {
            List<Integer> big = new CopyOnWriteArrayList<>();
            List<Integer> medium = new CopyOnWriteArrayList<>();
            List<Integer> small = new CopyOnWriteArrayList<>();
            nioFlow.match()
                    .is(x -> x > 100, lane -> lane
                            .handle(x -> {
                                big.add(x);
                                return x;
                            }))
                    .is(x -> x > 10, lane -> lane
                            .handle(x -> {
                                medium.add(x);
                                return x;
                            }))
                    .otherwise(lane -> lane
                            .handle(x -> {
                                small.add(x);
                                return x;
                            }));

            nioFlow.justAll(List.of(500, 50, 5));
            nioFlow.join();

            assertEquals(List.of(500), big);   // > 100 matched first, never reached "medium"
            assertEquals(List.of(50), medium);
            assertEquals(List.of(5), small);
        }
    }

    @Test
    void predicatesAfterTheMatchingCaseAreNotEvaluated() {
        try (NioFlow<Integer> nioFlow = new NioFlow<>()) {
            AtomicInteger secondEvaluations = new AtomicInteger();
            nioFlow.match()
                    .is(x -> x > 100, lane -> lane
                            .handle(x -> x))
                    .is(x -> {
                        secondEvaluations.incrementAndGet();
                        return x > 10;
                    }, lane -> lane
                            .handle(x -> x));

            nioFlow.just(500); // matches the first case: the second predicate must not run
            nioFlow.just(50);  // reaches and matches the second case

            nioFlow.join();
            assertEquals(1, secondEvaluations.get());
        }
    }

    @Test
    void unmatchedValuesPassThroughWithoutOtherwise() {
        try (NioFlow<Integer> nioFlow = new NioFlow<>()) {
            List<Integer> merged = new CopyOnWriteArrayList<>();
            nioFlow.match()
                    .is(x -> x > 100, lane -> lane
                            .handle(x -> x * 2))
                    .handle(x -> {
                        merged.add(x);
                        return x;
                    });

            nioFlow.justAll(List.of(500, 5));
            nioFlow.join();

            assertEquals(2, merged.size());
            assertTrue(merged.containsAll(List.of(1000, 5)));
        }
    }

    @Test
    void theMainLineContinuesForEveryValueAfterTheMatch() {
        try (NioFlow<Integer> nioFlow = new NioFlow<>()) {
            List<Integer> merged = new CopyOnWriteArrayList<>();
            nioFlow.match()
                    .is(x -> x > 100, lane -> lane
                            .submit(x -> x + 1))
                    .is(x -> x > 10, lane -> lane
                            .handle(x -> x + 2))
                    .otherwise(lane -> lane
                            .handle(x -> x + 3))
                    .handle(x -> {
                        merged.add(x);
                        return x;
                    });

            nioFlow.justAll(List.of(500, 50, 5));
            nioFlow.join();

            assertEquals(3, merged.size());
            assertTrue(merged.containsAll(List.of(501, 52, 8)));
        }
    }

    @Test
    void aMatchNestsInsideAWhenLane() {
        try (NioFlow<Integer> nioFlow = new NioFlow<>()) {
            List<String> routed = new CopyOnWriteArrayList<>();
            nioFlow.when(x -> x > 0)
                    .then(lane -> lane
                            .match()
                            .is(x -> x > 100, inner -> inner
                                    .handle(x -> {
                                        routed.add("big-" + x);
                                        return x;
                                    }))
                            .otherwise(inner -> inner
                                    .handle(x -> {
                                        routed.add("small-" + x);
                                        return x;
                                    })))
                    .otherwise(lane -> lane
                            .handle(x -> {
                                routed.add("negative-" + x);
                                return x;
                            }));

            nioFlow.justAll(List.of(500, 5, -3));
            nioFlow.join();

            assertEquals(3, routed.size());
            assertTrue(routed.containsAll(List.of("big-500", "small-5", "negative--3")));
        }
    }

    @Test
    void caseLanesCanHoldTheirOwnAsyncStages() {
        try (NioFlow<Integer> nioFlow = new NioFlow<>()) {
            List<Integer> completed = new CopyOnWriteArrayList<>();
            nioFlow.match()
                    .is(x -> x % 2 == 0, lane -> lane
                            .submit(x -> x * 10)
                            .handle(x -> x + 1))
                    .otherwise(lane -> lane
                            .submit(x -> -x))
                    .onComplete(completed::add);

            nioFlow.justAll(List.of(2, 3));
            nioFlow.join();

            assertEquals(2, completed.size());
            assertTrue(completed.containsAll(List.of(21, -3)));
        }
    }
}
