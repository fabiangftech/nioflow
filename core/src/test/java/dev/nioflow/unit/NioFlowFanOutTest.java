package dev.nioflow.unit;

import dev.nioflow.application.facade.NioFlow;
import dev.nioflow.core.model.Backpressure;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class NioFlowFanOutTest {

    @Test
    void eachElementContinuesAsItsOwnValue() {
        try (NioFlow<Integer> nioFlow = new NioFlow<>()) {
            List<Integer> completed = new CopyOnWriteArrayList<>();
            nioFlow.fanOut(x -> List.of(x, x + 1, x + 2))
                    .handle(x -> x * 10)
                    .onComplete(completed::add);

            nioFlow.just(1);
            nioFlow.join();

            assertEquals(3, completed.size());
            assertTrue(completed.containsAll(List.of(10, 20, 30)));
        }
    }

    @Test
    void fanOutChangesThePipelineType() {
        try (NioFlow<Integer> nioFlow = new NioFlow<>()) {
            List<String> completed = new CopyOnWriteArrayList<>();
            nioFlow.fanOut(x -> List.of("a" + x, "b" + x))
                    .onComplete(completed::add);

            nioFlow.just(7);
            nioFlow.join();

            assertEquals(2, completed.size());
            assertTrue(completed.containsAll(List.of("a7", "b7")));
        }
    }

    @Test
    void anEmptyListDropsTheValueLikeAFilter() {
        try (NioFlow<Integer> nioFlow = new NioFlow<>()) {
            List<Integer> completed = new CopyOnWriteArrayList<>();
            List<Throwable> errors = new CopyOnWriteArrayList<>();
            nioFlow.<Integer>fanOut(x -> List.of())
                    .onComplete(completed::add)
                    .onError(errors::add);

            nioFlow.just(1);

            assertNull(nioFlow.join());
            assertTrue(completed.isEmpty());
            assertTrue(errors.isEmpty());
        }
    }

    @Test
    void childrenInheritTheParentsLane() {
        try (NioFlow<Integer> nioFlow = new NioFlow<>()) {
            List<Integer> laneResults = new CopyOnWriteArrayList<>();
            nioFlow.when(x -> x > 10)
                    .then(lane -> lane
                            .fanOut(x -> List.of(x, x + 1))
                            .handle(x -> {
                                laneResults.add(x);
                                return x;
                            }))
                    .otherwise(lane -> lane
                            .handle(x -> x));

            nioFlow.just(20); // splits into 20, 21 — both stay in the true lane
            nioFlow.just(5);  // false lane: never reaches the lane collector
            nioFlow.join();

            assertEquals(2, laneResults.size());
            assertTrue(laneResults.containsAll(List.of(20, 21)));
        }
    }

    @Test
    void aFailingFanOutFailsOnlyTheParentAndIsRecoverable() {
        try (NioFlow<Integer> nioFlow = new NioFlow<>()) {
            List<Integer> completed = new CopyOnWriteArrayList<>();
            nioFlow.fanOut(x -> {
                        if (x == 2) {
                            throw new IllegalStateException("cannot split value 2");
                        }
                        return List.of(x, x * 100);
                    })
                    .onErrorResume(error -> -1)
                    .onComplete(completed::add);

            nioFlow.justAll(List.of(1, 2, 3));
            nioFlow.join(); // no throw: the failing parent recovered as one value

            assertEquals(5, completed.size());
            assertTrue(completed.containsAll(List.of(1, 100, -1, 3, 300)));
        }
    }

    @Test
    void fanOutsNest() {
        try (NioFlow<Integer> nioFlow = new NioFlow<>()) {
            List<Integer> completed = new CopyOnWriteArrayList<>();
            nioFlow.fanOut(x -> List.of(x, x + 1))
                    .fanOut(x -> List.of(x * 10, x * 10 + 1))
                    .onComplete(completed::add);

            nioFlow.just(1); // -> 1, 2 -> 10, 11, 20, 21
            nioFlow.join();

            assertEquals(4, completed.size());
            assertTrue(completed.containsAll(List.of(10, 11, 20, 21)));
        }
    }

    @Test
    void childrenBypassBackpressureAdmission() throws InterruptedException {
        try (NioFlow<Integer> nioFlow = new NioFlow<>(Backpressure.failing(1))) {
            CountDownLatch allDone = new CountDownLatch(3);
            nioFlow.fanOut(x -> List.of(x, x + 1, x + 2))
                    .submit(x -> x)
                    .onComplete(value -> allDone.countDown());

            nioFlow.just(1); // one admission slot; the split to 3 children is internal

            assertTrue(allDone.await(2, TimeUnit.SECONDS));
            nioFlow.join();
        }
    }
}
