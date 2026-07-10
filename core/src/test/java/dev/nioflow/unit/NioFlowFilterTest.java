package dev.nioflow.unit;

import dev.nioflow.application.facade.NioFlow;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class NioFlowFilterTest {

    @Test
    void valuesFailingThePredicateAreDropped() {
        try (NioFlow<Integer> nioFlow = new NioFlow<>()) {
            List<Integer> completed = new CopyOnWriteArrayList<>();
            nioFlow.filter(x -> x % 2 == 0)
                    .handle(x -> x * 10)
                    .onComplete(completed::add);

            nioFlow.justAll(List.of(1, 2, 3, 4, 5));
            nioFlow.join();

            assertEquals(2, completed.size());
            assertTrue(completed.containsAll(List.of(20, 40)));
        }
    }

    @Test
    void droppedValuesFireNeitherOnCompleteNorOnError() {
        try (NioFlow<Integer> nioFlow = new NioFlow<>()) {
            List<Integer> completed = new CopyOnWriteArrayList<>();
            List<Throwable> errors = new CopyOnWriteArrayList<>();
            nioFlow.filter(x -> x > 0)
                    .onComplete(completed::add)
                    .onError(errors::add);

            nioFlow.justAll(List.of(-1, 7, -2));
            nioFlow.join();

            assertEquals(List.of(7), completed);
            assertTrue(errors.isEmpty());
        }
    }

    @Test
    void joinDoesNotHangWhenEveryValueIsFiltered() {
        try (NioFlow<Integer> nioFlow = new NioFlow<>()) {
            nioFlow.filter(x -> false);

            nioFlow.justAll(List.of(1, 2, 3));

            assertNull(nioFlow.join());
        }
    }

    @Test
    void aThrowingPredicateFailsOnlyThatValue() throws InterruptedException {
        try (NioFlow<Integer> nioFlow = new NioFlow<>()) {
            CountDownLatch failed = new CountDownLatch(1);
            List<Integer> completed = new CopyOnWriteArrayList<>();
            nioFlow.filter(x -> {
                        if (x == 2) {
                            throw new IllegalStateException("predicate boom");
                        }
                        return true;
                    })
                    .onComplete(completed::add)
                    .onError(error -> failed.countDown());

            nioFlow.justAll(List.of(1, 2, 3));

            assertTrue(failed.await(2, TimeUnit.SECONDS));
            try {
                nioFlow.join();
            } catch (RuntimeException expected) {
                // the failing value may or may not have been consumed by this join
            }
            assertEquals(2, completed.size());
            assertTrue(completed.containsAll(List.of(1, 3)));
        }
    }

    @Test
    void aFilterAfterAnAsyncStageDropsResumedValues() {
        try (NioFlow<Integer> nioFlow = new NioFlow<>()) {
            List<Integer> completed = new CopyOnWriteArrayList<>();
            nioFlow.submit(x -> x + 1)
                    .filter(x -> x % 2 == 0)
                    .onComplete(completed::add);

            nioFlow.justAll(List.of(1, 2, 3)); // -> 2, 3, 4 -> keep 2 and 4
            nioFlow.join();

            assertEquals(2, completed.size());
            assertTrue(completed.containsAll(List.of(2, 4)));
        }
    }

    @Test
    void aFilterInsideALaneOnlyDropsItsOwnLane() {
        try (NioFlow<Integer> nioFlow = new NioFlow<>()) {
            List<Integer> completed = new CopyOnWriteArrayList<>();
            nioFlow.when(x -> x > 10)
                    .then(lane -> lane
                            .filter(x -> x < 100))
                    .otherwise(lane -> lane
                            .handle(x -> x))
                    .onComplete(completed::add);

            nioFlow.just(20);   // true lane, kept  (< 100)
            nioFlow.just(500);  // true lane, dropped
            nioFlow.just(5);    // false lane, the filter never applies
            nioFlow.join();

            assertEquals(2, completed.size());
            assertTrue(completed.containsAll(List.of(20, 5)));
        }
    }
}
