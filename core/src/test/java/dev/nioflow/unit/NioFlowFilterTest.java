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
        try (NioFlow<Integer> pipeline = new NioFlow<>()) {
            List<Integer> completed = new CopyOnWriteArrayList<>();
            pipeline.filter(x -> x % 2 == 0)
                    .handle(x -> x * 10)
                    .onComplete(completed::add);

            pipeline.justAll(List.of(1, 2, 3, 4, 5));
            pipeline.join();

            assertEquals(2, completed.size());
            assertTrue(completed.containsAll(List.of(20, 40)));
        }
    }

    @Test
    void droppedValuesFireNeitherOnCompleteNorOnError() {
        try (NioFlow<Integer> pipeline = new NioFlow<>()) {
            List<Integer> completed = new CopyOnWriteArrayList<>();
            List<Throwable> errors = new CopyOnWriteArrayList<>();
            pipeline.filter(x -> x > 0)
                    .onComplete(completed::add)
                    .onError(errors::add);

            pipeline.justAll(List.of(-1, 7, -2));
            pipeline.join();

            assertEquals(List.of(7), completed);
            assertTrue(errors.isEmpty());
        }
    }

    @Test
    void joinDoesNotHangWhenEveryValueIsFiltered() {
        try (NioFlow<Integer> pipeline = new NioFlow<>()) {
            pipeline.filter(x -> false);

            pipeline.justAll(List.of(1, 2, 3));

            assertNull(pipeline.join());
        }
    }

    @Test
    void aThrowingPredicateFailsOnlyThatValue() throws InterruptedException {
        try (NioFlow<Integer> pipeline = new NioFlow<>()) {
            CountDownLatch failed = new CountDownLatch(1);
            List<Integer> completed = new CopyOnWriteArrayList<>();
            pipeline.filter(x -> {
                        if (x == 2) {
                            throw new IllegalStateException("predicate boom");
                        }
                        return true;
                    })
                    .onComplete(completed::add)
                    .onError(error -> failed.countDown());

            pipeline.justAll(List.of(1, 2, 3));

            assertTrue(failed.await(2, TimeUnit.SECONDS));
            try {
                pipeline.join();
            } catch (RuntimeException expected) {
                // the failing value may or may not have been consumed by this join
            }
            assertEquals(2, completed.size());
            assertTrue(completed.containsAll(List.of(1, 3)));
        }
    }

    @Test
    void aFilterAfterAnAsyncStageDropsResumedValues() {
        try (NioFlow<Integer> pipeline = new NioFlow<>()) {
            List<Integer> completed = new CopyOnWriteArrayList<>();
            pipeline.submit(x -> x + 1)
                    .filter(x -> x % 2 == 0)
                    .onComplete(completed::add);

            pipeline.justAll(List.of(1, 2, 3)); // -> 2, 3, 4 -> keep 2 and 4
            pipeline.join();

            assertEquals(2, completed.size());
            assertTrue(completed.containsAll(List.of(2, 4)));
        }
    }

    @Test
    void aFilterInsideALaneOnlyDropsItsOwnLane() {
        try (NioFlow<Integer> pipeline = new NioFlow<>()) {
            List<Integer> completed = new CopyOnWriteArrayList<>();
            pipeline.when(x -> x > 10)
                    .then(lane -> lane
                            .filter(x -> x < 100))
                    .otherwise(lane -> lane
                            .handle(x -> x))
                    .onComplete(completed::add);

            pipeline.just(20);   // true lane, kept  (< 100)
            pipeline.just(500);  // true lane, dropped
            pipeline.just(5);    // false lane, the filter never applies
            pipeline.join();

            assertEquals(2, completed.size());
            assertTrue(completed.containsAll(List.of(20, 5)));
        }
    }
}
