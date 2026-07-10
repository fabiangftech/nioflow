package dev.nioflow.unit;

import dev.nioflow.application.facade.DefaultNioFlow;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class DefaultNioFlowFilterTest {

    @Test
    void valuesFailingThePredicateAreDropped() {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            List<Integer> completed = new CopyOnWriteArrayList<>();
            defaultNioFlow.filter(x -> x % 2 == 0)
                    .handle(x -> x * 10)
                    .onComplete(completed::add);

            defaultNioFlow.justAll(List.of(1, 2, 3, 4, 5));
            defaultNioFlow.join();

            assertEquals(2, completed.size());
            assertTrue(completed.containsAll(List.of(20, 40)));
        }
    }

    @Test
    void droppedValuesFireNeitherOnCompleteNorOnError() {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            List<Integer> completed = new CopyOnWriteArrayList<>();
            List<Throwable> errors = new CopyOnWriteArrayList<>();
            defaultNioFlow.filter(x -> x > 0)
                    .onComplete(completed::add)
                    .onError(errors::add);

            defaultNioFlow.justAll(List.of(-1, 7, -2));
            defaultNioFlow.join();

            assertEquals(List.of(7), completed);
            assertTrue(errors.isEmpty());
        }
    }

    @Test
    void joinDoesNotHangWhenEveryValueIsFiltered() {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            defaultNioFlow.filter(x -> false);

            defaultNioFlow.justAll(List.of(1, 2, 3));

            assertNull(defaultNioFlow.join());
        }
    }

    @Test
    void aThrowingPredicateFailsOnlyThatValue() throws InterruptedException {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            CountDownLatch failed = new CountDownLatch(1);
            List<Integer> completed = new CopyOnWriteArrayList<>();
            defaultNioFlow.filter(x -> {
                        if (x == 2) {
                            throw new IllegalStateException("predicate boom");
                        }
                        return true;
                    })
                    .onComplete(completed::add)
                    .onError(error -> failed.countDown());

            defaultNioFlow.justAll(List.of(1, 2, 3));

            assertTrue(failed.await(2, TimeUnit.SECONDS));
            try {
                defaultNioFlow.join();
            } catch (RuntimeException expected) {
                // the failing value may or may not have been consumed by this join
            }
            assertEquals(2, completed.size());
            assertTrue(completed.containsAll(List.of(1, 3)));
        }
    }

    @Test
    void aFilterAfterAnAsyncStageDropsResumedValues() {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            List<Integer> completed = new CopyOnWriteArrayList<>();
            defaultNioFlow.submit(x -> x + 1)
                    .filter(x -> x % 2 == 0)
                    .onComplete(completed::add);

            defaultNioFlow.justAll(List.of(1, 2, 3)); // -> 2, 3, 4 -> keep 2 and 4
            defaultNioFlow.join();

            assertEquals(2, completed.size());
            assertTrue(completed.containsAll(List.of(2, 4)));
        }
    }

    @Test
    void aFilterInsideALaneOnlyDropsItsOwnLane() {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            List<Integer> completed = new CopyOnWriteArrayList<>();
            defaultNioFlow.when(x -> x > 10)
                    .then(lane -> lane
                            .filter(x -> x < 100))
                    .otherwise(lane -> lane
                            .handle(x -> x))
                    .onComplete(completed::add);

            defaultNioFlow.just(20);   // true lane, kept  (< 100)
            defaultNioFlow.just(500);  // true lane, dropped
            defaultNioFlow.just(5);    // false lane, the filter never applies
            defaultNioFlow.join();

            assertEquals(2, completed.size());
            assertTrue(completed.containsAll(List.of(20, 5)));
        }
    }
}
