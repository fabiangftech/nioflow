package dev.nioflow.unit;

import dev.nioflow.application.facade.NioFlow;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.*;

class NioFlowOnCompleteTest {

    @Test
    void everyFinishedValueIsDelivered() {
        try (NioFlow<Integer> pipeline = new NioFlow<>()) {
            List<Integer> completed = new CopyOnWriteArrayList<>();
            pipeline.submit(x -> x * 10)
                    .onComplete(completed::add);

            pipeline.just(1);
            pipeline.just(2);
            pipeline.just(3);
            pipeline.join();

            assertEquals(3, completed.size());
            assertTrue(completed.containsAll(List.of(10, 20, 30)));
        }
    }

    @Test
    void failedValuesAreNotDelivered() {
        try (NioFlow<Integer> pipeline = new NioFlow<>()) {
            List<Integer> completed = new CopyOnWriteArrayList<>();
            pipeline.submit(x -> {
                        if (x == 2) {
                            throw new IllegalStateException("value 2 boom");
                        }
                        return x * 10;
                    })
                    .onComplete(completed::add);

            pipeline.just(1);
            pipeline.just(2);
            pipeline.just(3);

            assertThrows(CompletionException.class, pipeline::join);
            assertEquals(2, completed.size());
            assertTrue(completed.containsAll(List.of(10, 30)));
        }
    }

    @Test
    void everyHandlerReceivesTheValueEvenIfOneThrows() {
        try (NioFlow<Integer> pipeline = new NioFlow<>()) {
            List<Integer> first = new CopyOnWriteArrayList<>();
            List<Integer> second = new CopyOnWriteArrayList<>();
            pipeline.handle(x -> x + 1)
                    .onComplete(x -> {
                        first.add(x);
                        throw new RuntimeException("misbehaving handler");
                    })
                    .onComplete(second::add);

            pipeline.just(1);
            pipeline.just(2);
            pipeline.join();

            assertEquals(2, first.size());
            assertTrue(first.containsAll(List.of(2, 3)));
            assertEquals(2, second.size());
            assertTrue(second.containsAll(List.of(2, 3)));
        }
    }

    @Test
    void joinNeverReturnsBeforeCompleteHandlersFinished() {
        try (NioFlow<Integer> pipeline = new NioFlow<>()) {
            List<Integer> completed = new CopyOnWriteArrayList<>();
            pipeline.submit(x -> x)
                    .onComplete(x -> {
                        sleep(100); // a slow handler must still finish before join returns
                        completed.add(x);
                    });

            pipeline.just(7);
            pipeline.join();

            assertEquals(List.of(7), completed);
        }
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
