package dev.nioflow.unit;

import dev.nioflow.application.facade.DefaultNioFlow;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.*;

class DefaultNioFlowOnCompleteTest {

    @Test
    void everyFinishedValueIsDelivered() {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            List<Integer> completed = new CopyOnWriteArrayList<>();
            defaultNioFlow.submit(x -> x * 10)
                    .onComplete(completed::add);

            defaultNioFlow.just(1);
            defaultNioFlow.just(2);
            defaultNioFlow.just(3);
            defaultNioFlow.join();

            assertEquals(3, completed.size());
            assertTrue(completed.containsAll(List.of(10, 20, 30)));
        }
    }

    @Test
    void failedValuesAreNotDelivered() {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            List<Integer> completed = new CopyOnWriteArrayList<>();
            defaultNioFlow.submit(x -> {
                        if (x == 2) {
                            throw new IllegalStateException("value 2 boom");
                        }
                        return x * 10;
                    })
                    .onComplete(completed::add);

            defaultNioFlow.just(1);
            defaultNioFlow.just(2);
            defaultNioFlow.just(3);

            assertThrows(CompletionException.class, defaultNioFlow::join);
            assertEquals(2, completed.size());
            assertTrue(completed.containsAll(List.of(10, 30)));
        }
    }

    @Test
    void everyHandlerReceivesTheValueEvenIfOneThrows() {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            List<Integer> first = new CopyOnWriteArrayList<>();
            List<Integer> second = new CopyOnWriteArrayList<>();
            defaultNioFlow.handle(x -> x + 1)
                    .onComplete(x -> {
                        first.add(x);
                        throw new RuntimeException("misbehaving handler");
                    })
                    .onComplete(second::add);

            defaultNioFlow.just(1);
            defaultNioFlow.just(2);
            defaultNioFlow.join();

            assertEquals(2, first.size());
            assertTrue(first.containsAll(List.of(2, 3)));
            assertEquals(2, second.size());
            assertTrue(second.containsAll(List.of(2, 3)));
        }
    }

    @Test
    void joinNeverReturnsBeforeCompleteHandlersFinished() {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            List<Integer> completed = new CopyOnWriteArrayList<>();
            defaultNioFlow.submit(x -> x)
                    .onComplete(x -> {
                        sleep(100); // a slow handler must still finish before join returns
                        completed.add(x);
                    });

            defaultNioFlow.just(7);
            defaultNioFlow.join();

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
