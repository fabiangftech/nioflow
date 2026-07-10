package dev.nioflow.unit;

import dev.nioflow.application.facade.DefaultNioFlow;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.*;

class DefaultNioFlowSealTest {

    @Test
    void everyChainMutationThrowsAfterSeal() {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            defaultNioFlow.handle(x -> x + 1).seal();

            assertThrows(IllegalStateException.class, () -> defaultNioFlow.handle(x -> x));
            assertThrows(IllegalStateException.class, () -> defaultNioFlow.submit(x -> x));
            assertThrows(IllegalStateException.class, () -> defaultNioFlow.filter(x -> true));
            assertThrows(IllegalStateException.class, () -> defaultNioFlow.adapt(String::valueOf));
            assertThrows(IllegalStateException.class, () -> defaultNioFlow.onErrorResume(error -> -1));
            assertThrows(IllegalStateException.class, () -> defaultNioFlow.when(x -> x > 0));
        }
    }

    @Test
    void valuesKeepFlowingThroughASealedChain() {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            List<Integer> completed = new CopyOnWriteArrayList<>();
            defaultNioFlow.handle(x -> x * 2)
                    .submit(x -> x + 1)
                    .seal()
                    .onComplete(completed::add); // observers stay allowed

            defaultNioFlow.justAll(List.of(1, 2, 3));
            defaultNioFlow.join();

            assertEquals(3, completed.size());
            assertTrue(completed.containsAll(List.of(3, 5, 7)));
        }
    }

    @Test
    void aSealedPipelineReleasesFinishedValuesInsteadOfParking() {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            List<Integer> completed = new CopyOnWriteArrayList<>();
            defaultNioFlow.handle(x -> x * 2)
                    .seal()
                    .onComplete(completed::add);

            for (int i = 1; i <= 100; i++) {
                defaultNioFlow.just(i);
            }
            defaultNioFlow.join();

            assertEquals(100, completed.size());
            assertEquals(0, defaultNioFlow.diagnostics().parked(), "sealed chains must not retain values");
        }
    }

    @Test
    void sealIsIdempotent() {
        try (DefaultNioFlow<Integer> defaultNioFlow = new DefaultNioFlow<>()) {
            defaultNioFlow.handle(x -> x + 1).seal().seal();

            assertEquals(2, defaultNioFlow.just(1).join());
        }
    }
}
