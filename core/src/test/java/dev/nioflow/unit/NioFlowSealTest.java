package dev.nioflow.unit;

import dev.nioflow.application.facade.NioFlow;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.*;

class NioFlowSealTest {

    @Test
    void everyChainMutationThrowsAfterSeal() {
        try (NioFlow<Integer> nioFlow = new NioFlow<>()) {
            nioFlow.handle(x -> x + 1).seal();

            assertThrows(IllegalStateException.class, () -> nioFlow.handle(x -> x));
            assertThrows(IllegalStateException.class, () -> nioFlow.submit(x -> x));
            assertThrows(IllegalStateException.class, () -> nioFlow.filter(x -> true));
            assertThrows(IllegalStateException.class, () -> nioFlow.adapt(String::valueOf));
            assertThrows(IllegalStateException.class, () -> nioFlow.onErrorResume(error -> -1));
            assertThrows(IllegalStateException.class, () -> nioFlow.when(x -> x > 0));
        }
    }

    @Test
    void valuesKeepFlowingThroughASealedChain() {
        try (NioFlow<Integer> nioFlow = new NioFlow<>()) {
            List<Integer> completed = new CopyOnWriteArrayList<>();
            nioFlow.handle(x -> x * 2)
                    .submit(x -> x + 1)
                    .seal()
                    .onComplete(completed::add); // observers stay allowed

            nioFlow.justAll(List.of(1, 2, 3));
            nioFlow.join();

            assertEquals(3, completed.size());
            assertTrue(completed.containsAll(List.of(3, 5, 7)));
        }
    }

    @Test
    void aSealedPipelineReleasesFinishedValuesInsteadOfParking() {
        try (NioFlow<Integer> nioFlow = new NioFlow<>()) {
            List<Integer> completed = new CopyOnWriteArrayList<>();
            nioFlow.handle(x -> x * 2)
                    .seal()
                    .onComplete(completed::add);

            for (int i = 1; i <= 100; i++) {
                nioFlow.just(i);
            }
            nioFlow.join();

            assertEquals(100, completed.size());
            assertEquals(0, nioFlow.diagnostics().parked(), "sealed chains must not retain values");
        }
    }

    @Test
    void sealIsIdempotent() {
        try (NioFlow<Integer> nioFlow = new NioFlow<>()) {
            nioFlow.handle(x -> x + 1).seal().seal();

            assertEquals(2, nioFlow.just(1).join());
        }
    }
}
