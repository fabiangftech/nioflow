package dev.nioflow.unit;

import dev.nioflow.application.facade.NioFlow;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.*;

class NioFlowSealTest {

    @Test
    void everyChainMutationThrowsAfterSeal() {
        try (NioFlow<Integer> pipeline = new NioFlow<>()) {
            pipeline.handle(x -> x + 1).seal();

            assertThrows(IllegalStateException.class, () -> pipeline.handle(x -> x));
            assertThrows(IllegalStateException.class, () -> pipeline.submit(x -> x));
            assertThrows(IllegalStateException.class, () -> pipeline.filter(x -> true));
            assertThrows(IllegalStateException.class, () -> pipeline.adapt(String::valueOf));
            assertThrows(IllegalStateException.class, () -> pipeline.onErrorResume(error -> -1));
            assertThrows(IllegalStateException.class, () -> pipeline.when(x -> x > 0));
        }
    }

    @Test
    void valuesKeepFlowingThroughASealedChain() {
        try (NioFlow<Integer> pipeline = new NioFlow<>()) {
            List<Integer> completed = new CopyOnWriteArrayList<>();
            pipeline.handle(x -> x * 2)
                    .submit(x -> x + 1)
                    .seal()
                    .onComplete(completed::add); // observers stay allowed

            pipeline.justAll(List.of(1, 2, 3));
            pipeline.join();

            assertEquals(3, completed.size());
            assertTrue(completed.containsAll(List.of(3, 5, 7)));
        }
    }

    @Test
    void aSealedPipelineReleasesFinishedValuesInsteadOfParking() {
        try (NioFlow<Integer> pipeline = new NioFlow<>()) {
            List<Integer> completed = new CopyOnWriteArrayList<>();
            pipeline.handle(x -> x * 2)
                    .seal()
                    .onComplete(completed::add);

            for (int i = 1; i <= 100; i++) {
                pipeline.just(i);
            }
            pipeline.join();

            assertEquals(100, completed.size());
            assertEquals(0, pipeline.diagnostics().parked(), "sealed chains must not retain values");
        }
    }

    @Test
    void sealIsIdempotent() {
        try (NioFlow<Integer> pipeline = new NioFlow<>()) {
            pipeline.handle(x -> x + 1).seal().seal();

            assertEquals(2, pipeline.just(1).join());
        }
    }
}
