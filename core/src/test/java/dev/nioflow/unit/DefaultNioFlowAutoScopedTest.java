package dev.nioflow.unit;

import dev.nioflow.application.facade.DefaultNioFlow;
import dev.nioflow.core.facade.NioFlow;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.*;

class DefaultNioFlowAutoScopedTest {

    @Test
    void everyChainIsItsOwnScopeAndNothingAccumulates() {
        try (NioFlow<String> flow = DefaultNioFlow.autoScoped()) {
            for (int i = 0; i < 10; i++) {
                String greeting = flow.just("Hello")
                        .handle("greeting", s -> s + ", World!")
                        .join();
                assertEquals("Hello, World!", greeting);
            }
            assertEquals(0, flow.diagnostics().chain().size(), "the shared chain must stay empty");
        }
    }

    @Test
    void interleavedChainsNeverInterfere() {
        try (NioFlow<String> flow = DefaultNioFlow.autoScoped()) {
            NioFlow<String> first = flow.just("x");
            NioFlow<String> second = flow.just("y");
            first.handle(s -> s + "A");
            second.handle(s -> s + "B");

            assertEquals("xA", first.join());
            assertEquals("yB", second.join());
        }
    }

    @Test
    void callServesEachChainItsOwnResult() throws Exception {
        try (NioFlow<Integer> flow = DefaultNioFlow.autoScoped()) {
            CompletableFuture<Integer> doubled = flow.handle(x -> x * 2)
                    .call(21, Duration.ofSeconds(2));
            CompletableFuture<Integer> tripled = flow.handle(x -> x * 3)
                    .call(21, Duration.ofSeconds(2));

            assertEquals(42, doubled.get());
            assertEquals(63, tripled.get());
        }
    }

    @Test
    void forksWorkInsideAnAutoScopedChain() {
        try (NioFlow<Integer> flow = DefaultNioFlow.autoScoped()) {
            List<Integer> completed = new CopyOnWriteArrayList<>();
            flow.justAll(List.of(20, 5))
                    .when(x -> x > 10)
                    .then(lane -> lane.handle(x -> x + 100))
                    .otherwise(lane -> lane.handle(x -> x - 1))
                    .onComplete(completed::add)
                    .join();

            assertTrue(completed.containsAll(List.of(120, 4)));
        }
    }

    @Test
    void sharedChainOperationsThrowLoudlyOnTheFacade() {
        try (NioFlow<Integer> flow = DefaultNioFlow.autoScoped()) {
            assertThrows(IllegalStateException.class, flow::join);
            assertThrows(IllegalStateException.class, () -> flow.join(Duration.ofSeconds(1)));
            assertThrows(IllegalStateException.class, flow::seal);
            assertThrows(IllegalStateException.class, flow::release);
            assertThrows(IllegalStateException.class, () -> flow.remove("stage"));
            assertThrows(IllegalStateException.class,
                    () -> flow.replace("stage", segment -> segment.handle(x -> x)));
        }
    }

    @Test
    void facadeObserversSeeEveryChain() {
        try (NioFlow<Integer> flow = DefaultNioFlow.autoScoped()) {
            List<Integer> completed = new CopyOnWriteArrayList<>();
            flow.onComplete(completed::add); // global: registered on the facade

            flow.just(1).handle(x -> x + 1).join();
            flow.just(10).handle(x -> x * 10).join();

            assertTrue(completed.containsAll(List.of(2, 100)));
        }
    }

    @Test
    void explicitScopedStillWorksOnTheFacade() {
        try (NioFlow<String> flow = DefaultNioFlow.autoScoped()) {
            assertEquals("ok!", flow.scoped().just("ok").handle(s -> s + "!").join());
        }
    }

    @Test
    void closeStopsTheSharedEngine() {
        NioFlow<Integer> flow = DefaultNioFlow.autoScoped();
        flow.close();

        assertThrows(CompletionException.class, () -> flow.just(1).join());
    }
}
