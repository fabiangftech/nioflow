package dev.nioflow.application.facade;

import dev.nioflow.core.facade.NioFlow;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * fanOutAsync: the split-join whose branches return a CompletionStage, so a
 * worker only invokes each branch and is released — no parked worker per
 * branch. Results, ordering and failure→recover semantics match the sync
 * fanOut; only the completion mechanism differs.
 */
class DefaultNioFlowFanOutAsyncTest {

    @Test
    void branchesInvokeConcurrentlyAndJoinInDeclarationOrder() {
        // The barrier runs while each branch is being INVOKED on its worker:
        // unless all three invoke concurrently, none passes it and the branch
        // stages never build. That is the no-sequential-dispatch proof.
        var barrier = new CyclicBarrier(3);
        Function<Integer, CompletionStage<Integer>> meet = value -> {
            try {
                barrier.await(5, TimeUnit.SECONDS);
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
            return CompletableFuture.completedFuture(value);
        };
        List<Function<Integer, CompletionStage<Integer>>> branches = List.of(
                value -> meet.apply(value).thenApply(v -> v + 1),
                value -> meet.apply(value).thenApply(v -> v + 2),
                value -> meet.apply(value).thenApply(v -> v + 3));
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class);

        String result = flow.just(7)
                .fanOutAsync("enrich", branches,
                        results -> results.get(0) + "-" + results.get(1) + "-" + results.get(2))
                .execute();

        assertEquals("8-9-10", result);
    }

    @Test
    void joinSeesDeclarationOrderEvenWhenBranchesCompleteOutOfOrder() {
        // Branch 0 resolves LATE, branch 1 immediately — the slot is indexed by
        // declaration, so the join still sees [slow, fast].
        List<Function<Integer, CompletionStage<Integer>>> branches = List.of(
                value -> CompletableFuture.supplyAsync(() -> value + 1,
                        CompletableFuture.delayedExecutor(80, TimeUnit.MILLISECONDS)),
                value -> CompletableFuture.completedFuture(value + 100));
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class);

        String out = flow.just(1)
                .fanOutAsync(branches, results -> results.get(0) + "," + results.get(1))
                .execute();

        assertEquals("2,101", out);
    }

    @Test
    void retypesThePipeline() {
        List<Function<Integer, CompletionStage<String>>> branches = List.of(
                value -> CompletableFuture.completedFuture("a".repeat(value)),
                value -> CompletableFuture.completedFuture("b".repeat(value / 2)));
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class);

        Integer total = flow.just(10)
                .fanOutAsync(branches, parts -> parts.get(0).length() + parts.get(1).length())
                .handle(value -> value * 10)
                .execute();

        assertEquals(150, total);
    }

    @Test
    void aFailingAsyncBranchIsRecoverableAsItself() {
        List<Function<Integer, CompletionStage<Integer>>> branches = List.of(
                value -> CompletableFuture.completedFuture(value + 1),
                value -> CompletableFuture.failedFuture(new IllegalStateException("remote down")));
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class);

        Integer recovered = flow.just(1)
                .fanOutAsync(branches, results -> results.get(0) + results.get(1))
                .recover("fallback", error -> -1)
                .execute();
        assertEquals(-1, recovered);

        var pipeline = flow.just(1).fanOutAsync(branches, results -> results.get(0));
        var failure = assertThrows(CompletionException.class, pipeline::execute);
        assertEquals("remote down", failure.getCause().getMessage());
    }

    @Test
    void aSynchronousThrowWhileBuildingAStageIsAlsoRecoverable() {
        List<Function<Integer, CompletionStage<Integer>>> branches = List.of(
                CompletableFuture::completedFuture,
                value -> {
                    throw new IllegalStateException("could not build the call");
                });
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class);

        var pipeline = flow.just(1).fanOutAsync(branches, results -> results.get(0));
        var failure = assertThrows(CompletionException.class, pipeline::execute);
        assertEquals("could not build the call", failure.getCause().getMessage());
    }

    @Test
    void exactlyOneFailureSurfacesWhenTwoBranchesFail() {
        List<Function<Integer, CompletionStage<Integer>>> branches = List.of(
                value -> CompletableFuture.failedFuture(new IllegalStateException("first")),
                value -> CompletableFuture.failedFuture(new IllegalStateException("second")));
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class);

        // Whichever branch CASes the failure first wins; the other is dropped.
        // What must hold: a SINGLE throwable reaches recover, not a composite.
        var captured = new java.util.concurrent.atomic.AtomicReference<Throwable>();
        Integer recovered = flow.just(1)
                .fanOutAsync(branches, results -> results.get(0))
                .recover(error -> {
                    captured.set(error);
                    return -1;
                })
                .execute();

        assertEquals(-1, recovered);
        String message = captured.get().getMessage();
        assertTrue("first".equals(message) || "second".equals(message),
                "expected a single branch failure, got: " + captured.get());
    }

    @Test
    void insideALaneOnlyRunsForRoutedValues() {
        List<Function<Integer, CompletionStage<Integer>>> branches = List.of(
                value -> CompletableFuture.completedFuture(value + 1),
                value -> CompletableFuture.completedFuture(value + 2));
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class);
        flow.when(value -> value % 2 == 0)
                .then(lane -> lane
                        .fanOutAsync("even-enrich", branches,
                                results -> results.get(0) * results.get(1)))
                .otherwise(lane -> lane.handle(value -> -value));

        assertEquals(30, flow.just(4).execute()); // (4+1) * (4+2)
        assertEquals(-3, flow.just(3).execute()); // odd: the fan-out never runs
    }

    @Test
    void worksOnSealedCompiledChains() {
        List<Function<Integer, CompletionStage<Integer>>> branches = List.of(
                value -> CompletableFuture.completedFuture(value * 2),
                value -> CompletableFuture.completedFuture(value * 3));
        var engine = new DefaultNioEngine();
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        flow.handle("plus", value -> value + 1)
                .fanOutAsync("split", branches, results -> results.get(0) + results.get(1))
                .filter(value -> value > 0)
                .handle("tail", value -> value + 100);
        engine.seal();

        assertEquals(130, flow.just(5).execute()); // (6*2 + 6*3) + 100
        assertNull(flow.just(-10).execute());

        engine.shutdown(Duration.ofMillis(200));
    }
}
