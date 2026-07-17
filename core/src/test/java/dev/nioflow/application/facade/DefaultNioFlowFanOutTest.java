package dev.nioflow.application.facade;

import dev.nioflow.core.facade.NioFlow;
import dev.nioflow.core.facade.NioStep;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DefaultNioFlowFanOutTest {

    /**
     * A fan-out joins when its branch countdown reaches zero, and only a branch
     * ever counts down — so with no branches nothing ever did, the join never
     * ran, and the caller's future hung forever. The list is usually built
     * (a stream over configured enrichers), so empty is reachable in one
     * environment and not another: the worst kind of hang to debug. Rejected at
     * the caller's own fanOut(...) line instead.
     */
    @Test
    void anEmptyBranchListIsRejectedAtBuildTimeInsteadOfHangingTheRequest() {
        List<Function<Integer, Integer>> none = List.of();
        NioStep<Integer, Integer> step = DefaultNioFlow.<Integer, Integer>from(Integer.class).just(7);

        IllegalArgumentException rejected = assertThrows(IllegalArgumentException.class,
                () -> step.fanOut("enrich", none, List::size));
        assertEquals("fanOut 'enrich' needs at least one branch", rejected.getMessage());
    }

    @Test
    void branchesRunInParallelAndJoinInDeclarationOrder() {
        // A barrier of 3: unless all three branches run CONCURRENTLY, none can
        // pass it and the execution fails on the barrier timeout.
        var barrier = new CyclicBarrier(3);
        Function<Integer, Integer> meet = value -> {
            try {
                barrier.await(5, TimeUnit.SECONDS);
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
            return value;
        };
        List<Function<Integer, Integer>> branches = List.of(
                meet.andThen(value -> value + 1),
                meet.andThen(value -> value + 2),
                meet.andThen(value -> value + 3));
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class);

        String result = flow.just(7)
                .fanOut("enrich", branches,
                        results -> results.get(0) + "-" + results.get(1) + "-" + results.get(2))
                .execute();

        assertEquals("8-9-10", result); // declaration order, regardless of finish order
    }

    @Test
    void fanOutRetypesThePipeline() {
        List<Function<Integer, String>> branches = List.of(
                "a"::repeat,
                value -> "b".repeat(value / 2));
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class);

        Integer total = flow.just(10)
                .fanOut(branches, parts -> parts.get(0).length() + parts.get(1).length())
                .handle(value -> value * 10)
                .execute();

        assertEquals(150, total); // (10 + 5) * 10, typed String branches → Integer join
    }

    @Test
    void aFailingBranchFailsTheFanOutAndIsRecoverable() {
        List<Function<Integer, Integer>> branches = List.of(
                value -> value + 1,
                value -> {
                    throw new IllegalStateException("branch down");
                });
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class);

        Integer recovered = flow.just(1)
                .fanOut(branches, results -> results.get(0) + results.get(1))
                .recover("fallback", error -> -1)
                .execute();

        assertEquals(-1, recovered);

        var failure = assertThrows(CompletionException.class, () -> flow.just(1)
                .fanOut(branches, results -> results.get(0))
                .execute());
        assertEquals("branch down", failure.getCause().getMessage());
    }

    @Test
    void fanOutInsideALaneOnlyRunsForRoutedValues() {
        List<Function<Integer, Integer>> branches = List.of(
                value -> value + 1,
                value -> value + 2);
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class);
        flow.when(value -> value % 2 == 0)
                .then(lane -> lane
                        .fanOut("even-enrich", branches,
                                results -> results.get(0) * results.get(1)))
                .otherwise(lane -> lane
                        .handle(value -> -value));

        assertEquals(30, flow.just(4).execute()); // (4+1) * (4+2)
        assertEquals(-3, flow.just(3).execute()); // odd: the fan-out never runs
    }

    @Test
    void fanOutWorksOnSealedCompiledChains() {
        List<Function<Integer, Integer>> branches = List.of(
                value -> value * 2,
                value -> value * 3);
        var engine = new DefaultNioEngine();
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        flow.handle("plus", value -> value + 1)
                .fanOut("split", branches, results -> results.get(0) + results.get(1))
                .filter(value -> value > 0)
                .handle("tail", value -> value + 100);
        engine.seal();

        assertEquals(130, flow.just(5).execute()); // (6*2 + 6*3) + 100
        assertNull(flow.just(-10).execute());      // join < 0: filtered after the fan-out
    }
}
