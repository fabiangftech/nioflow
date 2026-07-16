package dev.nioflow.application.facade;

import dev.nioflow.core.facade.Context;
import dev.nioflow.core.facade.NioFlow;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * RFC 0020 — differential bug-hunt. The compiled dispatch plan is an
 * optimization, never a semantic: for every chain shape here, running it sealed
 * (compiled) and unsealed (interpreted) must produce the identical outcome —
 * the same value, or the same failure — across a battery of inputs. The oracle
 * is only "the two paths must agree", so a divergence is a bug with a reproducer
 * attached, no hand-computed expected value required.
 */
class DefaultNioEngineDifferentialProbeTest {

    private static final Context.Key<Integer> PROBE = Context.Key.of("probe");

    private static final int[] INPUTS = {-8, -1, 0, 1, 2, 3, 11, 12, 13, 15, 20, 49, 50, 100};

    @Test
    void plainStagesAgree() {
        assertCompiledMatchesInterpreted(flow -> flow
                .handle("plus", value -> value + 1)
                .handle("times", value -> value * 3)
                .handle("minus", value -> value - 7));
    }

    @Test
    void filterCutAgrees() {
        assertCompiledMatchesInterpreted(flow -> flow
                .handle("plus", value -> value + 1)
                .filter(value -> value != 13)
                .handle("double", value -> value * 2));
    }

    @Test
    void positionalRecoveryAgrees() {
        assertCompiledMatchesInterpreted(flow -> flow
                .handle("boom-on-fifty", value -> {
                    if (value == 50) {
                        throw new IllegalStateException("boom");
                    }
                    return value;
                })
                .recover("fallback", error -> -1)
                .handle("double", value -> value * 2));
    }

    @Test
    void unrecoveredFailureAgrees() {
        // No recover: both paths must FAIL identically on value 50.
        assertCompiledMatchesInterpreted(flow -> flow
                .handle("plus", value -> value + 1)
                .handle("boom-on-fifty-one", value -> {
                    if (value == 51) {
                        throw new IllegalArgumentException("boom");
                    }
                    return value;
                }));
    }

    @Test
    void multipleRecoveriesKeepPositionAgree() {
        assertCompiledMatchesInterpreted(flow -> flow
                .handle("a", value -> {
                    if (value == 1) {
                        throw new IllegalStateException("first");
                    }
                    return value;
                })
                .recover("r1", error -> 111)
                .handle("b", value -> {
                    if (value == 2) {
                        throw new IllegalStateException("second");
                    }
                    return value;
                })
                .recover("r2", error -> 222)
                .handle("c", value -> value + 1));
    }

    @Test
    void syncStagesFuseAndAgree() {
        assertCompiledMatchesInterpreted(flow -> flow
                .handleSync("s1", value -> value + 1)
                .handle("w", value -> value * 2)
                .handleSync("s2", value -> value - 3));
    }

    @Test
    void backgroundEffectDoesNotAlterValueAgree() {
        assertCompiledMatchesInterpreted(flow -> flow
                .handle("plus", value -> value + 1)
                .background("noise", value -> { /* pure side effect */ })
                .handle("double", value -> value * 2));
    }

    @Test
    void contextualStagesAgree() {
        assertCompiledMatchesInterpreted(flow -> flow
                .handleContextual("write", (value, ctx) -> {
                    ctx.put(PROBE, value * 2);
                    return value + 1;
                })
                .handleContextual("read", (value, ctx) -> value + ctx.getOrDefault(PROBE, 0)));
    }

    @Test
    void fanOutJoinAgrees() {
        assertCompiledMatchesInterpreted(flow -> {
            List<Function<Integer, Integer>> branches = List.of(
                    value -> value + 1,
                    value -> value * 2,
                    value -> value - 5);
            flow.fanOut(branches, results -> results.get(0) + results.get(1) + results.get(2))
                    .handle("after", value -> value + 100);
        });
    }

    @Test
    void whenBranchAgrees() {
        assertCompiledMatchesInterpreted(flow -> flow
                .when(value -> value > 10)
                .then(lane -> lane.handle(value -> value * 2))
                .otherwise(lane -> lane.handle(value -> value - 1))
                .handle("tail", value -> value + 100));
    }

    @Test
    void matchFirstMatchWinsAgrees() {
        assertCompiledMatchesInterpreted(flow -> flow
                .match()
                .is(value -> value % 2 == 0, lane -> lane.handle(value -> value * 10))
                .is(value -> value > 10, lane -> lane.handle(value -> value + 1000))
                .otherwise(lane -> lane.handle(value -> -value))
                .handle("tail", value -> value + 1));
    }

    @Test
    void nestedBranchesAgree() {
        assertCompiledMatchesInterpreted(flow -> flow
                .when(value -> value > 5)
                .then(lane -> lane
                        .when(value -> value % 2 == 0)
                        .then(inner -> inner.handle(value -> value + 10))
                        .otherwise(inner -> inner.handle(value -> value + 20)))
                .otherwise(lane -> lane.handle(value -> value + 30)));
    }

    @Test
    void laneScopedFilterAgrees() {
        assertCompiledMatchesInterpreted(flow -> flow
                .when(value -> value > 0)
                .then(lane -> lane.filter(value -> value != 15))
                .otherwise(lane -> lane.handle(value -> value))
                .handle("tail", value -> value + 1));
    }

    @Test
    void laneScopedRecoverOnlyCatchesItsBranchAgree() {
        assertCompiledMatchesInterpreted(flow -> flow
                .when(value -> value > 0)
                .then(lane -> lane
                        .handle("maybe-boom", value -> {
                            if (value == 15) {
                                throw new IllegalStateException("boom");
                            }
                            return value;
                        })
                        .recover("lane-fallback", error -> -99))
                .otherwise(lane -> lane.handle(value -> value))
                .handle("tail", value -> value + 1));
    }

    @Test
    void timeoutStageDispatchesAloneButAgrees() {
        // A generous timeout never fires (the function is instant) but forces the
        // stage to dispatch alone; compiled and interpreted must still agree.
        assertCompiledMatchesInterpreted(flow -> flow
                .handle("timed", value -> value + 1, Duration.ofSeconds(30))
                .handle("after", value -> value * 2));
    }

    @Test
    void guardSkippedDecisionInsideWindowAgrees() {
        // Once the first case matches, the second case predicate is ruled out, so
        // the compiled window has to step over it exactly as the interpreter does.
        assertCompiledMatchesInterpreted(flow -> flow
                .handle("pre", value -> value + 1)
                .match()
                .is(value -> value % 2 == 0, lane -> lane.handle(value -> value * 10))
                .is(value -> value % 2 == 0, lane -> lane.handle(value -> value * 100))
                .otherwise(lane -> lane.handle(value -> -value))
                .handle("post", value -> value + 7));
    }

    private static void assertCompiledMatchesInterpreted(Consumer<NioFlow<Integer, Integer>> shape) {
        var interpreted = new DefaultNioEngine();
        var compiled = new DefaultNioEngine();
        try {
            NioFlow<Integer, Integer> interpretedFlow = DefaultNioFlow.from(Integer.class, interpreted);
            NioFlow<Integer, Integer> compiledFlow = DefaultNioFlow.from(Integer.class, compiled);
            shape.accept(interpretedFlow);
            shape.accept(compiledFlow);
            compiled.seal();

            for (int input : INPUTS) {
                assertEquals(outcome(interpretedFlow, input), outcome(compiledFlow, input),
                        "compiled diverged from interpreted at input " + input);
            }
        } finally {
            interpreted.shutdown(Duration.ofMillis(200));
            compiled.shutdown(Duration.ofMillis(200));
        }
    }

    /** Value-or-failure as a comparable string, so the oracle is pure agreement. */
    private static String outcome(NioFlow<Integer, Integer> flow, int input) {
        try {
            return "V:" + flow.just(input).execute();
        } catch (Throwable failure) {
            Throwable root = failure;
            while (root.getCause() != null && root.getCause() != root) {
                root = root.getCause();
            }
            return "E:" + root.getClass().getSimpleName();
        }
    }
}
