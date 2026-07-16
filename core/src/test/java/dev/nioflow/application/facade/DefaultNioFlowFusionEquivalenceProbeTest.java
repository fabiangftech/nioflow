package dev.nioflow.application.facade;

import dev.nioflow.core.facade.NioFlow;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.function.BiConsumer;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * RFC 0020 — fusion-equivalence bug-hunt. A run of no-timeout stages/filters/
 * recoveries travels boss→worker→boss as one composed function; a stage with a
 * timeout dispatches alone and breaks the run. The two paths are separate code,
 * and the result is contractually identical. Each shape here is built twice —
 * once fusable (no timeout) and once with a generous per-stage timeout that
 * never fires but forces every stage to dispatch alone — and the outcomes must
 * match across a battery of inputs. The oracle is agreement, so a divergence is
 * a fusion bug with a reproducer attached.
 */
class DefaultNioFlowFusionEquivalenceProbeTest {

    private static final Duration NEVER = Duration.ofSeconds(30);

    private static final int[] INPUTS = {-8, -1, 0, 1, 2, 3, 11, 12, 13, 15, 20, 49, 50, 100};

    @Test
    void stageRunFusesEquivalently() {
        assertFusedMatchesUnfused((flow, timeout) -> {
            addStage(flow, "a", value -> value + 1, timeout);
            addStage(flow, "b", value -> value * 3, timeout);
            addStage(flow, "c", value -> value - 7, timeout);
        });
    }

    @Test
    void filterInsideRunFusesEquivalently() {
        assertFusedMatchesUnfused((flow, timeout) -> {
            addStage(flow, "a", value -> value + 1, timeout);
            flow.filter(value -> value != 13);
            addStage(flow, "b", value -> value * 2, timeout);
        });
    }

    @Test
    void positionalRecoveryInsideRunFusesEquivalently() {
        assertFusedMatchesUnfused((flow, timeout) -> {
            addStage(flow, "boom", value -> {
                if (value == 50) {
                    throw new IllegalStateException("boom");
                }
                return value;
            }, timeout);
            flow.recover("r", error -> -1);
            addStage(flow, "after", value -> value * 2, timeout);
        });
    }

    @Test
    void twoRecoveriesInsideRunKeepPositionEquivalently() {
        assertFusedMatchesUnfused((flow, timeout) -> {
            addStage(flow, "a", value -> {
                if (value == 1) {
                    throw new IllegalStateException("first");
                }
                return value;
            }, timeout);
            flow.recover("r1", error -> 111);
            addStage(flow, "b", value -> {
                if (value == 2) {
                    throw new IllegalStateException("second");
                }
                return value;
            }, timeout);
            flow.recover("r2", error -> 222);
            addStage(flow, "c", value -> value + 1, timeout);
        });
    }

    @Test
    void filterThenRecoveryInsideRunFuseEquivalently() {
        assertFusedMatchesUnfused((flow, timeout) -> {
            addStage(flow, "a", value -> value + 1, timeout);
            flow.filter(value -> value != 4);
            addStage(flow, "b", value -> {
                if (value == 16) {
                    throw new IllegalStateException("boom");
                }
                return value;
            }, timeout);
            flow.recover("r", error -> -5);
            addStage(flow, "c", value -> value * 2, timeout);
        });
    }

    private static void addStage(NioFlow<Integer, Integer> flow, String name,
                                 java.util.function.UnaryOperator<Integer> function, Duration timeout) {
        if (timeout == null) {
            flow.handle(name, function);
        } else {
            flow.handle(name, function, timeout);
        }
    }

    private static void assertFusedMatchesUnfused(BiConsumer<NioFlow<Integer, Integer>, Duration> shape) {
        var fusedEngine = new DefaultNioEngine();
        var unfusedEngine = new DefaultNioEngine();
        try {
            NioFlow<Integer, Integer> fused = DefaultNioFlow.from(Integer.class, fusedEngine);
            NioFlow<Integer, Integer> unfused = DefaultNioFlow.from(Integer.class, unfusedEngine);
            shape.accept(fused, null);      // no timeout: the run fuses
            shape.accept(unfused, NEVER);   // generous timeout: each stage dispatches alone
            fusedEngine.seal();
            unfusedEngine.seal();

            for (int input : INPUTS) {
                assertEquals(outcome(fused, input), outcome(unfused, input),
                        "fused diverged from unfused at input " + input);
            }
        } finally {
            fusedEngine.shutdown(Duration.ofMillis(200));
            unfusedEngine.shutdown(Duration.ofMillis(200));
        }
    }

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
