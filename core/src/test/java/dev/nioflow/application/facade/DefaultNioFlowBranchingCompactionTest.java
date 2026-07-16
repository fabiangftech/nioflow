package dev.nioflow.application.facade;

import dev.nioflow.core.facade.NioFlow;
import dev.nioflow.core.facade.NioStep;
import dev.nioflow.core.model.Decision;
import dev.nioflow.core.model.Guard;
import dev.nioflow.core.model.Link;
import dev.nioflow.core.model.Stage;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * RFC 0038 — a per-request when()/match() draws its decision ids from the
 * engine-wide counter, which climbs forever. Left alone, an id past the bitset
 * limit (511) drops every such execution onto the per-execution overflow map for
 * the life of the JVM. The fix compacts a per-request pipeline's decisions to
 * 0..n-1 before it compiles — exactly as a fork's guard-closed sub-chain already
 * did — so the plan fits the bitset however high the counter has gone.
 *
 * <p>The routing is identical either way (that is what
 * {@link DefaultNioEngineDecisionBitsetTest} guards), so the compaction is proven
 * white-box: the compiled plan's highest decision id.
 */
class DefaultNioFlowBranchingCompactionTest extends EngineTestSupport {

    private static Stage tagged(String tag, Guard guard) {
        Function<Object, Object> fn = value -> value + tag;
        return new Stage("s" + tag, fn, false, null, null, guard == null ? List.of() : List.of(guard));
    }

    // ── the compaction itself ───────────────────────────────────────────────

    @Test
    void compactDecisionsRenumbersToZeroBasedAndRemapsGuards() {
        // Ids drawn from a counter already past the bitset limit, sparse on top.
        List<Link> links = List.of(
                new Decision(value -> true, 600, List.of()),
                new Decision(value -> false, 640, List.of(new Guard(600, true))),
                tagged(":a", new Guard(640, true)),
                tagged(":b", new Guard(600, false)));

        List<Link> compacted = AbstractChain.compactDecisions(links);

        // 600 -> 0, 640 -> 1, in first-seen order; every guard follows.
        assertEquals(0, ((Decision) compacted.get(0)).id());
        Decision second = (Decision) compacted.get(1);
        assertEquals(1, second.id());
        assertEquals(List.of(new Guard(0, true)), second.guards());
        assertEquals(List.of(new Guard(1, true)), compacted.get(2).guards());
        assertEquals(List.of(new Guard(0, false)), compacted.get(3).guards());
    }

    @Test
    void compactDecisionsIsANoOpWithoutDecisions() {
        List<Link> links = List.of(tagged(":x", null));
        List<Link> compacted = AbstractChain.compactDecisions(links);

        assertEquals(1, compacted.size());
        assertInstanceOf(Stage.class, compacted.get(0));
    }

    @Test
    void compactDecisionsIfBeyondSkipsWorkBelowTheLimit() {
        // Id 5 already fits the bitset: no scan-and-rebuild, ids untouched.
        List<Link> links = List.of(
                new Decision(value -> true, 5, List.of()),
                tagged(":a", new Guard(5, true)));

        List<Link> result = AbstractChain.compactDecisionsIfBeyond(links, 511);

        assertEquals(5, ((Decision) result.get(0)).id());   // not renumbered
        assertEquals(List.of(new Guard(5, true)), result.get(1).guards());
    }

    @Test
    void compactDecisionsIfBeyondCompactsAboveTheLimit() {
        List<Link> links = List.of(
                new Decision(value -> true, 800, List.of()),
                tagged(":a", new Guard(800, true)));

        List<Link> result = AbstractChain.compactDecisionsIfBeyond(links, 511);

        assertEquals(0, ((Decision) result.get(0)).id());   // renumbered to fit
        assertEquals(List.of(new Guard(0, true)), result.get(1).guards());
    }

    // ── the per-request pipeline uses it (the wiring) ───────────────────────

    @Test
    void aPerRequestWhenCompactsItsDecisionEvenWhenTheCounterOutgrewTheBitset() {
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        pushDecisionCounterPastTheBitset();

        NioStep<Integer, Integer> step = flow.just(4);
        ExecutionNioFlow<Integer, Integer> pipeline = (ExecutionNioFlow<Integer, Integer>) step;
        step.when(value -> value % 2 == 0)
                .then(lane -> lane.handle(value -> value * 10))
                .otherwise(lane -> lane.handle(value -> -value));

        // The one local decision drew a global id > 511, but the compiled plan
        // renumbered it to 0 — so the execution rides the bitset, not the map.
        assertEquals(0, DefaultNioEngine.planMaxDecisionId(pipeline.preparedForTest()));
    }

    @Test
    void aPerRequestMatchCompactsAllItsDecisions() {
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        pushDecisionCounterPastTheBitset();

        NioStep<Integer, Integer> step = flow.just(7);
        ExecutionNioFlow<Integer, Integer> pipeline = (ExecutionNioFlow<Integer, Integer>) step;
        step.match()
                .is(value -> value < 0, lane -> lane.handle(value -> 0))
                .is(value -> value % 2 == 0, lane -> lane.handle(value -> value * 10))
                .otherwise(lane -> lane.handle(value -> -value));

        // Two case decisions, compacted to 0 and 1 — well inside the bitset.
        assertTrue(DefaultNioEngine.planMaxDecisionId(pipeline.preparedForTest()) <= 1,
                "match() decisions were not compacted");
    }

    // ── routing is unchanged (regression) ───────────────────────────────────

    @Test
    void routingStaysCorrectThroughTheCompactedPlan() {
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        pushDecisionCounterPastTheBitset();

        for (int i = 0; i < 3; i++) {
            assertEquals(40, flow.just(4)
                    .match()
                    .is(value -> value < 0, lane -> lane.handle(value -> 0))
                    .is(value -> value % 2 == 0, lane -> lane.handle(value -> value * 10))
                    .otherwise(lane -> lane.handle(value -> -value))
                    .execute());
            assertEquals(-7, flow.just(7)
                    .match()
                    .is(value -> value < 0, lane -> lane.handle(value -> 0))
                    .is(value -> value % 2 == 0, lane -> lane.handle(value -> value * 10))
                    .otherwise(lane -> lane.handle(value -> -value))   // 7 is odd & positive
                    .execute());
        }
    }

    private void pushDecisionCounterPastTheBitset() {
        for (int i = 0; i < 600; i++) {
            engine.nextDecision();
        }
    }
}
