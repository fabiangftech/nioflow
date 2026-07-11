package dev.nioflow.application.facade;

import dev.nioflow.core.facade.NioFlow;
import dev.nioflow.core.model.Decision;
import dev.nioflow.core.model.Guard;
import dev.nioflow.core.model.Stage;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Decisions live in a per-execution bitset (2 bits per id) sized by the
 * chain's highest Decision id; ids past the bitset limit overflow into a map.
 * Either representation must be observationally identical to the old
 * HashMap: an unrecorded decision fails any guard on it.
 */
class DefaultNioEngineDecisionBitsetTest extends EngineTestSupport {

    private static Stage guarded(String name, String tag, int decision, boolean expected) {
        return new Stage(name, value -> value + tag, false, null, null,
                List.of(new Guard(decision, expected)));
    }

    @Test
    void decisionIdsAcrossWordBoundariesRouteCorrectly() {
        // Ids 31 and 32 land on different words of the bitset; both must
        // record and read back independently.
        engine.append(new Decision(value -> true, 31, List.of()));
        engine.append(new Decision(value -> false, 32, List.of()));
        engine.append(guarded("on-31", ":31", 31, true));
        engine.append(guarded("on-32", ":32", 32, false));

        assertEquals("x:31:32", engine.call("x", new ConcurrentHashMap<>()).join());
    }

    @Test
    void guardOnUnrecordedDecisionSkipsTheLink() {
        // Id 7 sits inside the bitset range (max id is 9) but never records:
        // the guard must fail exactly like the old map returning null.
        engine.append(new Decision(value -> true, 9, List.of()));
        engine.append(guarded("dangling", ":dangling", 7, true));
        engine.append(guarded("recorded", ":recorded", 9, true));

        assertEquals("x:recorded", engine.call("x", new ConcurrentHashMap<>()).join());
    }

    @Test
    void guardBeyondTheBitsetRangeSkipsTheLink() {
        engine.append(new Decision(value -> true, 0, List.of()));
        engine.append(guarded("out-of-range", ":out", 5000, true));
        engine.append(guarded("in-range", ":in", 0, true));

        assertEquals("x:in", engine.call("x", new ConcurrentHashMap<>()).join());
    }

    @Test
    void negativeGuardIdSkipsTheLinkWithoutFailing() {
        engine.append(new Decision(value -> true, 0, List.of()));
        engine.append(guarded("negative", ":neg", -1, true));

        assertEquals("x", engine.call("x", new ConcurrentHashMap<>()).join());
    }

    @Test
    void idsPastTheBitsetLimitFallBackToTheOverflowMap() {
        // Max id 100_000 disables the bitset; routing must be identical.
        engine.append(new Decision(value -> (int) value > 10, 100_000, List.of()));
        engine.append(new Stage("big", value -> "big:" + value, false, null, null,
                List.of(new Guard(100_000, true))));
        engine.append(new Stage("small", value -> "small:" + value, false, null, null,
                List.of(new Guard(100_000, false))));

        assertEquals("big:42", engine.call(42, new ConcurrentHashMap<>()).join());
        assertEquals("small:3", engine.call(3, new ConcurrentHashMap<>()).join());
    }

    @Test
    void mixedSmallAndHugeIdsRouteThroughTheOverflowMapTogether() {
        engine.append(new Decision(value -> true, 2, List.of()));
        engine.append(new Decision(value -> false, 999_999, List.of()));
        engine.append(guarded("small-id", ":small", 2, true));
        engine.append(guarded("huge-id", ":huge", 999_999, false));

        assertEquals("x:small:huge", engine.call("x", new ConcurrentHashMap<>()).join());
    }

    @Test
    void sealedAndUnsealedChainsRouteIdentically() {
        int decision = engine.nextDecision();
        engine.append(new Decision(value -> (int) value % 2 == 0, decision, List.of()));
        engine.append(new Stage("even", value -> "even:" + value, false, null, null,
                List.of(new Guard(decision, true))));
        engine.append(new Stage("odd", value -> "odd:" + value, false, null, null,
                List.of(new Guard(decision, false))));

        assertEquals("even:4", engine.call(4, new ConcurrentHashMap<>()).join());
        assertEquals("odd:7", engine.call(7, new ConcurrentHashMap<>()).join());

        engine.seal();

        assertEquals("even:4", engine.call(4, new ConcurrentHashMap<>()).join());
        assertEquals("odd:7", engine.call(7, new ConcurrentHashMap<>()).join());
    }

    @Test
    void perRequestForksKeepRoutingAfterTheEngineCounterOutgrowsTheBitset() {
        // A long-lived engine keeps incrementing the decision counter under
        // per-request forks; executions whose ids passed the limit must route
        // exactly as before through the overflow map.
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        for (int i = 0; i < 600; i++) {
            engine.nextDecision();
        }

        for (int i = 0; i < 3; i++) {
            assertEquals(40, flow.just(4)
                    .when(value -> value % 2 == 0)
                    .then(lane -> lane.handle(value -> value * 10))
                    .otherwise(lane -> lane.handle(value -> -value))
                    .execute());
            assertEquals(-7, flow.just(7)
                    .when(value -> value % 2 == 0)
                    .then(lane -> lane.handle(value -> value * 10))
                    .otherwise(lane -> lane.handle(value -> -value))
                    .execute());
        }
    }
}
