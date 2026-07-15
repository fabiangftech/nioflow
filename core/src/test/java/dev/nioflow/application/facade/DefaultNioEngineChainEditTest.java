package dev.nioflow.application.facade;

import dev.nioflow.core.model.Background;
import dev.nioflow.core.model.Batch;
import dev.nioflow.core.model.Decision;
import dev.nioflow.core.model.FanOut;
import dev.nioflow.core.model.Filter;
import dev.nioflow.core.model.Recovery;
import dev.nioflow.core.model.Splice;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DefaultNioEngineChainEditTest extends EngineTestSupport {

    @Test
    void spliceAtRuntimeDoesNotAffectInFlightCalls() {
        var gate = new CountDownLatch(1);
        engine.append(stage("slow", value -> {
            try {
                gate.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return value + ":slow";
        }));
        engine.seal();

        CompletableFuture<Object> inFlight = engine.call("a", new ConcurrentHashMap<>());
        engine.splice("slow", Splice.AFTER, List.of(stage("extra", value -> value + ":extra")));
        gate.countDown();

        assertEquals("a:slow", inFlight.join());
        assertEquals("b:slow:extra", engine.call("b", new ConcurrentHashMap<>()).join());
    }

    @Test
    void sealBlocksAppendUntilRelease() {
        engine.append(stage("first", value -> value));
        engine.seal();

        assertThrows(IllegalStateException.class, () -> engine.append(stage("second", value -> value)));

        engine.release();
        engine.append(stage("second", value -> value));
        assertEquals(2, engine.chain().size());
    }

    /**
     * Splice anchors are names, and every named link kind carries one: a
     * runtime edit can hang off a background, a recovery, a fan-out or a
     * batch exactly like off a stage.
     */
    @Test
    void everyNamedLinkKindWorksAsASpliceAnchor() {
        engine.append(new Background("effect", value -> {
        }, List.of()));
        engine.append(new Recovery("rescue", error -> "rescued", List.of()));
        engine.append(new FanOut("fan", List.of(value -> value), parts -> parts.get(0), false, List.of()));
        engine.append(new Batch("bulk", 1, Duration.ofMillis(50), values -> values, List.of()));

        engine.splice("effect", Splice.AFTER, List.of(stage("afterBackground", value -> value + ":b")));
        engine.splice("rescue", Splice.AFTER, List.of(stage("afterRecovery", value -> value + ":r")));
        engine.splice("fan", Splice.AFTER, List.of(stage("afterFanOut", value -> value + ":f")));
        engine.splice("bulk", Splice.AFTER, List.of(stage("afterBatch", value -> value + ":t")));

        // Nothing fails, so the Recovery itself never applies — but the stage
        // spliced after it is a plain link on the main line and does run.
        assertEquals("x:b:r:f:t", engine.call("x", null).join());
    }

    @Test
    void spliceOnAnUnknownAnchorIsRejected() {
        engine.append(stage("first", value -> value));

        IllegalArgumentException rejected = assertThrows(IllegalArgumentException.class,
                () -> engine.splice("ghost", Splice.AFTER, List.of(stage("second", value -> value))));

        assertEquals(1, engine.chain().size());
        assertTrue(rejected.getMessage().contains("ghost"), rejected::getMessage);
    }

    @Test
    void spliceBeforeAnAnchorRunsTheNewLinksFirst() {
        engine.append(stage("anchor", value -> value + ":anchor"));

        engine.splice("anchor", Splice.BEFORE, List.of(stage("prelude", value -> value + ":prelude")));

        assertEquals("x:prelude:anchor", engine.call("x", null).join());
    }

    /** Unnamed links (a Decision, a Filter) are simply stepped over by the anchor scan. */
    @Test
    void theAnchorScanStepsOverLinksThatCarryNoName() {
        engine.append(new Decision(value -> true, 0, List.of()));
        engine.append(new Filter(value -> true, List.of()));
        engine.append(stage("anchor", value -> value + ":anchor"));

        engine.splice("anchor", Splice.AFTER, List.of(stage("tail", value -> value + ":tail")));

        assertEquals("x:anchor:tail", engine.call("x", null).join());
    }
}
