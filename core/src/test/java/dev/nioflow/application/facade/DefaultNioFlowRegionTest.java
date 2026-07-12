package dev.nioflow.application.facade;

import dev.nioflow.core.facade.ChainValidationException;
import dev.nioflow.core.facade.Segment;
import dev.nioflow.core.model.Guard;
import dev.nioflow.core.model.Splice;
import dev.nioflow.core.model.Stage;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Splice regions: use(name, segment) remembers the embedded span (by link
 * identity, immune to edits elsewhere) and replaceRegion/spliceRegion swap
 * the WHOLE span atomically — one chain swap, one validation, one
 * recompile — re-pointing the region at the new links so it stays
 * swappable.
 */
class DefaultNioFlowRegionTest {

    private DefaultNioFlow<Integer, Integer> flow;
    private DefaultNioEngine engine;

    private void buildFlow() {
        engine = new DefaultNioEngine();
        flow = DefaultNioFlow.from(Integer.class, engine);
        Segment<Integer, Integer> pricing = lane -> lane
                .handle("base-price", value -> value * 10)
                .handle("discount", value -> value - 5);
        flow.handle("head", value -> value + 1)
                .use("pricing", pricing)
                .handle("tail", value -> value * 2);
    }

    @Test
    void namedRegionSwapsAtomicallyLeavingNeighborsUntouched() {
        buildFlow();
        engine.seal();
        assertEquals(90, flow.just(4).execute()); // ((4+1)*10 - 5) * 2

        Segment<Integer, Integer> flatPricing = lane -> lane.handle("flat", value -> value + 100);
        flow.replaceRegion("pricing", flatPricing);

        assertEquals(210, flow.just(4).execute()); // ((4+1) + 100) * 2: head and tail intact
    }

    @Test
    void regionStaysSwappableAfterASwap() {
        buildFlow();
        flow.replaceRegion("pricing", lane -> lane.handle(value -> value + 100));
        assertEquals(210, flow.just(4).execute());

        flow.replaceRegion("pricing", lane -> lane.handle(value -> value + 1000));
        assertEquals(2010, flow.just(4).execute());
    }

    @Test
    void replacementSegmentsWithForksRouteCorrectly() {
        buildFlow();
        // The recorded fork draws decision ids from the LIVE engine: its
        // guards must not collide with any pre-existing decision.
        flow.replaceRegion("pricing", lane -> lane
                .when(value -> value % 2 == 0)
                .then(inner -> inner.handle(value -> value * 100))
                .otherwise(inner -> inner.handle(value -> -value)));
        engine.seal();

        assertEquals(1200, flow.just(5).execute()); // (5+1)=6 even -> 600 * 2
        assertEquals(-14, flow.just(6).execute());  // (6+1)=7 odd  -> -7 * 2
    }

    @Test
    void unknownAndDuplicateRegionsAreRejected() {
        buildFlow();
        assertThrows(IllegalArgumentException.class,
                () -> flow.replaceRegion("nope", lane -> lane.handle(value -> value)));
        assertThrows(IllegalArgumentException.class,
                () -> flow.use("pricing", lane -> lane.handle(value -> value)));
    }

    @Test
    void sealedChainRejectsAnInvalidSwapAndKeepsTheOldRegion() {
        buildFlow();
        engine.seal();
        assertEquals(90, flow.just(4).execute());

        // Hand-built replacement with a dangling guard: validation must
        // reject it and the previous chain (and region) must survive.
        List<dev.nioflow.core.model.Link> broken = List.of(
                new Stage("orphan", value -> value, false, null, null, List.of(new Guard(999_999, true))));
        assertThrows(ChainValidationException.class, () -> engine.spliceRegion("pricing", broken));

        assertEquals(90, flow.just(4).execute());
        flow.replaceRegion("pricing", lane -> lane.handle(value -> value + 100));
        assertEquals(210, flow.just(4).execute());
    }

    @Test
    void aBoundaryEditedAwayBySingleLinkSpliceFailsClearly() {
        buildFlow();
        engine.splice("base-price", Splice.REPLACE, List.of(
                new Stage("base-price-v2", value -> (int) value * 20, false, null, null, List.of())));

        var failure = assertThrows(IllegalStateException.class,
                () -> flow.replaceRegion("pricing", lane -> lane.handle(value -> value)));
        assertTrue(failure.getMessage().contains("pricing"));
    }

    @Test
    void emptyReplacementRetiresTheRegion() {
        buildFlow();
        engine.spliceRegion("pricing", List.of());

        assertEquals(10, flow.just(4).execute()); // (4+1) * 2: region gone
        assertThrows(IllegalArgumentException.class, () -> engine.spliceRegion("pricing", List.of()));
    }

    // Registering a region from an execution no longer compiles: use(region,
    // segment) lives on NioFlow (the shared definition), not on NioStep.

    @Test
    void aRegionWhoseSegmentAppendsNothingIsRejected() {
        DefaultNioFlow<String, String> flow = DefaultNioFlow.from(String.class);

        IllegalArgumentException empty = assertThrows(IllegalArgumentException.class,
                () -> flow.use("empty", lane -> lane));

        assertTrue(empty.getMessage().contains("empty"), empty::getMessage);
        flow.close();
    }
}
