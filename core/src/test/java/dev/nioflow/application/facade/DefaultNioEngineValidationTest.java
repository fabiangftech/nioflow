package dev.nioflow.application.facade;

import dev.nioflow.core.facade.ChainValidationException;
import dev.nioflow.core.facade.NioFlow;
import dev.nioflow.core.model.Guard;
import dev.nioflow.core.model.Splice;
import dev.nioflow.core.model.Stage;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DefaultNioEngineValidationTest extends EngineTestSupport {

    @Test
    void duplicateAnchorNamesFailSeal() {
        engine.append(stage("same", value -> value));
        engine.append(stage("same", value -> value));

        var failure = assertThrows(ChainValidationException.class, engine::seal);

        assertEquals(1, failure.problems().size());
        assertTrue(failure.problems().get(0).contains("duplicates the anchor name 'same'"));
    }

    @Test
    void danglingGuardFailsSeal() {
        engine.append(new Stage("orphan", value -> value, false, null, null,
                List.of(new Guard(99, true))));

        var failure = assertThrows(ChainValidationException.class, engine::seal);

        assertTrue(failure.problems().get(0).contains("decision 99 which is not declared upstream"));
    }

    @Test
    void contradictoryGuardsFailSeal() {
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        flow.when(value -> value > 0)
                .then(lane -> lane.handle("positive", value -> value));
        // Hand-built link demanding the same decision to be true AND false.
        engine.append(new Stage("impossible", value -> value, false, null, null,
                List.of(new Guard(0, true), new Guard(0, false))));

        var failure = assertThrows(ChainValidationException.class, engine::seal);

        assertTrue(failure.problems().get(0).contains("contradictory guards on decision 0"));
    }

    @Test
    void deadRecoveryFailsSeal() {
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        flow.recover("too-early", error -> -1)
                .handle("work", value -> value + 1);

        var failure = assertThrows(ChainValidationException.class, engine::seal);

        assertTrue(failure.problems().get(0).contains("nothing fallible upstream"));
    }

    @Test
    void invalidSpliceIsRejectedAndTheChainStaysIntact() {
        engine.append(stage("head", value -> (int) value + 1));
        engine.append(stage("tail", value -> (int) value * 2));
        engine.seal();

        // Splicing in a stage that duplicates an existing anchor is rejected...
        assertThrows(ChainValidationException.class, () ->
                engine.splice("head", Splice.AFTER, List.of(stage("tail", value -> value))));

        // ...and the previous chain (and its compiled plan) keeps serving.
        assertEquals(2, engine.chain().size());
        assertEquals(12, engine.call(5, new java.util.concurrent.ConcurrentHashMap<>()).join());
    }

    @Test
    void aRealisticForkedChainSealsClean() {
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        flow.filter(value -> value != 0)
                .handle("normalize", value -> Math.abs(value))
                .when(value -> value > 10)
                    .then(lane -> lane
                            .handle("big", value -> value * 2)
                            .recover("big-fallback", error -> -1))
                    .otherwise(lane -> lane
                            .handle("small", value -> value + 1))
                .match()
                    .is(value -> value % 2 == 0, lane -> lane.handle("even", value -> value))
                    .otherwise(lane -> lane.handle("odd", value -> value))
                .recover("net", error -> -99);

        engine.seal(); // must not throw

        assertEquals(22, engine.call(11, new java.util.concurrent.ConcurrentHashMap<>()).join());
    }
}
