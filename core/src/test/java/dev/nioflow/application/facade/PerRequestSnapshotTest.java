package dev.nioflow.application.facade;

import dev.nioflow.core.facade.NioFlow;
import dev.nioflow.core.facade.NioStep;
import dev.nioflow.core.facade.PreparedChain;
import dev.nioflow.core.model.Link;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * The dynamic {@code just()}-build path (RFC 0011 Part B) compiles its local
 * chain into a snapshot ONCE and caches it: a re-executed pipeline — a
 * re-subscribing Mono, a second executeAsync — reuses the plan instead of
 * copying and rescanning the chain again.
 */
class PerRequestSnapshotTest {

    /** Counts how many times a per-request chain is compiled into a snapshot. */
    private static final class CountingEngine extends DefaultNioEngine {
        final AtomicInteger planForCalls = new AtomicInteger();

        @Override
        public PreparedChain planFor(List<Link> chain) {
            planForCalls.incrementAndGet();
            return super.planFor(chain);
        }
    }

    @Test
    void aReExecutedDynamicPipelineCompilesItsSnapshotOnce() {
        CountingEngine engine = new CountingEngine();
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);

        NioStep<Integer, Integer> pipeline = flow.just(5).handle("plus", value -> value + 1);

        assertEquals(6, pipeline.executeAsync().join());
        assertEquals(6, pipeline.executeAsync().join());
        assertEquals(6, pipeline.execute());

        // Three runs of the same built pipeline, one compile.
        assertEquals(1, engine.planForCalls.get());
    }

    @Test
    void aPipelineWithoutLocalLinksNeverCompilesASnapshot() {
        CountingEngine engine = new CountingEngine();
        NioFlow<Integer, Integer> flow = DefaultNioFlow.<Integer, Integer>from(Integer.class, engine)
                .handle("shared", value -> value * 2);

        // No local links: the run dispatches off the engine's own chain (the
        // sealed shared chain matches its compiled plan by identity), so there
        // is no per-request snapshot to compile.
        assertEquals(10, flow.just(5).execute());
        assertEquals(10, flow.just(5).executeAsync().join());
        assertEquals(0, engine.planForCalls.get());
    }
}
