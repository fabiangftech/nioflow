package dev.nioflow.application.facade;

import dev.nioflow.core.facade.NioFlow;
import dev.nioflow.core.model.Link;
import dev.nioflow.core.model.Splice;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The compiled plan is an optimization, never a semantic: every scenario here
 * runs the same chain sealed (compiled) and unsealed (interpreted) and both
 * must produce identical results.
 */
class DefaultNioEngineCompiledChainTest {

    private static NioFlow<Integer, Integer> pipeline(DefaultNioEngine engine) {
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        flow.handle("plus", value -> value + 1)
                .filter(value -> value != 13)
                .handle("boom-on-fifty", value -> {
                    if (value == 50) {
                        throw new IllegalStateException("boom");
                    }
                    return value;
                })
                .recover("fallback", error -> -1)
                .handle("double", value -> value * 2);
        return flow;
    }

    @Test
    void sealedChainMatchesInterpretedResults() {
        var interpretedEngine = new DefaultNioEngine();
        var compiledEngine = new DefaultNioEngine();
        NioFlow<Integer, Integer> interpreted = pipeline(interpretedEngine);
        NioFlow<Integer, Integer> compiled = pipeline(compiledEngine);
        compiledEngine.seal();

        for (int input : new int[]{5, 49, 12, 0, -7}) {
            assertEquals(interpreted.just(input).execute(), compiled.just(input).execute(),
                    "input " + input + " diverged between interpreted and compiled");
        }
        // Filter cut (12 + 1 == 13) and recovery (49 + 1 == 50) on the compiled path.
        assertNull(compiled.just(12).execute());
        assertEquals(-2, compiled.just(49).execute());
        shutdown(interpretedEngine, compiledEngine);
    }

    @Test
    void compiledForksRouteIdenticallyToInterpreted() {
        var interpretedEngine = new DefaultNioEngine();
        var compiledEngine = new DefaultNioEngine();
        for (var engine : new DefaultNioEngine[]{interpretedEngine, compiledEngine}) {
            NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
            flow.match()
                    .is(value -> value % 2 == 0, lane -> lane.handle(value -> value * 10))
                    .is(value -> value > 10, lane -> lane.handle(value -> value + 1000))
                    .otherwise(lane -> lane.handle(value -> -value))
                    .handle("main", value -> value + 1);
        }
        compiledEngine.seal();

        for (int input : new int[]{4, 15, 3, 20, -8}) {
            assertEquals(interpretedEngine.call(input, new ConcurrentHashMap<>()).join(),
                    compiledEngine.call(input, new ConcurrentHashMap<>()).join(),
                    "input " + input + " took different lanes");
        }
        shutdown(interpretedEngine, compiledEngine);
    }

    @Test
    void spliceRecompilesThePlanForTheNextCall() {
        var engine = new DefaultNioEngine();
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        flow.handle("mutable", value -> value + 1);
        engine.seal();

        assertEquals(6, flow.just(5).execute());

        engine.splice("mutable", Splice.REPLACE, List.of(
                new dev.nioflow.core.model.Stage("mutable", value -> (int) value + 100, false, null, null, List.of())));

        assertEquals(105, flow.just(5).execute()); // the new plan, compiled once at splice time
        shutdown(engine);
    }

    @Test
    void executionLocalLinksFallBackToInterpreting() {
        var engine = new DefaultNioEngine();
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        flow.handle("shared", value -> value + 1);
        engine.seal();

        // Local links build a per-request chain: identity no longer matches the
        // compiled plan and the execution interprets — same result, no plan.
        assertEquals(12, flow.just(5).handle(value -> value * 2).execute());
        // A plain execution over the shared chain still uses the plan.
        assertEquals(6, flow.just(5).execute());
        shutdown(engine);
    }

    @Test
    void releaseAndAppendInvalidateThePlanSafely() {
        var engine = new DefaultNioEngine();
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        flow.handle("base", value -> value + 1);
        engine.seal();
        assertEquals(6, flow.just(5).execute());

        engine.release();
        flow.handle("extra", value -> value * 3); // appends: invalidates the plan
        assertEquals(18, flow.just(5).execute()); // interpreted, correct

        engine.seal(); // recompiled
        assertEquals(18, flow.just(5).execute());
        shutdown(engine);
    }

    /**
     * The plan is compared by identity on the hot path, but it is a record:
     * its value semantics must look at what the arrays CONTAIN, not at which
     * array object they happen to be.
     */
    @Test
    void thePlanIsAValueOverItsArrayContents() {
        List<Link> links = List.of(EngineTestSupport.stage("a", value -> value),
                EngineTestSupport.stage("b", value -> value));

        DefaultNioEngine.CompiledChain plan = DefaultNioEngine.CompiledChain.compile(links);
        DefaultNioEngine.CompiledChain same = DefaultNioEngine.CompiledChain.compile(links);
        DefaultNioEngine.CompiledChain other = DefaultNioEngine.CompiledChain.compile(
                List.of(EngineTestSupport.stage("a", value -> value)));

        assertEquals(plan, same);                          // distinct array instances, same contents
        assertEquals(plan.hashCode(), same.hashCode());
        assertNotEquals(plan, other);                      // a different chain is a different plan
        assertTrue(plan.toString().contains("runEnds="), plan::toString);
    }

    private static void shutdown(DefaultNioEngine... engines) {
        for (DefaultNioEngine engine : engines) {
            engine.shutdown(Duration.ofMillis(100));
        }
    }
}
