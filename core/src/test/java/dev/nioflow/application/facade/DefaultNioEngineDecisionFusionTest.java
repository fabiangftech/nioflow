package dev.nioflow.application.facade;

import dev.nioflow.core.facade.NioFlow;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * Fusion across recorded decisions: on compiled chains the static fusion
 * window extends through GUARDED non-fusable links (a match() case's
 * Decision, skipped lanes), because the per-execution scan steps over
 * guard-failed links — a run no longer ends at a Decision that this
 * execution's recorded decisions already ruled out. Semantics must be
 * untouched: skipped Decisions never evaluate, passing ones still end the
 * run, and lane filters/recoveries keep their positional behavior.
 */
class DefaultNioEngineDecisionFusionTest extends EngineTestSupport {

    @Test
    void firstCaseLaneFusesWithTheTailInOneWorkerTask() {
        var laneThread = new AtomicReference<Thread>();
        var tailThread = new AtomicReference<Thread>();
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        // The second case's Decision carries guards (case 1 must be false):
        // it is the link the case-1 window must now extend through.
        flow.match()
                .is(value -> value % 2 == 0, lane -> lane.handle(value -> {
                    laneThread.set(Thread.currentThread());
                    return value * 10;
                }))
                .is(value -> value > 100, lane -> lane.handle(value -> value + 1000))
                .otherwise(lane -> lane.handle(value -> -value))
                .handle("tail", value -> {
                    tailThread.set(Thread.currentThread());
                    return value + 1;
                });
        engine.seal();

        assertEquals(41, flow.just(4).execute());
        // One fused dispatch = one virtual worker thread for lane AND tail;
        // without fusion across the case-2 Decision they are two tasks.
        assertSame(laneThread.get(), tailThread.get());
    }

    @Test
    void skippedCaseDecisionIsNeverEvaluatedWhenFusedAcross() {
        var secondCaseEvaluations = new AtomicInteger();
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        flow.match()
                .is(value -> value % 3 == 0, lane -> lane.handle(value -> value * 2))
                .is(value -> {
                    secondCaseEvaluations.incrementAndGet();
                    return value % 3 == 1;
                }, lane -> lane.handle(value -> value * 3))
                .otherwise(lane -> lane.handle(value -> value * 5))
                .handle("tail", value -> value + 1);
        engine.seal();

        assertEquals(19, flow.just(9).execute());  // case 1: 9 * 2 + 1
        assertEquals(0, secondCaseEvaluations.get(), "first-match-wins: case 2 must not be evaluated");
        assertEquals(22, flow.just(7).execute());  // case 2: 7 * 3 + 1
        assertEquals(26, flow.just(5).execute());  // otherwise: 5 * 5 + 1
        assertEquals(2, secondCaseEvaluations.get(), "case 2 evaluates once per route that reaches it");
    }

    @Test
    void passingNestedDecisionStillEndsTheRunAndRoutesTheInnerFork() {
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        flow.when(value -> value > 5)
                .then(lane -> lane
                        .when(value -> value % 2 == 0)
                        .then(inner -> inner.handle(value -> value + 10))
                        .otherwise(inner -> inner.handle(value -> value + 20)))
                .otherwise(lane -> lane.handle(value -> value + 30))
                .handle("tail", value -> value * 100);
        engine.seal();

        assertEquals(1600, flow.just(6).execute()); // big and even
        assertEquals(2700, flow.just(7).execute()); // big and odd
        assertEquals(3200, flow.just(2).execute()); // small: inner fork never evaluated
    }

    @Test
    void laneFilterKeepsCuttingInsideTheExtendedWindow() {
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        flow.match()
                .is(value -> value % 2 == 0, lane -> lane
                        .handle(value -> value * 10)
                        .filter(value -> value < 100))
                .otherwise(lane -> lane.handle(value -> -value))
                .handle("tail", value -> value + 1);
        engine.seal();

        assertEquals(41, flow.just(4).execute());   // passes the lane filter, tail applies
        assertNull(flow.just(20).execute());        // 200 cut by the fused lane filter
        assertEquals(-6, flow.just(7).execute());   // otherwise route untouched
    }

    @Test
    void tailRecoveryCatchesAFailureFusedAcrossTheSkippedCases() {
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        flow.match()
                .is(value -> value % 2 == 0, lane -> lane.handle(value -> {
                    throw new IllegalStateException("lane boom");
                }))
                .otherwise(lane -> lane.handle(value -> -value))
                .recover("net", error -> -999)
                .handle("tail", value -> value + 1);
        engine.seal();

        assertEquals(-998, flow.just(4).execute()); // lane failure → recovery → tail
        assertEquals(-6, flow.just(7).execute());   // happy otherwise route
    }

    @Test
    void compiledRoutesMatchInterpretedOnEveryPath() {
        var interpretedEngine = new DefaultNioEngine();
        NioFlow<Integer, Integer> interpreted = forkedPipeline(interpretedEngine);
        NioFlow<Integer, Integer> compiled = forkedPipeline(engine);
        engine.seal();

        try {
            for (int input = -3; input <= 12; input++) {
                assertEquals(interpreted.just(input).execute(), compiled.just(input).execute(),
                        "input " + input + " diverged between interpreted and compiled");
            }
        } finally {
            interpretedEngine.shutdown(java.time.Duration.ofMillis(100));
        }
    }

    private static NioFlow<Integer, Integer> forkedPipeline(DefaultNioEngine engine) {
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        flow.match()
                .is(value -> value % 3 == 0, lane -> lane.handle(value -> value * 2))
                .is(value -> value % 3 == 1, lane -> lane
                        .handle(value -> value * 3)
                        .filter(value -> value != 12))
                .otherwise(lane -> lane.handle(value -> value * 5))
                .handle("tail-1", value -> value + 1)
                .handle("tail-2", value -> value * 2);
        return flow;
    }
}
