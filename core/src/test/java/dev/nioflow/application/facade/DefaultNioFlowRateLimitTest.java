package dev.nioflow.application.facade;

import dev.nioflow.core.facade.NioFlow;
import dev.nioflow.core.model.RateLimit;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Rate limiting per stage: a lazily-refilled token bucket (timestamps, no
 * timer thread) acquired on the virtual worker before each application.
 * Timing assertions only check LOWER bounds — throttling really happened —
 * with generous margins; upper bounds would flake under machine load.
 */
class DefaultNioFlowRateLimitTest extends EngineTestSupport {

    @Test
    void throughputIsCappedAtTheConfiguredRate() {
        // 5 permits per 100ms: 15 sequential calls = 5 burst + 10 paced at
        // 20ms each -> at least ~200ms total (asserted with 25% slack).
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        flow.handle("limited", value -> value + 1, RateLimit.of(5, Duration.ofMillis(100)));
        engine.seal();

        long start = System.nanoTime();
        for (int i = 0; i < 15; i++) {
            assertEquals(i + 1, flow.just(i).execute());
        }
        long elapsedMillis = (System.nanoTime() - start) / 1_000_000;

        assertTrue(elapsedMillis >= 150, "expected >= ~200ms of pacing, took " + elapsedMillis + "ms");
    }

    @Test
    void burstServesTheFirstPermitsWithoutWaiting() {
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        flow.handle("limited", value -> value, RateLimit.of(10, Duration.ofSeconds(5)));

        long start = System.nanoTime();
        for (int i = 0; i < 10; i++) {
            flow.just(i).execute();
        }
        long elapsedMillis = (System.nanoTime() - start) / 1_000_000;

        // 10 tokens sit in the idle bucket; pacing them would take 4.5s.
        assertTrue(elapsedMillis < 1_000, "burst should not pace, took " + elapsedMillis + "ms");
    }

    @Test
    void concurrentExecutionsShareTheBucket() {
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        flow.handle("limited", value -> value * 2, RateLimit.of(5, Duration.ofMillis(100)));
        engine.seal();

        long start = System.nanoTime();
        List<CompletableFuture<Integer>> calls = new ArrayList<>();
        for (int i = 0; i < 15; i++) {
            calls.add(flow.just(i).executeAsync());
        }
        for (int i = 0; i < 15; i++) {
            assertEquals(i * 2, calls.get(i).join());
        }
        long elapsedMillis = (System.nanoTime() - start) / 1_000_000;

        // Even in parallel, 15 executions cannot beat the shared bucket.
        assertTrue(elapsedMillis >= 150, "concurrent callers must share the rate, took " + elapsedMillis + "ms");
    }

    @Test
    void oneInstanceSharedByTwoStagesProtectsOneDownstream() {
        RateLimit shared = RateLimit.of(4, Duration.ofMillis(100));
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        flow.handle("first-hit", value -> value, shared)
                .handle("second-hit", value -> value, shared);
        engine.seal();

        long start = System.nanoTime();
        for (int i = 0; i < 6; i++) {
            flow.just(i).execute(); // 6 executions x 2 acquires = 12 tokens
        }
        long elapsedMillis = (System.nanoTime() - start) / 1_000_000;

        // 12 tokens at 4/100ms = 4 burst + 8 paced at 25ms -> >= ~200ms.
        assertTrue(elapsedMillis >= 150, "both stages must drain the same bucket, took " + elapsedMillis + "ms");
    }

    @Test
    void rateLimitedStageStillFusesAndRoutesCorrectly() {
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        flow.handle("before", value -> value + 1)
                .handle("limited", value -> value * 10, RateLimit.perSecond(1_000_000))
                .handle("after", value -> value - 2);
        engine.seal();

        assertEquals(38, flow.just(3).execute());
    }

    @Test
    void worksInsideLanes() {
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        flow.when(value -> value % 2 == 0)
                .then(lane -> lane.handle("even-limited", value -> value * 10,
                        RateLimit.perSecond(1_000_000)))
                .otherwise(lane -> lane.handle(value -> -value));

        assertEquals(40, flow.just(4).execute());
        assertEquals(-7, flow.just(7).execute());
    }

    @Test
    void invalidConfigurationsAreRejected() {
        assertThrows(IllegalArgumentException.class, () -> RateLimit.of(0, Duration.ofSeconds(1)));
        assertThrows(IllegalArgumentException.class, () -> RateLimit.of(10, Duration.ZERO));
        assertThrows(IllegalArgumentException.class, () -> RateLimit.of(10, null));
    }
}
