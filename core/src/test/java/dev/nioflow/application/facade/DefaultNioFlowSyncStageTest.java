package dev.nioflow.application.facade;

import dev.nioflow.core.facade.ChainValidationException;
import dev.nioflow.core.facade.NioFlow;
import dev.nioflow.core.model.Retry;
import dev.nioflow.core.model.Stage;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * handleSync: opt-in boss-inlined stages for pure-CPU, sub-microsecond
 * functions — they skip both thread hops. Contract mirrors Decision
 * predicates: run on the boss, a throw fails the value (recoverable), never
 * the boss task; timeout/retry are structurally rejected.
 */
class DefaultNioFlowSyncStageTest extends EngineTestSupport {

    @Test
    void syncStageRunsInlineOnTheBoss() {
        var stageThread = new AtomicReference<Thread>();
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        flow.handleSync("double", value -> {
            stageThread.set(Thread.currentThread());
            return value * 2;
        });

        assertEquals(14, flow.just(7).execute());
        assertTrue(stageThread.get().getName().startsWith("nio-flow-boss-"),
                "sync stages must run on the boss, ran on " + stageThread.get().getName());
    }

    @Test
    void chainOfSyncStagesNeverTouchesAWorker() {
        var threads = new java.util.concurrent.CopyOnWriteArraySet<String>();
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        for (int i = 0; i < 5; i++) {
            flow.handleSync(value -> {
                threads.add(Thread.currentThread().getName());
                return value + 1;
            });
        }
        engine.seal();

        assertEquals(15, flow.just(10).execute());
        assertEquals(1, threads.size(), "one boss walks the whole chain: " + threads);
        assertTrue(threads.iterator().next().startsWith("nio-flow-boss-"));
    }

    @Test
    void syncStageAfterAWorkerStageFusesIntoItsRun() {
        var workerThread = new AtomicReference<Thread>();
        var syncThread = new AtomicReference<Thread>();
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        flow.handle("worker", value -> {
            workerThread.set(Thread.currentThread());
            return value + 1;
        }).handleSync("riding", value -> {
            syncThread.set(Thread.currentThread());
            return value * 2;
        });
        engine.seal();

        assertEquals(12, flow.just(5).execute());
        // Fused into the preceding worker run (2 hops total) instead of
        // resuming on the boss just to inline one function.
        assertSame(workerThread.get(), syncThread.get());
        assertNotSame(Thread.currentThread(), syncThread.get());
    }

    @Test
    void throwingSyncStageFailsTheValueAndIsRecoverable() {
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        flow.handleSync("boom", value -> {
            if (value == 13) {
                throw new IllegalStateException("boom");
            }
            return value;
        }).recover("net", error -> -1);

        assertEquals(-1, flow.just(13).execute());
        assertEquals(7, flow.just(7).execute());
    }

    @Test
    void throwingSyncStageWithoutRecoveryFailsTheFutureInsteadOfHangingIt() {
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        flow.handleSync("boom", value -> {
            throw new IllegalStateException("boom");
        });

        CompletableFuture<Integer> result = flow.just(1).executeAsync().orTimeout(5, TimeUnit.SECONDS);

        assertThrows(Exception.class, result::join);
    }

    @Test
    void syncStagesInsideLanesRespectGuards() {
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        flow.when(value -> value % 2 == 0)
                .then(lane -> lane.handleSync(value -> value * 10))
                .otherwise(lane -> lane.handleSync(value -> -value))
                .handleSync("tail", value -> value + 1);
        engine.seal();

        assertEquals(41, flow.just(4).execute());
        assertEquals(-6, flow.just(7).execute());
    }

    @Test
    void sealRejectsSyncStagesWithTimeoutOrRetry() {
        engine.append(new Stage("cut-me", value -> value, true, Duration.ofMillis(10), null, List.of()));
        engine.append(new Stage("retry-me", value -> value, true, null, Retry.of(3, Duration.ofMillis(1)), List.of()));

        ChainValidationException rejection = assertThrows(ChainValidationException.class, engine::seal);

        assertEquals(2, rejection.problems().size());
        assertTrue(rejection.problems().get(0).contains("timeout"));
        assertTrue(rejection.problems().get(1).contains("retry"));
    }

    @Test
    void syncStageIsASpliceAnchorLikeAnyOtherStage() {
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        flow.handleSync("anchor", value -> value + 1);
        engine.seal();
        assertEquals(6, flow.just(5).execute());

        engine.splice("anchor", dev.nioflow.core.model.Splice.REPLACE, List.of(
                new Stage("anchor", value -> (int) value + 100, true, null, null, List.of())));

        assertEquals(105, flow.just(5).execute());
    }
}
