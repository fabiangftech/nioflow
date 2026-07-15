package dev.nioflow.application.facade;

import dev.nioflow.core.facade.Cancellable;
import dev.nioflow.core.facade.NioFlow;
import dev.nioflow.core.model.Retry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * RFC 0013 — a run of consecutive unguarded AsyncStages is driven from the
 * worker side, touching the boss once for the whole run. The driver is a second
 * execution driver, so these tests pin the invariants that matter: it produces
 * the same result as single dispatch, a failure mid-run recovers from the right
 * place, cancellation runs NO stage after the cut, and per-stage timeout/retry
 * still work inside the run.
 */
class DefaultNioFlowAsyncStageFusionTest {

    @Test
    void aFusedAsyncRunProducesTheSameResultAsSingleDispatch() {
        var engine = new DefaultNioEngine();
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        flow.handleAsync("a", value -> done(value + 1))
                .handleAsync("b", value -> done(value * 2))
                .handleAsync("c", value -> done(value - 3))
                .handleAsync("d", value -> done(value * 5));
        engine.seal(); // compiled → the four async stages fuse into one run

        assertEquals(((1 + 1) * 2 - 3) * 5, flow.just(1).execute());
        engine.shutdown(Duration.ofMillis(200));
    }

    @Test
    void stagesRunInOrderInsideTheRun() {
        var order = new CopyOnWriteArrayList<String>();
        var engine = new DefaultNioEngine();
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        flow.handleAsync("a", value -> {
                    order.add("a");
                    return done(value);
                })
                .handleAsync("b", value -> {
                    order.add("b");
                    return done(value);
                })
                .handleAsync("c", value -> {
                    order.add("c");
                    return done(value);
                });
        engine.seal();

        flow.just(0).execute();
        assertEquals(List.of("a", "b", "c"), new ArrayList<>(order));
        engine.shutdown(Duration.ofMillis(200));
    }

    @Test
    void aFailureMidRunRecoversFromRightAfterTheFailingStage() {
        var reached = new CopyOnWriteArrayList<String>();
        var engine = new DefaultNioEngine();
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        flow.handleAsync("a", value -> done(value + 1))
                .handleAsync("b", value -> CompletableFuture.failedFuture(new IllegalStateException("b down")))
                .handleAsync("c", value -> {
                    reached.add("c-ran");   // must NOT run: c is upstream-continuation after recover
                    return done(value + 100);
                })
                .recover("net", error -> -1);
        engine.seal();

        // recover catches b's failure; the flow continues AFTER the recover with
        // the recovered value. c sits between b and recover, so it is skipped.
        assertEquals(-1, flow.just(1).execute());
        assertTrue(reached.isEmpty(), "stage after the failing one must not run");
        engine.shutdown(Duration.ofMillis(200));
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 2, 3})
    void cancellingMidRunRunsNoStageAfterTheCut(int cut) throws Exception {
        int stageCount = 4;
        var invoked = new CopyOnWriteArrayList<Integer>();
        List<CompletableFuture<Integer>> gates = new ArrayList<>();
        List<CountDownLatch> invokedLatch = new ArrayList<>();
        for (int i = 0; i < stageCount; i++) {
            gates.add(new CompletableFuture<>());
            invokedLatch.add(new CountDownLatch(1));
        }

        var engine = new DefaultNioEngine();
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        NioFlow<Integer, Integer> building = flow;
        for (int i = 0; i < stageCount; i++) {
            int idx = i;
            building = building.handleAsync("s" + i, value -> {
                invoked.add(idx);
                invokedLatch.get(idx).countDown();
                return gates.get(idx);   // parks the run until this gate resolves
            });
        }
        engine.seal();

        Cancellable<Integer> handle = flow.just(0).executeCancellable();
        // Walk the run up to the cut: resolve each earlier stage so the next
        // invokes, then wait until the cut stage is in flight.
        for (int i = 0; i < cut; i++) {
            assertTrue(invokedLatch.get(i).await(2, TimeUnit.SECONDS));
            gates.get(i).complete(i + 1);
        }
        assertTrue(invokedLatch.get(cut).await(2, TimeUnit.SECONDS));

        // Cut here: the in-flight call is cancelled and the execution ends.
        handle.cancel();
        CompletableFuture<Integer> future = handle.future();
        assertThrows(CancellationException.class, future::join);

        // The execution is finished, so the driver invoked no stage past the cut.
        assertEquals(rangeInclusive(cut), new ArrayList<>(invoked));
        engine.shutdown(Duration.ofMillis(200));
    }

    @Test
    void timeoutAndRetryWorkOnAStageInsideTheRun() {
        var attempts = new AtomicInteger();
        var engine = new DefaultNioEngine();
        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        flow.handleAsync("a", value -> done(value + 1))
                // First attempt never completes → the per-stage timeout fires and
                // the retry re-invokes; the second attempt resolves at once.
                .handleAsync("b", value -> attempts.getAndIncrement() == 0
                                ? new CompletableFuture<>()   // hangs → times out
                                : done(value * 10),
                        Duration.ofMillis(80), new Retry(2, Duration.ofMillis(10), 1.0))
                .handleAsync("c", value -> done(value + 5));
        engine.seal();

        assertEquals((1 + 1) * 10 + 5, flow.just(1).execute());
        assertEquals(2, attempts.get());   // timed out once, then succeeded
        engine.shutdown(Duration.ofMillis(500));
    }

    private static CompletionStage<Integer> done(int value) {
        return CompletableFuture.completedFuture(value);
    }

    private static List<Integer> rangeInclusive(int last) {
        List<Integer> range = new ArrayList<>();
        for (int i = 0; i <= last; i++) {
            range.add(i);
        }
        return range;
    }
}
