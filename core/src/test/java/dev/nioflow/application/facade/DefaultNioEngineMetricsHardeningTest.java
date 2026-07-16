package dev.nioflow.application.facade;

import dev.nioflow.core.facade.NioFlow;
import dev.nioflow.core.facade.NioFlowMetrics;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.List;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * RFC 0023 — a throwing {@link NioFlowMetrics} sink must never break the flow.
 * Every SPI call is guarded, so a failing sink is reported through the error
 * handlers and completion proceeds: no hung future, no stuck key lane, no good
 * value misrouted into recover(). Every joined future carries an
 * {@code orTimeout} so a regression (the bug this RFC fixes) surfaces as a
 * visible test failure, not an infinite wait.
 */
class DefaultNioEngineMetricsHardeningTest {

    /** A sink that throws from whichever callback the test arms. */
    private static final class ThrowingMetrics implements NioFlowMetrics {

        static final RuntimeException BOOM = new IllegalStateException("metrics sink is down");

        volatile boolean onExecutionCompleted;
        volatile boolean onStageCompleted;
        volatile boolean onExecutionFailed;
        volatile boolean onForkCompleted;
        // When > 0, only the Nth executionCompleted call throws (1-based); the
        // rest are silent. Lets a keyed test blow up the FIRST terminal only.
        volatile int throwOnlyOnCompletion; // 0 = every call throws when armed
        private final AtomicInteger completions = new AtomicInteger();

        @Override
        public void executionCompleted(long nanos) {
            if (!onExecutionCompleted) {
                return;
            }
            int n = completions.incrementAndGet();
            if (throwOnlyOnCompletion == 0 || n == throwOnlyOnCompletion) {
                throw BOOM;
            }
        }

        @Override
        public void executionFailed(Throwable error, long nanos) {
            if (onExecutionFailed) {
                throw BOOM;
            }
        }

        @Override
        public void stageCompleted(String stage, long nanos) {
            if (onStageCompleted) {
                throw BOOM;
            }
        }

        @Override
        public void forkCompleted(String fork, long nanos) {
            if (onForkCompleted) {
                throw BOOM;
            }
        }
    }

    private static final class Errors implements Consumer<Throwable> {
        final List<Throwable> seen = new CopyOnWriteArrayList<>();

        @Override
        public void accept(Throwable error) {
            seen.add(error);
        }
    }

    @Test
    void aThrowingExecutionCompletedStillCompletesTheFutureAndReportsToOnError() {
        DefaultNioEngine engine = new DefaultNioEngine();
        ThrowingMetrics metrics = new ThrowingMetrics();
        metrics.onExecutionCompleted = true;
        engine.metrics(metrics);
        Errors errors = new Errors();

        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        flow.onError(errors);
        flow.handle("double", value -> value * 2);

        // Without the guard this join would block forever: the sink throw
        // escapes complete() before result.complete() runs.
        Integer result = flow.just(5).executeAsync()
                .orTimeout(2, TimeUnit.SECONDS).join();

        assertEquals(10, result);
        assertTrue(errors.seen.contains(ThrowingMetrics.BOOM),
                () -> "the sink throw must be reported through onError, saw: " + errors.seen);
        engine.shutdown(Duration.ofMillis(200));
    }

    @Test
    void aThrowingStageCompletedDoesNotFailOrMisrouteTheValue() {
        DefaultNioEngine engine = new DefaultNioEngine();
        ThrowingMetrics metrics = new ThrowingMetrics();
        metrics.onStageCompleted = true;
        engine.metrics(metrics);
        Errors errors = new Errors();

        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        flow.onError(errors);
        // A recover() would catch a misrouted metrics failure; assert it is
        // NEVER invoked, so the value is not corrupted into a recovered one.
        flow.handle("plus", value -> value + 1)
                .recover("guard", error -> -999);

        Integer result = flow.just(5).executeAsync()
                .orTimeout(2, TimeUnit.SECONDS).join();

        assertEquals(6, result, "the good value must flow; recover must not fire");
        assertTrue(errors.seen.contains(ThrowingMetrics.BOOM));
        engine.shutdown(Duration.ofMillis(200));
    }

    @Test
    void aThrowingExecutionFailedStillFailsTheFutureWithTheOriginalError() {
        DefaultNioEngine engine = new DefaultNioEngine();
        ThrowingMetrics metrics = new ThrowingMetrics();
        metrics.onExecutionFailed = true;
        engine.metrics(metrics);

        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        flow.handle("boom", value -> {
            throw new IllegalArgumentException("original failure");
        });

        CompletableFuture<Integer> future = flow.just(1).executeAsync().orTimeout(2, TimeUnit.SECONDS);
        CompletionException thrown = assertThrows(CompletionException.class, future::join);
        assertInstanceOf(IllegalArgumentException.class, thrown.getCause(),
                "the caller must see the ORIGINAL failure, not the sink's");
        assertEquals("original failure", thrown.getCause().getMessage());
        engine.shutdown(Duration.ofMillis(200));
    }

    @Test
    void aThrowingSinkOnAsyncStageStillAdvancesTheChain() {
        DefaultNioEngine engine = new DefaultNioEngine();
        ThrowingMetrics metrics = new ThrowingMetrics();
        metrics.onStageCompleted = true;   // settleAsync path calls stageCompleted
        engine.metrics(metrics);

        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        flow.handleAsync("triple", value -> CompletableFuture.completedFuture(value * 3))
                .handle("after", value -> value + 1);

        Integer result = flow.just(2).executeAsync()
                .orTimeout(2, TimeUnit.SECONDS).join();

        assertEquals(7, result, "the async stage must settle and the chain advance despite the sink throw");
        engine.shutdown(Duration.ofMillis(200));
    }

    @Test
    void aThrowingTerminalOnAKeyedExecutionStillReleasesTheLane() {
        DefaultNioEngine engine = new DefaultNioEngine();
        ThrowingMetrics metrics = new ThrowingMetrics();
        metrics.onExecutionCompleted = true;
        metrics.throwOnlyOnCompletion = 1;   // only the FIRST terminal blows up
        engine.metrics(metrics);

        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        flow.handle("double", value -> value * 2);

        // Same key: strict FIFO on one boss. If the first terminal's sink throw
        // skipped releaseKey(), the second future would hang forever.
        CompletableFuture<Integer> first = flow.just(1).key("k").executeAsync().orTimeout(2, TimeUnit.SECONDS);
        CompletableFuture<Integer> second = flow.just(2).key("k").executeAsync().orTimeout(2, TimeUnit.SECONDS);

        assertEquals(2, first.join());
        assertEquals(4, second.join(), "the lane must release even though the first terminal's sink threw");
        engine.shutdown(Duration.ofMillis(200));
    }

    @Test
    void aThrowingForkCompletedStillDrainsAndReportsToOnError() {
        DefaultNioEngine engine = new DefaultNioEngine();
        ThrowingMetrics metrics = new ThrowingMetrics();
        metrics.onForkCompleted = true;
        engine.metrics(metrics);
        Errors errors = new Errors();

        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        flow.onError(errors);
        flow.fork("audit", sub -> sub.handle("persist", value -> value * 2));
        flow.handle("main", value -> value + 1);

        assertEquals(2, flow.just(1).executeAsync().orTimeout(2, TimeUnit.SECONDS).join());
        // The fork is in-flight work: a clean drain proves its terminal ran to
        // completion (drain slot released) despite the sink throw.
        assertEquals(0, engine.shutdown(Duration.ofSeconds(2)), "the fork must still drain");
        assertTrue(errors.seen.contains(ThrowingMetrics.BOOM),
                () -> "the fork sink throw must reach onError, saw: " + errors.seen);
    }
}
