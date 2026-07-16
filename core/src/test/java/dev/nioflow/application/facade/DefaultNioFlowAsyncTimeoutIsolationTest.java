package dev.nioflow.application.facade;

import dev.nioflow.core.facade.Cancellable;
import dev.nioflow.core.facade.NioFlow;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * RFC 0025 — the subscription-cancel of an async stage must not run on the
 * shared {@link TimerWheel} thread (nor on an outside caller's thread).
 * Cancelling a {@code mono.toFuture()} can run reactor-netty's connection
 * teardown, which is neither cheap nor non-blocking; the one timer thread ticks
 * every timeout and batch window in the JVM, so a teardown on it stalls all of
 * them. The cancel is handed to a worker instead.
 *
 * <p>The observing future records the thread its {@code cancel} runs on (and can
 * block there to simulate a slow teardown), which turns "off the timer thread"
 * into a deterministic assertion.
 */
class DefaultNioFlowAsyncTimeoutIsolationTest {

    private static final String TIMER_THREAD = "nio-flow-timer";

    /** A never-completing CompletionStage that records — and optionally blocks — where it is cancelled. */
    private static final class ObservingFuture<T> extends CompletableFuture<T> {

        final CountDownLatch cancelled = new CountDownLatch(1);
        final AtomicReference<String> cancelThread = new AtomicReference<>();
        private final Runnable teardown;

        ObservingFuture(Runnable teardown) {
            this.teardown = teardown;
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            cancelThread.set(Thread.currentThread().getName());
            cancelled.countDown();
            if (teardown != null) {
                teardown.run();   // simulate connection teardown — may block
            }
            return super.cancel(mayInterruptIfRunning);
        }
    }

    @Test
    void theAsyncTimeoutRunsTheSubscriptionCancelOffTheTimerThread() {
        DefaultNioEngine engine = new DefaultNioEngine();
        ObservingFuture<Integer> hung = new ObservingFuture<>(null);
        AtomicReference<Throwable> recovered = new AtomicReference<>();

        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        flow.handleAsync("hung-remote", value -> hung, Duration.ofMillis(50))
                .recover(error -> {
                    recovered.set(error);
                    return -1;
                });

        assertEquals(-1, flow.just(1).execute());
        assertTrue(awaitQuietly(hung.cancelled), "the timed-out call was never cancelled");
        assertInstanceOf(TimeoutException.class, recovered.get(),
                "recover() must still see a TimeoutException, not a wrapper or a CancellationException");
        assertFalse(hung.cancelThread.get().contains(TIMER_THREAD),
                () -> "the subscription cancel ran on the shared timer thread: " + hung.cancelThread.get());

        engine.shutdown(Duration.ofMillis(200));
    }

    @Test
    void theOutsideCancelRunsTheSubscriptionCancelOffTheCallerThread() throws InterruptedException {
        DefaultNioEngine engine = new DefaultNioEngine();
        CountDownLatch inFlight = new CountDownLatch(1);
        ObservingFuture<Integer> hung = new ObservingFuture<>(null);

        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        Cancellable<Integer> handle = flow.just(1)
                .handleAsync("remote", value -> {
                    inFlight.countDown();
                    return hung;
                })
                .executeCancellable();

        assertTrue(inFlight.await(2, TimeUnit.SECONDS), "the async call never started");
        String callerThread = Thread.currentThread().getName();

        handle.cancel();

        assertTrue(awaitQuietly(hung.cancelled), "the in-flight call was never cancelled");
        assertNotEquals(callerThread, hung.cancelThread.get(),
                "the subscription cancel ran on the caller's thread, not a worker");

        engine.shutdown(Duration.ofMillis(200));
    }

    @Test
    void aSlowSubscriptionTeardownDoesNotStallOtherTimeoutsOnTheSharedWheel() {
        DefaultNioEngine engine = new DefaultNioEngine();
        CountDownLatch releaseTeardown = new CountDownLatch(1);
        // A's cancel blocks (a slow teardown); B is normal. Both time out on the
        // same shared wheel. If A's teardown ran on the timer thread it would
        // stall the wheel and B's timeout would never fire — B would hang here.
        ObservingFuture<Integer> slow = new ObservingFuture<>(() -> awaitQuietly(releaseTeardown));
        ObservingFuture<Integer> fast = new ObservingFuture<>(null);

        NioFlow<Integer, Integer> flow = DefaultNioFlow.from(Integer.class, engine);
        flow.handleAsync("hung", value -> value == 1 ? slow : fast, Duration.ofMillis(40))
                .recover(error -> -1);

        CompletableFuture<Integer> a = flow.just(1).executeAsync().orTimeout(5, TimeUnit.SECONDS);
        CompletableFuture<Integer> b = flow.just(2).executeAsync().orTimeout(5, TimeUnit.SECONDS);

        // B must recover on time even while A's teardown is still blocked: the
        // wheel kept ticking because A's cancel went to a worker, not the wheel.
        assertEquals(-1, b.join(), "B timed out but never recovered — the wheel was stalled by A's teardown");

        releaseTeardown.countDown();
        assertEquals(-1, a.join());

        engine.shutdown(Duration.ofSeconds(2));
    }

    private static boolean awaitQuietly(CountDownLatch latch) {
        try {
            return latch.await(3, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }
}
