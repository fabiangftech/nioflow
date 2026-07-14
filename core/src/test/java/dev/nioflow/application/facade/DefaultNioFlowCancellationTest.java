package dev.nioflow.application.facade;

import dev.nioflow.core.facade.Cancellable;
import dev.nioflow.core.facade.FlowResult;
import dev.nioflow.core.facade.NioFlow;
import dev.nioflow.core.facade.NioFlowMetrics;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Cancellation, in core — no Reactor anywhere. An HTTP server on virtual
 * threads whose client disconnected has exactly the same problem a disposed
 * Mono does, and gets exactly the same handle (RFC 0007).
 *
 * <p>Half of these tests assert what cancellation does NOT do. That is
 * deliberate: "cooperative" is a promise with an edge, and an edge nobody
 * asserts quietly becomes a promise the engine cannot keep.
 */
class DefaultNioFlowCancellationTest extends EngineTestSupport {

    private NioFlow<Integer, Integer> flow() {
        return DefaultNioFlow.from(Integer.class, engine);
    }

    /** A stage that blocks until released, so the execution can be cancelled mid-flight. */
    private static Integer block(Integer value, CountDownLatch entered, CountDownLatch release) {
        entered.countDown();
        try {
            release.await(2, TimeUnit.SECONDS);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
        return value;
    }

    @Test
    void cancelStopsTheChainAtTheNextLink() throws InterruptedException {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch charged = new CountDownLatch(1);

        Cancellable<Integer> handle = flow().just(1)
                .handle("slow", value -> block(value, entered, release))
                .handle("charge", value -> {
                    charged.countDown();
                    return value;
                })
                .executeCancellable();

        assertTrue(entered.await(1, TimeUnit.SECONDS));
        handle.cancel();
        release.countDown();

        // The future is cancelled — never a null, which could not be told apart
        // from a Filter cut. CompletableFuture reports a CancellationException
        // as itself, unwrapped: the handle behaves like any cancelled future.
        CompletableFuture<Integer> future = handle.future();
        assertThrows(CancellationException.class, () -> future.get(2, TimeUnit.SECONDS));
        // ...and the stage after the one in flight never ran. THIS is the feature.
        // Asserted as a latch that must NOT fall: proving a negative needs a
        // bounded wait, and this one fails loudly instead of sleeping blindly.
        assertFalse(charged.await(300, TimeUnit.MILLISECONDS),
                "a cancelled execution ran the next stage anyway");
    }

    /**
     * The limitation, asserted so it cannot silently become a promise: a
     * blocking stage already running on a worker is NOT interrupted. It runs to
     * its end (its result is simply dropped), because interrupting a virtual
     * worker in the middle of arbitrary user code is a worse bug than the one
     * cancellation fixes.
     */
    @Test
    void aBlockingStageInFlightIsNotInterrupted() throws InterruptedException {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch ranToTheEnd = new CountDownLatch(1);

        Cancellable<Integer> handle = flow().just(1)
                .handle("slow", value -> {
                    Integer result = block(value, entered, release);
                    ranToTheEnd.countDown();
                    return result;
                })
                .executeCancellable();

        assertTrue(entered.await(1, TimeUnit.SECONDS));
        handle.cancel();
        release.countDown();

        assertTrue(ranToTheEnd.await(2, TimeUnit.SECONDS),
                "the blocking stage was interrupted — cancellation is supposed to be cooperative");
    }

    /**
     * The dividend of RFC 0006: an async stage's CompletionStage is a HANDLE,
     * and cancelling the execution cancels the remote call itself. A parked
     * worker could never do this — which is why cancellation waited for it.
     */
    @Test
    void cancellingAnAsyncStageCancelsTheCallItIsWaitingOn() throws InterruptedException {
        CountDownLatch invoked = new CountDownLatch(1);
        CountDownLatch cancelledAtTheSource = new CountDownLatch(1);
        CompletableFuture<Integer> remote = new CompletableFuture<>();
        remote.whenComplete((value, error) -> {
            if (error instanceof CancellationException) {
                cancelledAtTheSource.countDown();
            }
        });

        Cancellable<Integer> handle = flow().just(1)
                .handleAsync("remote", value -> {
                    invoked.countDown();
                    return remote;   // never completes on its own
                })
                .executeCancellable();

        assertTrue(invoked.await(1, TimeUnit.SECONDS));
        handle.cancel();

        // The call the engine was waiting on is cancelled at the source: a
        // mono.toFuture() would tear down the subscription here, and
        // reactor-netty would release the connection.
        assertTrue(cancelledAtTheSource.await(2, TimeUnit.SECONDS),
                "the in-flight remote call was left running");
        assertTrue(remote.isCancelled());
        CompletableFuture<Integer> future = handle.future();
        assertThrows(CancellationException.class, () -> future.get(2, TimeUnit.SECONDS));
    }

    /**
     * The counterpart: a cancelled async call must NOT look like a stage
     * failure. Its CancellationException would otherwise walk into recover()
     * and hand a "recovered" value to a caller who has gone home.
     */
    @Test
    void cancellationDoesNotTriggerRecovery() throws InterruptedException {
        CountDownLatch invoked = new CountDownLatch(1);
        CountDownLatch recovered = new CountDownLatch(1);
        CompletableFuture<Integer> remote = new CompletableFuture<>();

        Cancellable<Integer> handle = flow().just(1)
                .handleAsync("remote", value -> {
                    invoked.countDown();
                    return remote;
                })
                .recover(error -> {
                    recovered.countDown();
                    return -1;
                })
                .executeCancellable();

        assertTrue(invoked.await(1, TimeUnit.SECONDS));
        handle.cancel();

        CompletableFuture<Integer> future = handle.future();
        assertThrows(CancellationException.class, () -> future.get(2, TimeUnit.SECONDS));
        assertFalse(recovered.await(300, TimeUnit.MILLISECONDS),
                "cancellation was recovered from as if it were a failure");
    }

    /**
     * Cancelling one member of a batch never breaks the group. The window is
     * what bounds this, deliberately: depending on the timing, the cancelled
     * execution either joined the group already (the bulk runs with its element
     * and its result is dropped — arity is what the bulk was promised, so it is
     * NOT pulled out mid-window) or never got there. Either way the flush
     * happens and everyone else is served, which is the property a caller
     * depends on.
     */
    @Test
    void cancellingAMemberOfABatchDoesNotBreakTheGroup() throws Exception {
        NioFlow<Integer, Integer> flow = flow();
        // Window-driven flush (the size trigger is never reached), so the group
        // cannot stall on a member that went away.
        flow.batch("bulk", 5, Duration.ofMillis(300),
                values -> values.stream().map(value -> value * 10).toList());

        Cancellable<Integer> cancelled = flow.just(1).executeCancellable();
        CompletableFuture<Integer> survivor = flow.just(2).executeAsync();

        cancelled.cancel();

        assertEquals(20, survivor.get(2, TimeUnit.SECONDS), "a cancelled member stalled the batch");
        CompletableFuture<Integer> future = cancelled.future();
        assertThrows(CancellationException.class, () -> future.get(2, TimeUnit.SECONDS));
    }

    /** A fork is detached BY DEFINITION (RFC 0001): cancelling the parent leaves it alone. */
    @Test
    void aForkAlreadySpawnedIsUnaffected() throws InterruptedException {
        CountDownLatch forkRan = new CountDownLatch(1);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);

        Cancellable<Integer> handle = flow().just(1)
                .fork("audit", lane -> lane.handle("write", value -> {
                    forkRan.countDown();
                    return value;
                }))
                .handle("slow", value -> block(value, entered, release))
                .executeCancellable();

        assertTrue(entered.await(1, TimeUnit.SECONDS));
        handle.cancel();
        release.countDown();

        assertTrue(forkRan.await(2, TimeUnit.SECONDS), "cancelling the parent killed its detached fork");
    }

    @Test
    void cancelIsIdempotentAndAFinishedExecutionIgnoresIt() {
        Cancellable<Integer> handle = flow().just(1)
                .handle("double", value -> value * 2)
                .executeCancellable();

        assertEquals(2, handle.future().join());

        // Cancelling a finished execution is a no-op, and so is cancelling twice:
        // the result stands.
        handle.cancel();
        handle.cancel();
        assertEquals(2, handle.future().join());
    }

    @Test
    void aCancelledExecutionReportsCancelledReleasesItsSlotAndNotifiesNobody() throws InterruptedException {
        ConcurrentLinkedQueue<String> reported = new ConcurrentLinkedQueue<>();
        CountDownLatch done = new CountDownLatch(1);
        AtomicInteger completeHandlerCalls = new AtomicInteger();
        engine.metrics(new NioFlowMetrics() {
            @Override
            public void executionCompleted(long nanos) {
                reported.add("completed");
                done.countDown();
            }

            @Override
            public void executionFailed(Throwable error, long nanos) {
                reported.add("failed");
                done.countDown();
            }

            @Override
            public void executionCancelled(long nanos) {
                reported.add("cancelled");
                done.countDown();
            }
        });
        NioFlow<Integer, Integer> flow = flow();
        flow.onComplete(value -> completeHandlerCalls.incrementAndGet());

        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        Cancellable<Integer> handle = flow.just(1)
                .handle("slow", value -> block(value, entered, release))
                .executeCancellable();

        assertTrue(entered.await(1, TimeUnit.SECONDS));
        handle.cancel();
        release.countDown();

        assertTrue(done.await(2, TimeUnit.SECONDS), "the cancelled execution never reported");
        assertEquals(1, reported.size());
        assertEquals("cancelled", reported.peek());
        // It reached neither side: it produced no output for the complete
        // handlers, and it is not a failure for the error handlers.
        assertEquals(0, completeHandlerCalls.get(), "a cancelled execution notified the complete handlers");
        // And it released its drain slot: a clean drain means everything reported.
        assertEquals(0, engine.shutdown(Duration.ofSeconds(2)), "the cancelled execution held its drain slot");
    }

    @Test
    void executeResultTellsCancelledApartFromFilteredAndCompleted() throws InterruptedException {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);

        Cancellable<Integer> handle = flow().just(1)
                .handle("slow", value -> block(value, entered, release))
                .executeCancellable();

        assertTrue(entered.await(1, TimeUnit.SECONDS));
        handle.cancel();
        release.countDown();

        CompletableFuture<Integer> future = handle.future();
        assertThrows(CancellationException.class, () -> future.get(2, TimeUnit.SECONDS));

        // The sealed outcome grew a third case, and it is a third kind of
        // nothing: not a value, not a cut — a request nobody is waiting for.
        FlowResult<Integer> completed = flow().just(2).handle("double", v -> v * 2).executeResult();
        assertInstanceOf(FlowResult.Completed.class, completed);
        assertFalse(completed.cancelled());
        assertNotNull(new FlowResult.Cancelled<Integer>());
        assertTrue(new FlowResult.Cancelled<Integer>().cancelled());
    }

    /** A key lane must be handed to the next execution, or same-key traffic stalls forever. */
    @Test
    void aCancelledKeyedExecutionReleasesItsLane() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        NioFlow<Integer, Integer> flow = flow();

        Cancellable<Integer> first = flow.just(1)
                .key("account-7")
                .handle("slow", value -> block(value, entered, release))
                .executeCancellable();

        assertTrue(entered.await(1, TimeUnit.SECONDS));
        first.cancel();
        release.countDown();

        // The next execution on the SAME key must run: if the cancelled one kept
        // its lane, this would hang until the test's timeout.
        CompletableFuture<Integer> second = flow.just(2)
                .key("account-7")
                .handle("double", value -> value * 2)
                .executeAsync();

        assertEquals(4, second.get(2, TimeUnit.SECONDS));
    }
}
