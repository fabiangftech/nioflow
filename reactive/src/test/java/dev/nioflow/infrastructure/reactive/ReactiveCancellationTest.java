package dev.nioflow.infrastructure.reactive;

import dev.nioflow.application.facade.DefaultNioEngine;
import dev.nioflow.application.facade.DefaultNioFlow;
import dev.nioflow.core.facade.NioFlowMetrics;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * What a cancelled subscription does to the execution behind it — the case a
 * WebFlux app hits every time a client hangs up mid-request.
 *
 * <p>It used to do nothing: the engine had no cancellation, and disposing the
 * Mono abandoned the RESULT while the pipeline ran on and charged the card.
 * RFC 0007 made it real, and COOPERATIVE — which is a promise with an edge, so
 * both sides of it are pinned here:
 *
 * <ul>
 *   <li>the chain stops advancing: no further stage is invoked (that is what
 *       "the card is not charged" actually means);</li>
 *   <li>an in-flight <b>async</b> call is cancelled at the source — the
 *       subscription dies, the connection is released;</li>
 *   <li>a blocking handleMono already parked on a worker is <b>not</b>
 *       interrupted. It runs to its end and its result is dropped.</li>
 * </ul>
 *
 * <p>And it still ends CLEANLY: metrics fire (as cancelled, not completed) and
 * the drain slot is released, because an execution that vanished without
 * reporting would leave shutdown(grace) waiting for it forever.
 */
class ReactiveCancellationTest {

    private DefaultNioEngine engine;
    private ReactiveFlow<Integer, Integer> flow;

    @BeforeEach
    void setUp() {
        engine = new DefaultNioEngine();
        flow = Reactive.<Integer, Integer>flow(DefaultNioFlow.from(Integer.class, engine)).allowUnbudgeted();
    }

    @AfterEach
    void tearDown() {
        engine.shutdown(Duration.ofSeconds(2));
    }

    /** A stage parked on a remote call that takes 200ms to answer. */
    private Mono<Integer> slowCall(Integer value, CountDownLatch started, CountDownLatch finished) {
        return Mono.delay(Duration.ofMillis(200))
                .doOnSubscribe(subscription -> started.countDown())
                .map(ignored -> value)
                .doOnNext(ignored -> finished.countDown());
    }

    @Test
    void cancellingTheMonoNeitherHangsNorBreaksTheEngine() throws InterruptedException {
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch finished = new CountDownLatch(1);

        Disposable subscription = flow.just(1)
                .handleMono("slow", value -> slowCall(value, started, finished))
                .executeMono()
                .subscribe();

        assertTrue(started.await(1, TimeUnit.SECONDS));
        subscription.dispose();   // the client hung up

        // Cooperative: a BLOCKING stage already on a worker is not interrupted.
        // It runs to its end and its value is dropped at the next boundary.
        assertTrue(finished.await(2, TimeUnit.SECONDS), "the in-flight stage never finished");

        // ...and the engine still serves the next request.
        assertEquals(42, flow.just(2).handle("work", value -> value * 21).executeMono().block());
    }

    /**
     * The SAME rule, in the THIRD execution driver: two consecutive
     * {@code handleMonoAsync} stages FUSE into one worker-side async run
     * ({@code AsyncRun.drive}, RFC 0013), so — exactly as with the fused BLOCKING
     * run in {@link #theStageAfterTheCancelledOneNeverRuns} — the cancellation
     * check has to live BETWEEN the stages of that run, not just in the boss loop.
     * Without it, the second stage would run on a cancelled execution. This is the
     * driver whose invariant RFC 0032 left untested; a per-reference driver
     * unification was judged the wrong fix, so the rule is pinned here instead.
     */
    @Test
    void aCancelledFusedAsyncRunStopsBeforeTheNextStage() throws InterruptedException {
        CountDownLatch firstSubscribed = new CountDownLatch(1);
        CountDownLatch secondRan = new CountDownLatch(1);

        Disposable subscription = flow.just(1)
                // First async stage: pending forever, so the fused async run parks
                // on its completion callback with the second stage still ahead.
                .handleMonoAsync("first", value -> Mono.<Integer>never()
                        .doOnSubscribe(ignored -> firstSubscribed.countDown()))
                .handleMonoAsync("second", value -> {
                    secondRan.countDown();
                    return Mono.just(value);
                })
                .executeMono()
                .subscribe();

        assertTrue(firstSubscribed.await(1, TimeUnit.SECONDS));
        subscription.dispose();   // cancel while the fused run is parked between stages

        // The first stage's callback fires (its subscription is cancelled), sees the
        // cancelled flag, and ends the run — the second stage is never invoked.
        assertFalse(secondRan.await(300, TimeUnit.MILLISECONDS),
                "a cancelled fused async run ran the next stage anyway");

        // ...and the engine still serves the next request.
        assertEquals(42, flow.just(2).handle("work", value -> value * 21).executeMono().block());
    }

    /**
     * The RFC in one test: what cancellation buys is that the NEXT stage never
     * runs. The card is not charged because charge() is never invoked.
     */
    @Test
    void theStageAfterTheCancelledOneNeverRuns() throws InterruptedException {
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch finished = new CountDownLatch(1);
        CountDownLatch charged = new CountDownLatch(1);

        Disposable subscription = flow.just(1)
                .handleMono("slow", value -> slowCall(value, started, finished))
                .handle("charge", value -> {
                    charged.countDown();
                    return value;
                })
                .executeMono()
                .subscribe();

        assertTrue(started.await(1, TimeUnit.SECONDS));
        subscription.dispose();

        // Let the abandoned worker finish: the execution is already cancelled, so
        // the run stops at charge() instead of walking into it. Note the two
        // stages FUSE — they run back-to-back on one worker with no boss
        // boundary between them — which is exactly why the cancellation check
        // also lives inside the fused run.
        assertTrue(finished.await(2, TimeUnit.SECONDS));

        assertFalse(charged.await(300, TimeUnit.MILLISECONDS),
                "a cancelled execution charged the card anyway");
    }

    /**
     * The dividend RFC 0006 paid for: an ASYNC stage holds a CompletionStage,
     * and a handle is something the engine can cancel. The Mono's subscription
     * is torn down at the source — this is the assertion the parking stage
     * could never make.
     */
    @Test
    void cancellingAnAsyncStageCancelsTheRemoteCall() throws InterruptedException {
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch cancelledAtTheSource = new CountDownLatch(1);

        Disposable subscription = flow.just(1)
                .handleMonoAsync("remote", value -> Mono.<Integer>never()
                        .doOnSubscribe(ignored -> started.countDown())
                        .doOnCancel(cancelledAtTheSource::countDown))
                .executeMono()
                .subscribe();

        assertTrue(started.await(1, TimeUnit.SECONDS));
        subscription.dispose();

        assertTrue(cancelledAtTheSource.await(2, TimeUnit.SECONDS),
                "the remote call outlived the client that asked for it");
    }

    @Test
    void aCancelledExecutionStillReleasesItsDrainSlot() throws InterruptedException {
        // The leak this guards: if a cancelled subscription's execution never
        // released its slot, shutdown(grace) would report stragglers forever and
        // a graceful drain would degrade into the full grace wait, every time.
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch finished = new CountDownLatch(1);

        Disposable subscription = flow.just(1)
                .handleMono("slow", value -> slowCall(value, started, finished))
                .executeMono()
                .subscribe();

        assertTrue(started.await(1, TimeUnit.SECONDS));
        subscription.dispose();

        int stragglers = engine.shutdown(Duration.ofSeconds(2));

        assertEquals(0, stragglers, "the cancelled execution never released its drain slot");
    }

    /**
     * The behavior change RFC 0007 ships as one: a cancelled request used to
     * report executionCompleted. It is neither a success nor a failure — a
     * dashboard that counts it as either is lying about a client who left.
     */
    @Test
    void aCancelledExecutionReportsCancelledAndNothingElse() throws InterruptedException {
        ConcurrentLinkedQueue<String> reported = new ConcurrentLinkedQueue<>();
        CountDownLatch done = new CountDownLatch(1);
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
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch finished = new CountDownLatch(1);

        Disposable subscription = flow.just(1)
                .handleMono("slow", value -> slowCall(value, started, finished))
                .executeMono()
                .subscribe();

        assertTrue(started.await(1, TimeUnit.SECONDS));
        subscription.dispose();

        // "In flight" means "not fully reported": the abandoned execution reports
        // like any other, so the numbers still add up after a client hangs up.
        assertTrue(done.await(2, TimeUnit.SECONDS), "the cancelled execution never reported");
        assertEquals(1, reported.size());
        assertEquals("cancelled", reported.peek());
    }
}
