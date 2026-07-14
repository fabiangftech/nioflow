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
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * What a cancelled subscription does to the execution behind it — the case a
 * WebFlux app hits every time a client hangs up mid-request.
 *
 * <p>The honest summary, pinned here: the engine has no cancellation. A
 * cancelled Mono abandons the RESULT, not the work — the execution runs to its
 * end. What must hold is that it ends CLEANLY: its handlers and metrics still
 * fire and its drain slot is released, because an execution that vanished
 * without reporting would leave shutdown(grace) waiting for it forever.
 */
class ReactiveCancellationTest {

    private DefaultNioEngine engine;
    private ReactiveFlow<Integer, Integer> flow;

    @BeforeEach
    void setUp() {
        engine = new DefaultNioEngine();
        flow = Reactive.flow(DefaultNioFlow.from(Integer.class, engine));
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

        // The work is not cancelled: it runs to completion on its worker, and
        // completing an already-cancelled future is a no-op the engine survives.
        assertTrue(finished.await(2, TimeUnit.SECONDS), "the in-flight stage never finished");

        // ...and the engine still serves the next request.
        assertEquals(42, flow.just(2).handle("work", value -> value * 21).executeMono().block());
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

    @Test
    void aCancelledExecutionStillReportsItsMetrics() throws InterruptedException {
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
        // like any other, so the numbers still add up after a client hangs up —
        // a cancelled request is a COMPLETED execution, not a failed one.
        assertTrue(done.await(2, TimeUnit.SECONDS), "the cancelled execution never reported");
        assertEquals(1, reported.size());
        assertEquals("completed", reported.peek());
    }
}
