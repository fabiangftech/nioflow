package dev.nioflow.application.facade;

import dev.nioflow.core.model.Background;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Nothing a user callback does may break the engine: a throwing error handler
 * must not starve the other handlers, a throwing background effect must not
 * fail the flow, and an interrupted await must not swallow the interrupt.
 */
class DefaultNioEngineErrorHandlingTest extends EngineTestSupport {

    @Test
    void aThrowingErrorHandlerDoesNotStarveTheOthers() {
        List<String> seen = new CopyOnWriteArrayList<>();
        engine.addErrorHandler(error -> {
            seen.add("first");
            throw new IllegalStateException("handler is broken");
        });
        engine.addErrorHandler(error -> seen.add("second"));
        engine.append(stage("boom", value -> {
            throw new IllegalArgumentException("boom");
        }));

        engine.call(1, null).handle((value, error) -> null).join();

        assertEquals(List.of("first", "second"), seen);
    }

    @Test
    void aThrowingBackgroundEffectIsReportedButNeverFailsTheFlow() {
        AtomicReference<Throwable> reported = new AtomicReference<>();
        CountDownLatch handled = new CountDownLatch(1);
        engine.addErrorHandler(error -> {
            reported.set(error);
            handled.countDown();
        });
        engine.append(new Background("effect", value -> {
            throw new IllegalStateException("effect is broken");
        }, List.of()));
        engine.append(stage("after", value -> (Integer) value + 1));

        assertEquals(2, engine.call(1, null).join());   // the flow is untouched by the effect

        assertTrue(awaitLatch(handled), "the background failure reached the error handlers");
        assertInstanceOf(IllegalStateException.class, reported.get());
    }

    @Test
    void anInterruptedAwaitRestoresTheInterruptAndFails() throws Exception {
        assertInterruptible(() -> engine.await());
        assertInterruptible(() -> engine.await(Duration.ofSeconds(30)));
    }

    /** Blocks in await() with nothing in flight, then gets interrupted. */
    private static void assertInterruptible(Runnable await) throws InterruptedException {
        AtomicReference<Throwable> failure = new AtomicReference<>();
        AtomicReference<Boolean> interrupted = new AtomicReference<>();
        CountDownLatch waiting = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(1);

        Thread awaiter = new Thread(() -> {
            waiting.countDown();
            try {
                await.run();
            } catch (Throwable error) {
                failure.set(error);
            }
            interrupted.set(Thread.currentThread().isInterrupted());
            done.countDown();
        });
        awaiter.start();

        assertTrue(waiting.await(2, TimeUnit.SECONDS), "the awaiter started");
        Thread.sleep(50);   // let it park inside await()
        awaiter.interrupt();

        assertTrue(done.await(2, TimeUnit.SECONDS), "the interrupt unblocked await()");
        assertInstanceOf(IllegalStateException.class, failure.get());
        assertTrue(interrupted.get(), "await() re-set the interrupt flag it consumed");
    }

    private static boolean awaitLatch(CountDownLatch latch) {
        try {
            return latch.await(2, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }
}
