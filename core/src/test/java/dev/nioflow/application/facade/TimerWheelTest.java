package dev.nioflow.application.facade;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The shared hashed timer wheel behind stage timeouts. Coarse by contract:
 * an action may fire up to ~2 ticks late, never before its deadline. Tests
 * use small wheels (few buckets, 10ms tick) so multi-round deadlines are
 * exercised in milliseconds, and only assert bounds loose enough to hold
 * under machine load.
 */
class TimerWheelTest {

    @Test
    void firesAfterTheDeadlineNeverBefore() throws Exception {
        var wheel = new TimerWheel(16, 10_000_000L);
        var fired = new CountDownLatch(1);
        long start = System.nanoTime();

        wheel.schedule(50_000_000L, fired::countDown);

        assertTrue(fired.await(2, TimeUnit.SECONDS), "timeout never fired");
        long elapsedMillis = (System.nanoTime() - start) / 1_000_000;
        assertTrue(elapsedMillis >= 50, "fired " + elapsedMillis + "ms in, before its 50ms deadline");
    }

    @Test
    void cancelledTimeoutsNeverFire() throws Exception {
        var wheel = new TimerWheel(16, 10_000_000L);
        var fired = new AtomicBoolean();

        TimerWheel.Timeout timeout = wheel.schedule(60_000_000L, () -> fired.set(true));
        timeout.cancel();

        Thread.sleep(250);
        assertFalse(fired.get(), "cancelled timeout fired anyway");
    }

    @Test
    void deadlinesBeyondOneRoundSurviveTheExtraRounds() throws Exception {
        // 8 buckets x 10ms = 80ms per round; 250ms crosses three rounds.
        var wheel = new TimerWheel(8, 10_000_000L);
        var fired = new CountDownLatch(1);
        long start = System.nanoTime();

        wheel.schedule(250_000_000L, fired::countDown);

        assertTrue(fired.await(2, TimeUnit.SECONDS), "multi-round timeout never fired");
        long elapsedMillis = (System.nanoTime() - start) / 1_000_000;
        assertTrue(elapsedMillis >= 250, "fired " + elapsedMillis + "ms in, before its 250ms deadline");
    }

    @Test
    void manyTimeoutsAcrossBucketsAllFireOnce() throws Exception {
        var wheel = new TimerWheel(8, 10_000_000L);
        int count = 100;
        var fired = new CountDownLatch(count);

        for (int i = 0; i < count; i++) {
            wheel.schedule((10 + (i * 7) % 190) * 1_000_000L, fired::countDown);
        }

        assertTrue(fired.await(3, TimeUnit.SECONDS),
                "missing firings: " + fired.getCount() + " of " + count);
    }

    @Test
    void mixedCancellationsOnlySpareTheCancelled() throws Exception {
        var wheel = new TimerWheel(8, 10_000_000L);
        var fired = new CountDownLatch(50);
        var cancelledFired = new AtomicBoolean();

        for (int i = 0; i < 100; i++) {
            long delay = (20 + (i % 10) * 15) * 1_000_000L;
            if (i % 2 == 0) {
                wheel.schedule(delay, fired::countDown);
            } else {
                wheel.schedule(delay, () -> cancelledFired.set(true)).cancel();
            }
        }

        assertTrue(fired.await(3, TimeUnit.SECONDS), "live timeouts missing: " + fired.getCount());
        assertFalse(cancelledFired.get(), "a cancelled timeout fired");
    }
}
