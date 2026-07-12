package dev.nioflow.core.model;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

/**
 * Token bucket configured as permits per period, refilled lazily from
 * timestamps — no timer thread anywhere. An idle bucket holds one full
 * period of burst (`permits` immediate tokens); once drained, acquire()
 * paces callers at the refill interval by parking the calling thread —
 * always a virtual worker in nio-flow, so parking is cheap and the boss
 * never waits. The wait is naturally visible through the stage latency
 * metric and counts against a stage timeout budget.
 *
 * One instance = one bucket: pass the same instance to several stages to
 * protect a single downstream dependency behind all of them.
 */
public final class RateLimit {

    private final long intervalNanos;
    private final long burstNanos;
    // Timestamp of the last granted slot; acquire() CAS-advances it by one
    // interval and parks until its slot is due. max(prev, now - burst)
    // caps how far the bucket can fill while idle.
    private final AtomicLong lastSlot = new AtomicLong(Long.MIN_VALUE);

    private RateLimit(int permits, Duration per) {
        if (permits < 1) {
            throw new IllegalArgumentException("permits must be >= 1");
        }
        if (per == null || per.isZero() || per.isNegative()) {
            throw new IllegalArgumentException("period must be positive");
        }
        this.intervalNanos = Math.max(1, per.toNanos() / permits);
        this.burstNanos = per.toNanos();
    }

    public static RateLimit of(int permits, Duration per) {
        return new RateLimit(permits, per);
    }

    public static RateLimit perSecond(int permits) {
        return of(permits, Duration.ofSeconds(1));
    }

    /** Parks the calling thread until a permit is due. Never throws. */
    public void acquire() {
        long now = System.nanoTime();
        long floor = now - burstNanos;
        long slot = lastSlot.updateAndGet(prev -> Math.max(prev, floor) + intervalNanos);
        // parkNanos may wake early (spurious); repark until the slot is due.
        long wait;
        while ((wait = slot - System.nanoTime()) > 0) {
            LockSupport.parkNanos(wait);
        }
    }
}
