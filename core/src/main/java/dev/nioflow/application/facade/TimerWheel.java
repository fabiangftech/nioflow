package dev.nioflow.application.facade;

import java.util.ArrayDeque;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.LockSupport;

/**
 * Hashed timer wheel shared by every engine in the JVM: schedule() and
 * cancel() are O(1) — an enqueue and a volatile flag — instead of the
 * lock-guarded priority queue CompletableFuture's orTimeout pays per call.
 *
 * Deliberately coarse: deadlines round up to the tick, and a new timeout
 * enters the wheel at the next tick's transfer, so an action fires up to
 * ~2 ticks late. A stage budget is a guard against hung calls, not a
 * precise timer. Actions must be cheap and never user code (the engine
 * only completes futures exceptionally; dependent continuations run
 * async elsewhere).
 *
 * Single-writer design: scheduling threads only touch the MPSC staging
 * queue; the one daemon worker transfers staged timeouts into buckets at
 * each tick and fires the due ones — buckets are plain, unsynchronized.
 * Cancelled timeouts are dropped at transfer or when their bucket turns
 * up; until then they linger, bounded by their own deadline.
 *
 * The worker ticks until it is interrupted (stop()); the shared wheel is
 * a daemon and simply lives as long as the JVM, but a wheel built for a
 * test or a dedicated engine can be shut down without leaking its thread.
 */
final class TimerWheel {

    static final class Timeout {

        private final Runnable action;
        private final long deadlineNanos;
        private long deadlineTick;
        private volatile boolean canceled;

        private Timeout(Runnable action, long deadlineNanos) {
            this.action = action;
            this.deadlineNanos = deadlineNanos;
        }

        void cancel() {
            canceled = true;
        }
    }

    private static final class Holder {
        // 512 buckets x 10ms tick = 5.12s per round; longer deadlines just
        // survive extra rounds (fired by deadline-tick comparison).
        private static final TimerWheel SHARED = new TimerWheel(512, 10_000_000L);
    }

    static TimerWheel shared() {
        return Holder.SHARED;
    }

    private final ArrayDeque<Timeout>[] buckets;
    private final int mask;
    private final long tickNanos;
    private final long startNanos;
    private final ConcurrentLinkedQueue<Timeout> staged = new ConcurrentLinkedQueue<>();
    private final Thread worker;

    @SuppressWarnings("unchecked")
    TimerWheel(int bucketCount, long tickNanos) {
        this.buckets = new ArrayDeque[bucketCount];
        for (int i = 0; i < bucketCount; i++) {
            buckets[i] = new ArrayDeque<>();
        }
        this.mask = bucketCount - 1;
        this.tickNanos = tickNanos;
        this.startNanos = System.nanoTime();
        this.worker = Thread.ofPlatform().name("nio-flow-timer").daemon(true).start(this::run);
    }

    Timeout schedule(long delayNanos, Runnable action) {
        Timeout timeout = new Timeout(action, System.nanoTime() + delayNanos);
        staged.add(timeout);
        return timeout;
    }

    /** Ends the ticking: pending timeouts are abandoned, never fired. */
    void stop() {
        worker.interrupt();
    }

    boolean ticking() {
        return worker.isAlive();
    }

    private void run() {
        long tick = 0;
        while (!Thread.currentThread().isInterrupted()) {
            long targetNanos = startNanos + (tick + 1) * tickNanos;
            long sleep;
            while ((sleep = targetNanos - System.nanoTime()) > 0) {
                // parkNanos returns on interrupt without clearing the flag:
                // bail out instead of spinning until the tick is due.
                LockSupport.parkNanos(sleep);
                if (Thread.currentThread().isInterrupted()) {
                    return;
                }
            }
            tick++;
            transferStaged(tick);
            fireDue(buckets[(int) (tick & mask)], tick);
        }
    }

    // Only the worker places timeouts into buckets, relative to ITS tick:
    // no scheduling thread can race a bucket being drained.
    private void transferStaged(long tick) {
        Timeout timeout;
        while ((timeout = staged.poll()) != null) {
            if (timeout.canceled) {
                continue;
            }
            long ticksLeft = Math.max(1, ceilTicks(timeout.deadlineNanos - startNanos) - tick);
            timeout.deadlineTick = tick + ticksLeft;
            buckets[(int) (timeout.deadlineTick & mask)].add(timeout);
        }
    }

    private void fireDue(ArrayDeque<Timeout> bucket, long tick) {
        // Full drain, then requeue survivors: entries whose deadline sits
        // whole rounds away come back around in bucketCount ticks.
        for (int remaining = bucket.size(); remaining > 0; remaining--) {
            Timeout timeout = bucket.poll();
            if (timeout.canceled) {
                continue;
            }
            if (timeout.deadlineTick <= tick) {
                timeout.action.run();
            } else {
                bucket.add(timeout);
            }
        }
    }

    private long ceilTicks(long nanos) {
        return (nanos + tickNanos - 1) / tickNanos;
    }
}
