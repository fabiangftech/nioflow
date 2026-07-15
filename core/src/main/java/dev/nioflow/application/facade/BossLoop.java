package dev.nioflow.application.facade;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

/**
 * A single-consumer event loop for one boss, in place of
 * {@code Executors.newSingleThreadExecutor}. The point is the wake-up: a
 * {@code ThreadPoolExecutor} over a {@code LinkedBlockingQueue} takes a lock and
 * {@code unpark}s a parked thread through the kernel on every handoff, and the
 * boss is handed to twice per request. This loop, while busy, never parks and
 * never syscalls — a handoff is an atomic swap, not a lock plus a wake.
 *
 * <p>Two mechanisms, both standard (this is Netty's {@code SingleThreadEventExecutor}
 * design, for the same reason):
 *
 * <ul>
 * <li><b>MPSC queue</b> (Vyukov): any thread enqueues with one
 *     {@code getAndSet} on the head (wait-free, a full barrier); the single
 *     consumer reads the tail with no lock and no CAS.</li>
 * <li><b>Spin-then-park</b>: after draining, the consumer spins
 *     {@link Thread#onSpinWait()} for a bounded budget before parking, and a
 *     producer only {@code unpark}s when the consumer actually parked — a
 *     Dekker handshake on {@code wake}. Under load the consumer stays awake and
 *     a handoff costs a CAS, not a syscall.</li>
 * </ul>
 *
 * <p>Extends {@link AbstractExecutorService} so it is a drop-in
 * {@code ExecutorService} for the engine, which only ever calls
 * {@code execute} plus the shutdown lifecycle: {@code submit}/{@code invokeAll}
 * come for free, implemented over {@code execute}.
 */
final class BossLoop extends AbstractExecutorService {

    // Number of onSpinWait iterations the consumer burns before parking when the
    // queue runs dry. Bounded and small on purpose: this is a one-shot burst
    // after the last task, not a continuous idle spin, but a workload that
    // trickles tasks just faster than the budget would keep a core hot. Tunable.
    private static final int SPIN_BUDGET =
            Math.max(0, Integer.getInteger("nioflow.boss.spin", 1000));

    private static final int AWAKE = 0;
    private static final int PARKED = 1;

    /** One queue node. {@code task} is null for the consumer-owned stub. */
    private static final class Node {
        Runnable task;
        Node next;

        Node(Runnable task) {
            this.task = task;
        }
    }

    private static final VarHandle HEAD_VH;
    private static final VarHandle NEXT_VH;
    private static final VarHandle WAKE_VH;

    static {
        try {
            MethodHandles.Lookup lookup = MethodHandles.lookup();
            HEAD_VH = lookup.findVarHandle(BossLoop.class, "head", Node.class);
            NEXT_VH = lookup.findVarHandle(Node.class, "next", Node.class);
            WAKE_VH = lookup.findVarHandle(BossLoop.class, "wake", int.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    // Producer end (most recent node). Accessed only through HEAD_VH, so no
    // `volatile` keyword — sidesteps S3077 and keeps the ordering explicit: the
    // swap is a full barrier, the acquire-load orders later reads after it.
    @SuppressWarnings("unused")
    private Node head;
    // Consumer end (oldest not-yet-consumed node, acts as the next stub). Touched
    // only by the consumer thread, so a plain field.
    private Node tail;
    // AWAKE/PARKED, via WAKE_VH. The producer/consumer Dekker handshake variable.
    @SuppressWarnings("unused")
    private int wake = AWAKE;

    private final Thread consumer;
    // shutdown() sets this: the loop drains what is queued, then exits.
    private volatile boolean shutdown;
    // shutdownNow() sets this: the loop exits without draining.
    private volatile boolean stopNow;

    BossLoop(ThreadFactory factory) {
        Node stub = new Node(null);
        this.head = stub;
        this.tail = stub;
        this.consumer = factory.newThread(this::runLoop);
        this.consumer.start();
    }

    // ── producer side: any thread ──

    @Override
    public void execute(Runnable task) {
        if (task == null) {
            throw new NullPointerException("task");
        }
        if (shutdown) {
            throw new RejectedExecutionException("BossLoop is shut down");
        }
        enqueue(task);
        // The swap in enqueue is a full barrier, so this read sees a PARKED
        // state written before the consumer re-checked the queue. If it parked,
        // wake it — and the compare-and-set elects exactly one producer as waker.
        if ((int) WAKE_VH.getAcquire(this) == PARKED && WAKE_VH.compareAndSet(this, PARKED, AWAKE)) {
            LockSupport.unpark(consumer);
        }
    }

    private void enqueue(Runnable task) {
        Node node = new Node(task);
        Node prev = (Node) HEAD_VH.getAndSet(this, node);
        // Publish the link with release so the consumer's acquire-load of
        // prev.next sees a fully constructed node.
        NEXT_VH.setRelease(prev, node);
    }

    // ── consumer side: the single boss thread ──

    private void runLoop() {
        int spins = SPIN_BUDGET;
        while (true) {
            Runnable task = poll();
            if (task != null) {
                run(task);
                spins = SPIN_BUDGET; // stay hot after real work
                continue;
            }
            if (stopNow || (shutdown && !hasWork())) {
                return;
            }
            if (hasWork()) {
                // A producer is mid-insert (head advanced, next not linked yet):
                // do not park, just retry — it publishes in nanoseconds.
                Thread.onSpinWait();
                continue;
            }
            if (spins > 0) {
                spins--;
                Thread.onSpinWait();
                continue;
            }
            park();
            spins = SPIN_BUDGET;
        }
    }

    private void park() {
        WAKE_VH.setVolatile(this, PARKED);
        // StoreLoad between publishing PARKED and re-reading the queue: without
        // it the empty-check could float above the PARKED store and miss a task
        // enqueued in between, losing the wake-up.
        VarHandle.fullFence();
        if (!hasWork() && !shutdown && !stopNow) {
            LockSupport.park(this);
        }
        WAKE_VH.setVolatile(this, AWAKE);
    }

    // Runs one task, keeping the loop alive whatever it throws. Engine
    // continuations catch their own failures and route them to the recovery
    // path; this is the backstop, and it follows the same rule the engine's own
    // Throwable-catch does — a dead boss thread hangs every request pinned to it
    // forever. S1181 is deliberate here, see tools/sonarlint/README.md.
    private void run(Runnable task) {
        try {
            task.run();
        } catch (Throwable failure) {
            Thread.UncaughtExceptionHandler handler = consumer.getUncaughtExceptionHandler();
            if (handler != null) {
                handler.uncaughtException(consumer, failure);
            }
        }
    }

    /**
     * Vyukov dequeue: returns the oldest task, or null when the queue is empty
     * OR a producer is mid-insert (head swapped, next not yet linked). The
     * caller tells the two apart with {@link #hasWork()}.
     */
    private Runnable poll() {
        Node t = tail;
        Node next = (Node) NEXT_VH.getAcquire(t);
        if (next == null) {
            return null;
        }
        // next becomes the new consumer-owned stub; drop t.
        tail = next;
        Runnable task = next.task;
        next.task = null; // help GC, mark consumed
        return task;
    }

    // True when at least one node has been enqueued past the consumer's stub —
    // reliable even mid-insert, unlike poll(). The park decision uses this.
    private boolean hasWork() {
        return HEAD_VH.getAcquire(this) != tail;
    }

    // ── lifecycle: only ever called on engine-owned bosses ──

    @Override
    public void shutdown() {
        shutdown = true;
        LockSupport.unpark(consumer);
    }

    @Override
    public List<Runnable> shutdownNow() {
        stopNow = true;
        shutdown = true;
        LockSupport.unpark(consumer);
        // Best effort, as the contract allows: the not-yet-run tasks are left in
        // the queue and abandoned. The engine ignores this list.
        return Collections.emptyList();
    }

    @Override
    public boolean isShutdown() {
        return shutdown;
    }

    @Override
    public boolean isTerminated() {
        return !consumer.isAlive();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        consumer.join(unit.toMillis(timeout));
        return isTerminated();
    }
}
