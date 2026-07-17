package dev.nioflow.application.facade;

import dev.nioflow.core.facade.Cancellable;
import dev.nioflow.core.facade.ChainValidationException;
import dev.nioflow.core.facade.NioEngine;
import dev.nioflow.core.facade.NioFlowMetrics;
import dev.nioflow.core.facade.PreparedChain;
import dev.nioflow.core.model.AsyncStage;
import dev.nioflow.core.model.Background;
import dev.nioflow.core.model.Batch;
import dev.nioflow.core.model.FanOut;
import dev.nioflow.core.model.FlowSignal;
import dev.nioflow.core.model.Fork;
import dev.nioflow.core.model.Link;
import dev.nioflow.core.model.OverflowPolicy;
import dev.nioflow.core.model.Recovery;
import dev.nioflow.core.model.Splice;
import dev.nioflow.core.model.Stage;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public class DefaultNioEngine implements NioEngine {

    static final String NIO_FLOW_BOSS = "nio-flow-boss-";
    static final String ASYNC_STAGE = "Async stage '";
    private static final String IN_FLIGHT_CAPACITY = "In-flight capacity ";

    // Bitset limit: decision ids 0..511 fit in 16 longs (128 bytes) per execution.
    // Beyond that the bitset would outgrow a small map, so decisions overflow into
    // one. Package-visible so a per-request plan can decide whether its decisions
    // need compacting to fit (RFC 0038), the same limit the Execution bitset uses.
    static final int MAX_BITSET_DECISION_ID = 511;

    // Public sentinel carried by raw call() futures when a Filter cut the
    // execution. Engine exits (await, complete handlers) and the flow-level
    // execute()/executeAsync() map it to null; executeResult() observes it.
    static final Object FILTERED = FlowSignal.FILTERED;

    // Public sentinel carried by raw call() futures when the execution was
    // cancelled from the outside. It ends the execution through the very same
    // exactly-once door FILTERED does — the flow facade maps it to a
    // CancellationException, and executeResult() reports it as Cancelled.
    static final Object CANCELLED = FlowSignal.CANCELLED;

    // Internal to advance(): the link took the execution over (dispatched to a
    // worker, forked, batched) or ended it, so the boss must stop walking.
    static final Object HANDED_OFF = new Object();

    static ExecutorService[] createBossPool(int count, String namePrefix) {
        ThreadFactory factory = Thread.ofPlatform().name(namePrefix, 0).daemon(true).factory();
        ExecutorService[] bosses = new ExecutorService[count];
        for (int i = 0; i < bosses.length; i++) {
            // A purpose-built single-consumer event loop, not a ThreadPoolExecutor:
            // handoffs to a busy boss cost an atomic swap, not a lock plus an
            // unpark syscall. See BossLoop.
            bosses[i] = new BossLoop(factory);
        }
        return bosses;
    }

    private final ExecutorService[] bossExecutorServices;
    final ExecutorService workersExecutorService;
    private final boolean ownsExecutors;

    // Backpressure for BOTH admission paths (RFC 0031): a permit is acquired
    // BEFORE the execution starts and released on a path-specific seam.
    //   - inject/await: the bounded resource is the un-collected results queue,
    //     so the permit is released when await() collects the result.
    //   - call/callCancellable: the bounded resource is in-flight calls (there
    //     is no results queue), so the permit is released in finishBookkeeping
    //     when the execution reaches any terminal.
    // Same capacity + OverflowPolicy govern both. null = unbounded (default).
    private final Semaphore inFlightPermits;
    private final OverflowPolicy overflowPolicy;
    private final int inFlightCapacity;

    // Optional per-key backlog bound (RFC 0039): caps how many executions may
    // queue behind ONE key's running head. 0 = unbounded (default), so keyed
    // execution is unchanged unless keyLaneCapacity(...) is called. Checked at
    // admission (the caller's thread), because enrollment happens on the boss,
    // which must never park: FAIL throws, DROP fails the future, BLOCK parks the
    // caller on keyLaneVacancy until the hot key drains. Volatile: set once by
    // keyLaneCapacity() before use, then only read.
    private volatile int keyLaneCapacity;
    private volatile OverflowPolicy keyLaneOverflowPolicy = OverflowPolicy.FAIL;
    // BLOCK producers wait here; the key's boss notifies as it releases a lane.
    private final Object keyLaneVacancy = new Object();

    // Metrics SPI: null (default) means zero instrumentation on the hot path.
    // AtomicReference for the publication semantics (a plain volatile field of a
    // non-primitive type is what SonarQube's S3077 flags); readers snapshot it
    // once with get() and use that reference for the whole execution.
    final AtomicReference<NioFlowMetrics> metrics = new AtomicReference<>();

    // Graceful drain: closed rejects new work; activeExecutions tracks
    // in-flight ones so shutdown() can wait for them to finish. A LongAdder,
    // not an AtomicInteger: it is bumped for every execution and every fork on
    // one JVM-wide cacheline, and striping it removes that contention from the
    // hot path. The cost is that the zero transition is no longer atomically
    // observable, so awaitDrain polls sum() (a cold path — shutdown only).
    private volatile boolean closed;
    final LongAdder activeExecutions = new LongAdder();
    // Detached sub-flows currently running. They are counted in
    // activeExecutions too (the drain must wait for them); this one only feeds
    // the forksInFlight gauge.
    final AtomicInteger activeForks = new AtomicInteger();

    // The chain and its compiled plan as ONE atomic value (see ChainVersion).
    private final AtomicReference<ChainVersion> version =
            new AtomicReference<>(new ChainVersion(List.of(), null));
    private volatile boolean sealed;
    private final AtomicInteger decisionIds = new AtomicInteger();
    private final List<Consumer<Throwable>> errorHandlers = new CopyOnWriteArrayList<>();
    final List<Consumer<Object>> completeHandlers = new CopyOnWriteArrayList<>();
    private final BlockingQueue<CompletableFuture<Object>> inFlight = new LinkedBlockingQueue<>();
    // One in-flight group per batch POINT, keyed by Batch#groupKey: executions
    // from ANY boss park here until size or window. Keyed by the group key and
    // not the link instance because a Batch is rebuilt whenever the chain around
    // it is rebuilt with different guards (a lane, or RFC 0038's per-request
    // decision compaction) — keying by instance gave every rebuilt copy its own
    // group, which stopped coalescing and leaked a group per request.
    private final ConcurrentHashMap<Object, BatchGroup> batchGroups = new ConcurrentHashMap<>();
    // FIFO lane per active business key. The map is concurrent because
    // different keys live on different bosses; each lane's INTERNALS are
    // only touched by its key's boss (deterministic affinity), and the
    // entry is removed as soon as the lane drains — no key leak.
    final ConcurrentHashMap<Object, KeyLane> keyLanes = new ConcurrentHashMap<>();

    static final class KeyLane {
        // A ConcurrentLinkedQueue, not an ArrayDeque (RFC 0026): in steady state
        // only the key's boss touches it, but during dedicated-engine shutdown a
        // rejected resumeOnBoss can reach it from a worker while the boss is still
        // draining queued run() tasks that add to it. FIFO is unchanged (add to
        // tail, poll from head), and it stays lock-free — the affinity that keeps
        // it single-writer in steady state is preserved; this only makes the
        // shutdown-window access safe.
        final ConcurrentLinkedQueue<Execution> waiting = new ConcurrentLinkedQueue<>();
        boolean active;
    }

    // Named regions for atomic multi-link swaps: boundaries are remembered
    // by LINK IDENTITY, not by index, so edits elsewhere in the chain never
    // stale them. Guarded by the engine's synchronized edit methods.
    private final Map<String, Region> regions = new HashMap<>();

    public DefaultNioEngine() {
        this(SharedExecutors.BOSSES, SharedExecutors.WORKERS, false, 0, OverflowPolicy.BLOCK);
    }

    /**
     * A capacity-bounded engine. The bound applies to BOTH admission paths
     * (RFC 0031): {@code inject}/{@code await} (bounding the un-collected results
     * queue) and {@code call}/{@code callCancellable} (bounding in-flight calls —
     * the path the reactive facade runs on). FAIL rejects the excess call with a
     * {@link RejectedExecutionException} (thrown to a producer, a failed future to
     * a caller); DROP discards it (reported to the error handlers and the metrics
     * sink); BLOCK parks the CALLING thread until a slot frees — so do NOT use
     * BLOCK when calls originate on an event loop (a Netty/boss thread), where
     * parking is the hazard the engine otherwise forbids; prefer FAIL or DROP and
     * bound admission upstream (WebFlux concurrency, a {@code RateLimit} stage).
     */
    public DefaultNioEngine(int inFlightCapacity, OverflowPolicy overflowPolicy) {
        this(SharedExecutors.BOSSES, SharedExecutors.WORKERS, false, inFlightCapacity, overflowPolicy);
    }

    public DefaultNioEngine(ExecutorService bossExecutorService,
                            ExecutorService workersExecutorService) {
        this(new ExecutorService[]{bossExecutorService}, workersExecutorService, true, 0, OverflowPolicy.BLOCK);
    }

    /**
     * Dedicated event loop for latency-critical flows: this engine gets its
     * own boss pool (bossCount single-threaded bosses) and its own virtual
     * worker pool instead of the JVM-shared ones — no other engine can queue
     * orchestration behind its bosses, and shutdown() terminates them.
     * Executions still pin to one boss each, so bossCount bounds how many
     * executions this engine orchestrates in parallel (user code runs on
     * virtual workers either way).
     */
    public static DefaultNioEngine dedicated(int bossCount) {
        return dedicated(bossCount, 0, OverflowPolicy.BLOCK);
    }

    public static DefaultNioEngine dedicated(int bossCount, int inFlightCapacity, OverflowPolicy overflowPolicy) {
        if (bossCount < 1) {
            throw new IllegalArgumentException("bossCount must be >= 1");
        }
        return new DefaultNioEngine(createBossPool(bossCount, NIO_FLOW_BOSS + "dedicated-"),
                Executors.newVirtualThreadPerTaskExecutor(), true, inFlightCapacity, overflowPolicy);
    }

    private DefaultNioEngine(ExecutorService[] bossExecutorServices, ExecutorService workersExecutorService,
                             boolean ownsExecutors, int inFlightCapacity, OverflowPolicy overflowPolicy) {
        this.bossExecutorServices = bossExecutorServices;
        this.workersExecutorService = workersExecutorService;
        this.ownsExecutors = ownsExecutors;
        this.inFlightCapacity = inFlightCapacity;
        this.inFlightPermits = inFlightCapacity > 0 ? new Semaphore(inFlightCapacity) : null;
        this.overflowPolicy = overflowPolicy;
    }

    @Override
    public void inject(Object input) {
        // No context: nothing on the hot path reads it, so nothing is allocated
        // for it (the future Context API will lazy-init on first access).
        inject(input, null);
    }

    @Override
    public void inject(Object input, Map<String, Object> context) {
        NioFlowMetrics sink = metrics.get();
        if (closed) {
            notifyError(new RejectedExecutionException("Engine is shut down; value rejected"));
            return;
        }
        if (!admit()) {
            // DROP: the value never runs; observable through the error handlers.
            RejectedExecutionException rejection = new RejectedExecutionException(
                    IN_FLIGHT_CAPACITY + inFlightCapacity + " reached; value dropped");
            if (sink != null) {
                sink.valueDropped();
            }
            notifyError(rejection);
            return;
        }
        // Submit directly, NOT through the public call(): call() now admits on
        // its own (RFC 0031), and routing inject through it would take a second
        // permit. inject's permit is released by await() (it bounds the
        // un-collected results queue), so the execution does not release it.
        inFlight.add(submit(input, context, version.get().links(), null, false).result);
        if (sink != null) {
            sink.queueDepth(inFlight.size());
        }
    }

    private boolean admit() {
        if (inFlightPermits == null) {
            return true;
        }
        return switch (overflowPolicy) {
            case BLOCK -> {
                try {
                    inFlightPermits.acquire();
                    yield true;
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("Interrupted while waiting for in-flight capacity", e);
                }
            }
            case DROP -> inFlightPermits.tryAcquire();
            case FAIL -> {
                if (!inFlightPermits.tryAcquire()) {
                    throw new RejectedExecutionException(IN_FLIGHT_CAPACITY + inFlightCapacity + " reached");
                }
                yield true;
            }
        };
    }

    void releasePermit() {
        if (inFlightPermits != null) {
            inFlightPermits.release();
        }
    }

    /**
     * Bounds each per-key FIFO lane's backlog (RFC 0039): at most {@code maxDepth}
     * executions may queue behind one key's running head. Off by default
     * (unbounded), so keyed execution is unchanged unless this is called — like
     * {@link #metrics}, set it before the engine takes traffic.
     *
     * <p>The bound is a per-KEY backpressure, distinct from the in-flight capacity
     * (which bounds total admission): a hot key whose head stalls can otherwise
     * grow its backlog without limit, each queued execution holding a drain slot
     * so {@code shutdown(grace)} never completes. FAIL rejects the excess call
     * with a {@link RejectedExecutionException}, DROP fails its future (reported to
     * the error handlers and the metrics sink), BLOCK parks the CALLING thread
     * until the key drains — so, exactly as for the in-flight BLOCK, do NOT use
     * BLOCK when keyed calls originate on an event loop; prefer FAIL or DROP there.
     */
    public DefaultNioEngine keyLaneCapacity(int maxDepth, OverflowPolicy policy) {
        if (maxDepth < 1) {
            throw new IllegalArgumentException("keyLaneCapacity maxDepth must be at least 1, was " + maxDepth);
        }
        this.keyLaneCapacity = maxDepth;
        this.keyLaneOverflowPolicy = policy;
        return this;
    }

    /**
     * Admission for a keyed call (RFC 0039), on the caller's thread. Returns true
     * to proceed; false is DROP (the caller fails the future). FAIL throws; BLOCK
     * parks here until the key's backlog has room. Unbounded (the default) is one
     * volatile read. The depth counted is the BACKLOG — executions already queued
     * behind the key's running head — so the running head itself is never blocked.
     */
    private boolean admitKeyLane(Object key) {
        int capacity = keyLaneCapacity;
        if (capacity == 0 || key == null) {
            return true;
        }
        return switch (keyLaneOverflowPolicy) {
            case DROP -> keyLaneDepth(key) < capacity;
            case FAIL -> {
                if (keyLaneDepth(key) >= capacity) {
                    throw new RejectedExecutionException(
                            "Key-lane capacity " + capacity + " reached for key " + key);
                }
                yield true;
            }
            case BLOCK -> {
                awaitKeyLaneVacancy(key, capacity);
                yield true;
            }
        };
    }

    private void awaitKeyLaneVacancy(Object key, int capacity) {
        synchronized (keyLaneVacancy) {
            while (keyLaneDepth(key) >= capacity && !closed) {
                try {
                    keyLaneVacancy.wait();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("Interrupted while waiting for key-lane capacity", e);
                }
            }
        }
    }

    // Wakes BLOCK producers after the key's boss released a lane slot (or at
    // shutdown, so nobody hangs). Guarded so an engine that never set a BLOCK
    // bound touches neither the monitor nor the boss with a lock.
    void signalKeyLaneVacancy() {
        if (keyLaneCapacity > 0 && keyLaneOverflowPolicy == OverflowPolicy.BLOCK) {
            synchronized (keyLaneVacancy) {
                keyLaneVacancy.notifyAll();
            }
        }
    }

    private RejectedExecutionException keyLaneRejection(Object key) {
        RejectedExecutionException rejection = new RejectedExecutionException(
                "Key-lane capacity " + keyLaneCapacity + " reached for key " + key + "; call dropped");
        NioFlowMetrics sink = metrics.get();
        if (sink != null) {
            sink.valueDropped();
        }
        notifyError(rejection);
        return rejection;
    }

    @Override
    public CompletableFuture<Object> call(Object input, Map<String, Object> context) {
        return call(input, context, version.get().links());
    }

    @Override
    public CompletableFuture<Object> call(Object input, Map<String, Object> context, List<Link> chain) {
        return call(input, context, chain, null);
    }

    @Override
    public CompletableFuture<Object> call(Object input, Map<String, Object> context, List<Link> chain, Object key) {
        if (closed) {
            return CompletableFuture.failedFuture(rejection());
        }
        // Per-key backlog bound first (RFC 0039), then the in-flight bound (RFC
        // 0031): a keyed call that its hot key cannot admit never takes an
        // in-flight permit. Both no-op unless configured.
        if (!admitKeyLane(key)) {
            return CompletableFuture.failedFuture(keyLaneRejection(key));
        }
        // Admission on the request/response path too (RFC 0031): capacity bounds
        // in-flight calls, not just inject's results queue. FAIL throws here; DROP
        // fails the future; BLOCK parks the caller (never use BLOCK when calls
        // originate on an event loop). Unbounded (the default) is one null check.
        if (!admit()) {
            return CompletableFuture.failedFuture(capacityRejection());
        }
        // The raw result future goes straight to the caller: bookkeeping
        // (drain slot, metrics, handlers) runs inside Execution BEFORE the
        // future completes, so no dependent whenComplete future is allocated.
        return submit(input, context, chain, key, true).result;
    }

    /**
     * The same submission, with the handle on top. A caller who can cancel pays
     * one small record for it; call() above still allocates nothing extra, which
     * is why these are two methods and not one.
     */
    @Override
    public Cancellable<Object> callCancellable(Object input, Map<String, Object> context,
                                               List<Link> chain, Object key) {
        if (closed) {
            return new RejectedCall(CompletableFuture.failedFuture(rejection()));
        }
        if (!admitKeyLane(key)) {
            return new RejectedCall(CompletableFuture.failedFuture(keyLaneRejection(key)));
        }
        if (!admit()) {
            return new RejectedCall(CompletableFuture.failedFuture(capacityRejection()));
        }
        return new ExecutionHandle(submit(input, context, chain, key, true));
    }

    @Override
    public CompletableFuture<Object> call(Object input, Map<String, Object> context,
                                          PreparedChain prepared, Object key) {
        if (closed) {
            return CompletableFuture.failedFuture(rejection());
        }
        if (!admitKeyLane(key)) {
            return CompletableFuture.failedFuture(keyLaneRejection(key));
        }
        if (!admit()) {
            return CompletableFuture.failedFuture(capacityRejection());
        }
        return submit(input, context, ((Prepared) prepared).plan(), key, true).result;
    }

    @Override
    public Cancellable<Object> callCancellable(Object input, Map<String, Object> context,
                                               PreparedChain prepared, Object key) {
        if (closed) {
            return new RejectedCall(CompletableFuture.failedFuture(rejection()));
        }
        if (!admitKeyLane(key)) {
            return new RejectedCall(CompletableFuture.failedFuture(keyLaneRejection(key)));
        }
        if (!admit()) {
            return new RejectedCall(CompletableFuture.failedFuture(capacityRejection()));
        }
        return new ExecutionHandle(submit(input, context, ((Prepared) prepared).plan(), key, true));
    }

    @Override
    public PreparedChain prepare(List<Link> chain) {
        List<Link> links = List.copyOf(chain);
        List<String> problems = ChainValidator.validate(links);
        if (!problems.isEmpty()) {
            throw new ChainValidationException(problems);
        }
        return new Prepared(CompiledChain.compile(links));
    }

    @Override
    public PreparedChain planFor(List<Link> chain) {
        // No validation on purpose: a per-request chain's anonymous names may
        // duplicate the shared chain's (both counters start at 0), which is
        // fine to run but the validator would reject. Same behaviour the
        // interpreted per-request path always had, now with a plan.
        return new Prepared(CompiledChain.compile(List.copyOf(chain)));
    }

    /**
     * Test seam (RFC 0038): the compiled plan's highest decision id. Lets a test
     * assert a per-request pipeline compacts its decisions to fit the bitset
     * instead of dragging the engine-wide counter into it — a difference the
     * public API hides, because the bitset and the overflow map route identically.
     */
    static int planMaxDecisionId(PreparedChain prepared) {
        return ((Prepared) prepared).plan().maxDecisionId();
    }

    private RejectedExecutionException rejection() {
        RejectedExecutionException rejection =
                new RejectedExecutionException("Engine is shut down; call rejected");
        notifyError(rejection);
        return rejection;
    }

    /**
     * The DROP outcome on the call path (RFC 0031): the value never runs, its
     * future completes exceptionally, and the drop is observable exactly as
     * inject's is — valueDropped() to the metrics sink, the exception to the
     * error handlers. FAIL throws from admit() before reaching here; BLOCK parks.
     */
    private RejectedExecutionException capacityRejection() {
        RejectedExecutionException rejection = new RejectedExecutionException(
                IN_FLIGHT_CAPACITY + inFlightCapacity + " reached; call rejected");
        NioFlowMetrics sink = metrics.get();
        if (sink != null) {
            sink.valueDropped();
        }
        notifyError(rejection);
        return rejection;
    }

    private Execution submit(Object input, Map<String, Object> context, List<Link> chain, Object key,
                             boolean releasesPermit) {
        // The plan only applies to the exact chain version it was built for;
        // execution-local chains, and a chain snapshot taken before an edit
        // that has since landed (identity mismatch), fall back to interpreting.
        ChainVersion current = version.get();
        CompiledChain plan = current.links() == chain ? current.plan() : null;
        Execution execution = new Execution(this, key == null ? nextBoss() : bossFor(key),
                plan != null ? chain : List.copyOf(chain), plan, input, key, null);
        execution.context = context;
        execution.releasesPermit = releasesPermit;
        activeExecutions.increment();
        try {
            execution.boss.execute(execution);
        } catch (RejectedExecutionException rejected) {
            execution.fail(rejected);
        }
        return execution;
    }

    /**
     * The same submission off a prebuilt plan: no defensive copy and no
     * decision rescan (the plan carries both the links it was compiled for and
     * their highest decision id). The chain the Execution walks IS the plan's
     * own list, so positions line up.
     */
    private Execution submit(Object input, Map<String, Object> context, CompiledChain plan, Object key,
                             boolean releasesPermit) {
        Execution execution = new Execution(this, key == null ? nextBoss() : bossFor(key),
                plan.links(), plan, input, key, null);
        execution.context = context;
        execution.releasesPermit = releasesPermit;
        activeExecutions.increment();
        try {
            execution.boss.execute(execution);
        } catch (RejectedExecutionException rejected) {
            execution.fail(rejected);
        }
        return execution;
    }

    /** The handle callCancellable() hands out: the execution's own future, and its cancel. */
    private record ExecutionHandle(Execution execution) implements Cancellable<Object> {

        @Override
        public CompletableFuture<Object> future() {
            return execution.result;
        }

        @Override
        public void cancel() {
            execution.cancel();
        }
    }

    @Override
    public void metrics(NioFlowMetrics metrics) {
        this.metrics.set(metrics);
    }

    @Override
    public List<Link> chain() {
        return version.get().links();
    }

    @Override
    public synchronized void append(Link link) {
        if (sealed) {
            throw new IllegalStateException("Chain is sealed; call release() before appending");
        }
        List<Link> next = new ArrayList<>(chain());
        next.add(link);
        // Appending invalidates the plan: the next call interprets until seal().
        version.set(new ChainVersion(List.copyOf(next), null));
    }

    @Override
    public synchronized void seal() {
        // Fail fast at seal time: a broken definition stops the deploy instead
        // of producing runtime surprises.
        List<Link> links = chain();
        List<String> problems = ChainValidator.validate(links);
        if (!problems.isEmpty()) {
            throw new ChainValidationException(problems);
        }
        sealed = true;
        version.set(new ChainVersion(links, CompiledChain.compile(links)));
    }

    @Override
    public void release() {
        // The chain itself is unchanged: the compiled plan stays valid until the
        // next append() invalidates it.
        sealed = false;
    }

    @Override
    public synchronized void splice(String anchor, Splice position, List<Link> links) {
        List<Link> next = new ArrayList<>(chain());
        int index = anchorIndex(next, anchor);
        if (index < 0) {
            throw new IllegalArgumentException("No link named '" + anchor + "' in chain");
        }
        switch (position) {
            case BEFORE -> next.addAll(index, links);
            case AFTER -> next.addAll(index + 1, links);
            case REPLACE -> {
                next.remove(index);
                next.addAll(index, links);
            }
        }
        List<Link> edited = List.copyOf(next);
        if (sealed) {
            // A rejected runtime edit leaves the previous chain (and its plan)
            // completely untouched.
            List<String> problems = ChainValidator.validate(edited);
            if (!problems.isEmpty()) {
                throw new ChainValidationException(problems);
            }
        }
        // Runtime edits pay compilation once per edit, never per request.
        publish(edited);
    }

    @Override
    public synchronized void rememberRegion(String name, Link first, Link last) {
        if (regions.containsKey(name)) {
            throw new IllegalArgumentException("Region '" + name + "' is already registered");
        }
        regions.put(name, new Region(first, last));
    }

    @Override
    public synchronized void spliceRegion(String region, List<Link> links) {
        Region span = regions.get(region);
        if (span == null) {
            throw new IllegalArgumentException("No region named '" + region + "'");
        }
        List<Link> next = new ArrayList<>(chain());
        int from = identityIndexOf(next, span.first());
        int to = identityIndexOf(next, span.last());
        if (from < 0 || to < 0 || to < from) {
            throw new IllegalStateException("Region '" + region
                    + "' boundaries are no longer in the chain (edited away by a single-link splice?)");
        }
        next.subList(from, to + 1).clear();
        next.addAll(from, links);
        List<Link> edited = List.copyOf(next);
        if (sealed) {
            List<String> problems = ChainValidator.validate(edited);
            if (!problems.isEmpty()) {
                throw new ChainValidationException(problems);
            }
        }
        publish(edited);
        // Re-point the region at its new span so it stays swappable; an
        // empty replacement retires it.
        if (links.isEmpty()) {
            regions.remove(region);
        } else {
            regions.put(region, new Region(links.get(0), links.get(links.size() - 1)));
        }
    }

    /**
     * Publishes an edited chain together with the plan that belongs to it — one
     * write, so no call can ever see the new chain paired with the old plan. A
     * sealed chain keeps a plan (recompiled here, once per edit); an unsealed
     * one is interpreted.
     */
    private void publish(List<Link> edited) {
        version.set(new ChainVersion(edited, sealed ? CompiledChain.compile(edited) : null));
    }

    private static int identityIndexOf(List<Link> links, Link target) {
        for (int i = 0; i < links.size(); i++) {
            if (links.get(i) == target) {
                return i;
            }
        }
        return -1;
    }

    @Override
    public int nextDecision() {
        return decisionIds.getAndIncrement();
    }

    @Override
    public void addErrorHandler(Consumer<Throwable> handler) {
        errorHandlers.add(handler);
    }

    @Override
    public void addCompleteHandler(Consumer<Object> handler) {
        completeHandlers.add(handler);
    }

    @Override
    public Object await() {
        try {
            CompletableFuture<Object> pending = inFlight.take();
            releasePermit();
            NioFlowMetrics sink = metrics.get();
            if (sink != null) {
                sink.queueDepth(inFlight.size());
            }
            Object value = pending.join();
            return value == FILTERED ? null : value;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while awaiting a result", e);
        }
    }

    @Override
    public Object await(Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        try {
            CompletableFuture<Object> pending = inFlight.poll(timeout.toMillis(), TimeUnit.MILLISECONDS);
            if (pending == null) {
                throw new IllegalStateException("No result available within " + timeout);
            }
            releasePermit();
            Object value = pending.get(Math.max(deadline - System.nanoTime(), 0), TimeUnit.NANOSECONDS);
            return value == FILTERED ? null : value;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while awaiting a result", e);
        } catch (ExecutionException e) {
            throw new CompletionException(e.getCause());
        } catch (TimeoutException e) {
            throw new IllegalStateException("No result available within " + timeout, e);
        }
    }

    @Override
    public int shutdown(Duration gracePeriod) {
        // 1. Stop accepting: call/inject reject from this point on.
        closed = true;
        // Wake any BLOCK producer parked on a full key lane (RFC 0039): the
        // closed flag it re-checks now makes it stop waiting, so shutdown never
        // hangs on a keyed backpressure wait.
        signalKeyLaneVacancy();
        // 2. Drain: wait up to the grace period for in-flight executions.
        int pending = awaitDrain(gracePeriod);
        // 3. Engine-owned executors are terminated; JVM-shared ones outlive the
        //    engine (shutting one flow down must never starve the others) and
        //    stragglers complete on their own threads.
        if (ownsExecutors) {
            for (ExecutorService boss : bossExecutorServices) {
                boss.shutdown();
            }
            workersExecutorService.shutdown();
            try {
                if (!workersExecutorService.awaitTermination(gracePeriod.toMillis(), TimeUnit.MILLISECONDS)) {
                    workersExecutorService.shutdownNow();
                }
                for (ExecutorService boss : bossExecutorServices) {
                    if (!boss.awaitTermination(gracePeriod.toMillis(), TimeUnit.MILLISECONDS)) {
                        boss.shutdownNow();
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                workersExecutorService.shutdownNow();
                for (ExecutorService boss : bossExecutorServices) {
                    boss.shutdownNow();
                }
            }
        }
        return pending;
    }

    private int awaitDrain(Duration gracePeriod) {
        long deadline = System.nanoTime() + gracePeriod.toNanos();
        // Poll instead of wait/notify: a LongAdder cannot signal its zero
        // transition, and this is the shutdown path — millisecond latency here
        // is invisible. The contract is unchanged: 0 returned means every
        // execution has fully reported.
        while (activeExecutions.sum() > 0 && System.nanoTime() < deadline) {
            try {
                TimeUnit.MILLISECONDS.sleep(1);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        return Math.toIntExact(Math.max(0, activeExecutions.sum()));
    }

    // Error handlers never break the engine or each other: one throwing
    // handler must not starve the rest, kill a worker or hang a future.
    void notifyError(Throwable error) {
        for (Consumer<Throwable> handler : errorHandlers) {
            try {
                handler.accept(error);
            } catch (Throwable ignored) {
                // Nowhere left to report a failing error handler.
            }
        }
    }

    // Test hook: drained key lanes must disappear (no key leak).
    int activeKeyLanes() {
        return keyLanes.size();
    }

    // Test hook: the drain counter. After everything reports it is 0; a NEGATIVE
    // value is the smoking gun of a double terminal decrementing one execution
    // twice, which the finished CAS prevents (RFC 0024).
    long inFlightCount() {
        return activeExecutions.sum();
    }

    // The backlog behind a key's running head, 0 if the key is idle. Also the
    // keyed-admission depth check (RFC 0039): size() on the lock-free queue is
    // thread-safe, and the caller reads a snapshot, so the bound is SOFT — a burst
    // of concurrent same-key admissions can overshoot slightly, which is exactly
    // what a backpressure limit needs (approximate, never a hang). Package-private
    // so the keyed tests can assert the backlog too.
    int keyLaneDepth(Object key) {
        KeyLane lane = keyLanes.get(key);
        return lane == null ? 0 : lane.waiting.size();
    }

    // Caller-thread affinity instead of a shared round-robin cursor: no atomic
    // on the call() path, and a request thread keeps landing on the same boss
    // (cache locality). Sequential thread ids spread across the pool at least
    // as evenly as a counter would; execute() is synchronous per thread, so
    // one producer never floods one boss with concurrent work.
    private ExecutorService nextBoss() {
        if (bossExecutorServices.length == 1) {
            return bossExecutorServices[0];
        }
        return bossExecutorServices[Math.floorMod(Thread.currentThread().threadId(), bossExecutorServices.length)];
    }

    // Deterministic affinity: the same key always lands on the same boss, so
    // its lane state is only ever touched by one thread — no locks. The hash is
    // spread (as HashMap does) so keys with clustered low bits — a Long id, a
    // sequential order number — do not all serialize onto one boss.
    private ExecutorService bossFor(Object key) {
        int h = key.hashCode();
        h ^= (h >>> 16);
        return bossExecutorServices[Math.floorMod(h, bossExecutorServices.length)];
    }

    private static int anchorIndex(List<Link> links, String anchor) {
        for (int i = 0; i < links.size(); i++) {
            String name = switch (links.get(i)) {
                case Stage stage -> stage.name();
                case AsyncStage async -> async.name();
                case Background background -> background.name();
                case Recovery recovery -> recovery.name();
                case FanOut fanOut -> fanOut.name();
                case Batch batch -> batch.name();
                case Fork fork -> fork.name();
                default -> null;
            };
            if (anchor.equals(name)) {
                return i;
            }
        }
        return -1;
    }

    /**
     * The failure a Recovery (and the caller's future) must see: the cause, not
     * the CompletableFuture plumbing around it. Loops, because the wrappers
     * nest — a retried stage rethrows its last failure in a CompletionException,
     * and that failure may already be one (the reactive facade wraps a Mono's
     * checked failure that way).
     */
    static Throwable unwrap(Throwable error) {
        Throwable cause = error;
        while (cause instanceof CompletionException && cause.getCause() != null) {
            cause = cause.getCause();
        }
        return cause;
    }


    // The in-flight group for a Batch link, created on first join (RFC 0032):
    // BatchGroup is a non-static inner class, so its construction stays here where
    // the enclosing instance is implicit — Execution reaches it through this
    // factory instead of `new BatchGroup(...)`, which it could not write once it
    // became a top-level class.
    BatchGroup batchGroupFor(Batch batch) {
        return batchGroups.computeIfAbsent(batch.groupKey(), ignored -> new BatchGroup(batch));
    }

    /**
     * In-flight state of one Batch link. The batch point is where otherwise
     * share-nothing executions meet, so a brief lock is the price of
     * coalescing — the documented exception to the no-locks rule; it only
     * guards list adds and the swap, never user code. The bulk function runs
     * on a worker (the flush may be triggered by the timer wheel thread,
     * which must never run user code) and each parked execution resumes on
     * its own boss through its continuation.
     */
    final class BatchGroup {

        private final Batch batch;
        private List<Object> values = new ArrayList<>();
        private List<BiConsumer<Object, Throwable>> continuations = new ArrayList<>();
        private TimerWheel.Timeout windowTimer;
        // Bumped on every flush: a window firing for an already size-flushed
        // (or previous) batch sees a stale generation and does nothing.
        private long generation;

        private BatchGroup(Batch batch) {
            this.batch = batch;
        }

        void add(Object value, BiConsumer<Object, Throwable> continuation) {
            List<Object> flushValues = null;
            List<BiConsumer<Object, Throwable>> flushContinuations = null;
            synchronized (this) {
                values.add(value);
                continuations.add(continuation);
                if (values.size() == 1) {
                    long expected = generation;
                    windowTimer = TimerWheel.shared().schedule(batch.window().toNanos(),
                            () -> stageWindowFlush(expected));
                }
                if (values.size() >= batch.size()) {
                    flushValues = values;
                    flushContinuations = continuations;
                    reset();
                }
            }
            if (flushValues != null) {
                dispatchBulk(flushValues, flushContinuations);
            }
        }

        // Scheduled on the shared TimerWheel: do the ABSOLUTE MINIMUM on the timer
        // thread (RFC 0041) — hand the flush to a worker, taking NO group lock
        // here, so a boss adding to a batch never contends the one thread that
        // ticks every timeout and window in the JVM. The generation re-check on
        // the worker still makes a size flush that beat the window a no-op.
        private void stageWindowFlush(long expectedGeneration) {
            try {
                workersExecutorService.execute(() -> flushWindow(expectedGeneration));
            } catch (RejectedExecutionException rejected) {
                // Workers gone as the window fired at shutdown: swap here (the
                // timer thread, but nothing contends now that admission is closed)
                // and fail the parked members so none hangs. The group lock this
                // RFC keeps off the timer thread in steady state is acceptable on
                // the shutdown path.
                failWindow(expectedGeneration, rejected);
            }
        }

        // Runs on a WORKER (staged above). Takes the group lock there, re-checks
        // the generation, swaps, and runs the bulk inline — already on a worker,
        // so no second dispatch hop.
        private void flushWindow(long expectedGeneration) {
            List<Object> flushValues;
            List<BiConsumer<Object, Throwable>> flushContinuations;
            synchronized (this) {
                if (generation != expectedGeneration) {
                    return; // a size flush already took this batch
                }
                flushValues = values;
                flushContinuations = continuations;
                reset();
            }
            runBulk(flushValues, flushContinuations);
        }

        private void failWindow(long expectedGeneration, Throwable cause) {
            List<BiConsumer<Object, Throwable>> flushContinuations;
            synchronized (this) {
                if (generation != expectedGeneration) {
                    return;
                }
                flushContinuations = continuations;
                reset();
            }
            flushContinuations.forEach(continuation -> continuation.accept(null, cause));
        }

        // Always called under the group lock.
        private void reset() {
            values = new ArrayList<>();
            continuations = new ArrayList<>();
            generation++;
            if (windowTimer != null) {
                windowTimer.cancel();
                windowTimer = null;
            }
        }

        private void dispatchBulk(List<Object> batchValues, List<BiConsumer<Object, Throwable>> batchContinuations) {
            try {
                workersExecutorService.execute(() -> runBulk(batchValues, batchContinuations));
            } catch (RejectedExecutionException rejected) {
                // Workers gone mid-shutdown: every parked execution must
                // still end (exceptionally), never hang.
                batchContinuations.forEach(continuation -> continuation.accept(null, rejected));
            }
        }

        private void runBulk(List<Object> batchValues, List<BiConsumer<Object, Throwable>> batchContinuations) {
            List<Object> results;
            try {
                results = batch.bulk().apply(batchValues);
                if (results == null || results.size() != batchValues.size()) {
                    throw new IllegalStateException("Batch '" + batch.name() + "' bulk returned "
                            + (results == null ? "null" : results.size() + " results")
                            + " for " + batchValues.size() + " values");
                }
            } catch (Throwable error) {
                Throwable failure = unwrap(error);
                batchContinuations.forEach(continuation -> continuation.accept(null, failure));
                return;
            }
            for (int i = 0; i < batchContinuations.size(); i++) {
                batchContinuations.get(i).accept(results.get(i), null);
            }
        }
    }

    /**
     * One execution per request: chain snapshot, decisions and result of its own,
     * pinned to one boss. Orchestration (advance/recover) always runs on that
     * boss; user code from Stage/Background/Recovery runs on the workers.
     * Runnable IS the initial boss task (advance from link 0) — no extra
     * closure per call.
     */
}
