package dev.nioflow.application.facade;

import dev.nioflow.core.facade.ChainValidationException;
import dev.nioflow.core.facade.Context;
import dev.nioflow.core.facade.Context.Key;
import dev.nioflow.core.facade.NioEngine;
import dev.nioflow.core.facade.NioFlowMetrics;
import dev.nioflow.core.model.Background;
import dev.nioflow.core.model.Batch;
import dev.nioflow.core.model.Decision;
import dev.nioflow.core.model.FanOut;
import dev.nioflow.core.model.Filter;
import dev.nioflow.core.model.FlowSignal;
import dev.nioflow.core.model.Guard;
import dev.nioflow.core.model.Link;
import dev.nioflow.core.model.OverflowPolicy;
import dev.nioflow.core.model.Recovery;
import dev.nioflow.core.model.Retry;
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
import java.util.concurrent.locks.LockSupport;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

public class DefaultNioEngine implements NioEngine {

    private static final String NIO_FLOW_BOSS = "nio-flow-boss-";

    // Public sentinel carried by raw call() futures when a Filter cut the
    // execution. Engine exits (await, complete handlers) and the flow-level
    // execute()/executeAsync() map it to null; executeResult() observes it.
    private static final Object FILTERED = FlowSignal.FILTERED;

    /**
     * Executors shared by every engine in the JVM (commonPool style): a pool of
     * daemon boss threads plus one virtual-thread worker pool, no matter how many
     * DefaultNioEngine/DefaultNioFlow instances exist. Each execution is pinned to
     * ONE boss (EventLoopGroup-style affinity), which keeps its orchestration
     * state single-threaded while letting concurrent executions spread across
     * bosses instead of queueing behind a single thread.
     */
    private static final class SharedExecutors {

        // Tunable at JVM level: -Dnioflow.bosses=N (default: available cores,
        // floor 2). Read once — the shared pool is a JVM-wide singleton.
        private static final int BOSS_COUNT = Integer.getInteger("nioflow.bosses",
                Math.max(2, Runtime.getRuntime().availableProcessors()));
        private static final ExecutorService[] BOSSES = createBossPool(BOSS_COUNT, NIO_FLOW_BOSS);
        private static final ExecutorService WORKERS = Executors.newVirtualThreadPerTaskExecutor();
    }

    private static ExecutorService[] createBossPool(int count, String namePrefix) {
        ThreadFactory factory = Thread.ofPlatform().name(namePrefix, 0).daemon(true).factory();
        ExecutorService[] bosses = new ExecutorService[count];
        for (int i = 0; i < bosses.length; i++) {
            bosses[i] = Executors.newSingleThreadExecutor(factory);
        }
        return bosses;
    }

    private final ExecutorService[] bossExecutorServices;
    private final ExecutorService workersExecutorService;
    private final boolean ownsExecutors;
    private final AtomicInteger bossCursor = new AtomicInteger();

    // Backpressure for inject/await: permits are acquired BEFORE the execution
    // starts and released when await() collects the result — the bounded
    // resource is the pending-results queue. null = unbounded (default).
    private final Semaphore inFlightPermits;
    private final OverflowPolicy overflowPolicy;
    private final int inFlightCapacity;

    // Metrics SPI: null (default) means zero instrumentation on the hot path.
    private volatile NioFlowMetrics metrics;

    // Graceful drain: closed rejects new work; activeExecutions tracks
    // in-flight ones so shutdown() can wait for them to finish.
    private volatile boolean closed;
    private final AtomicInteger activeExecutions = new AtomicInteger();
    private final Object drainLock = new Object();

    // Immutable list swapped atomically: in-flight calls keep their snapshot even
    // while the chain is being edited at runtime.
    private volatile List<Link> chain = List.of();
    private volatile boolean sealed;
    // Execution plan compiled at seal() for the current chain version (null =
    // interpreted). splice() recompiles; append() invalidates. Executions match
    // it by chain identity, so local per-request chains simply fall back.
    private volatile CompiledChain compiled;
    private final AtomicInteger decisionIds = new AtomicInteger();
    private final List<Consumer<Throwable>> errorHandlers = new CopyOnWriteArrayList<>();
    private final List<Consumer<Object>> completeHandlers = new CopyOnWriteArrayList<>();
    private final BlockingQueue<CompletableFuture<Object>> inFlight = new LinkedBlockingQueue<>();
    // One in-flight group per Batch link instance (identity equality by
    // design): executions from ANY boss park here until size or window.
    private final ConcurrentHashMap<Batch, BatchGroup> batchGroups = new ConcurrentHashMap<>();

    public DefaultNioEngine() {
        this(SharedExecutors.BOSSES, SharedExecutors.WORKERS, false, 0, OverflowPolicy.BLOCK);
    }

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
        NioFlowMetrics metrics = this.metrics;
        if (closed) {
            notifyError(new RejectedExecutionException("Engine is shut down; value rejected"));
            return;
        }
        if (!admit()) {
            // DROP: the value never runs; observable through the error handlers.
            RejectedExecutionException rejection = new RejectedExecutionException(
                    "In-flight capacity " + inFlightCapacity + " reached; value dropped");
            if (metrics != null) {
                metrics.valueDropped();
            }
            notifyError(rejection);
            return;
        }
        inFlight.add(call(input, context));
        if (metrics != null) {
            metrics.queueDepth(inFlight.size());
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
                    throw new RejectedExecutionException("In-flight capacity " + inFlightCapacity + " reached");
                }
                yield true;
            }
        };
    }

    private void releasePermit() {
        if (inFlightPermits != null) {
            inFlightPermits.release();
        }
    }

    @Override
    public CompletableFuture<Object> call(Object input, Map<String, Object> context) {
        return call(input, context, chain);
    }

    @Override
    public CompletableFuture<Object> call(Object input, Map<String, Object> context, List<Link> chain) {
        if (closed) {
            RejectedExecutionException rejection =
                    new RejectedExecutionException("Engine is shut down; call rejected");
            notifyError(rejection);
            return CompletableFuture.failedFuture(rejection);
        }
        // The plan only applies to the exact chain version it was built for;
        // execution-local chains (identity mismatch) fall back to interpreting.
        CompiledChain plan = this.compiled;
        if (plan != null && plan.links() != chain) {
            plan = null;
        }
        // The raw result future goes straight to the caller: bookkeeping
        // (drain slot, metrics, handlers) runs inside Execution BEFORE the
        // future completes, so no dependent whenComplete future is allocated.
        Execution execution = new Execution(nextBoss(), plan != null ? chain : List.copyOf(chain),
                context, plan, input);
        activeExecutions.incrementAndGet();
        try {
            execution.boss.execute(execution);
        } catch (RejectedExecutionException rejected) {
            execution.fail(rejected);
        }
        return execution.result;
    }

    @Override
    public void metrics(NioFlowMetrics metrics) {
        this.metrics = metrics;
    }

    @Override
    public List<Link> chain() {
        return chain;
    }

    @Override
    public synchronized void append(Link link) {
        if (sealed) {
            throw new IllegalStateException("Chain is sealed; call release() before appending");
        }
        List<Link> next = new ArrayList<>(chain);
        next.add(link);
        chain = List.copyOf(next);
        compiled = null;
    }

    @Override
    public synchronized void seal() {
        // Fail fast at seal time: a broken definition stops the deploy instead
        // of producing runtime surprises.
        List<String> problems = ChainValidator.validate(chain);
        if (!problems.isEmpty()) {
            throw new ChainValidationException(problems);
        }
        sealed = true;
        compiled = CompiledChain.compile(chain);
    }

    @Override
    public void release() {
        // The chain itself is unchanged: the compiled plan stays valid until the
        // next append() invalidates it.
        sealed = false;
    }

    @Override
    public synchronized void splice(String anchor, Splice position, List<Link> links) {
        List<Link> next = new ArrayList<>(chain);
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
        chain = edited;
        // Runtime edits pay compilation once per edit, never per request.
        compiled = sealed ? CompiledChain.compile(edited) : null;
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
            NioFlowMetrics metrics = this.metrics;
            if (metrics != null) {
                metrics.queueDepth(inFlight.size());
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
        synchronized (drainLock) {
            while (activeExecutions.get() > 0) {
                long remainingMillis = TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime());
                if (remainingMillis <= 0) {
                    break;
                }
                try {
                    drainLock.wait(remainingMillis);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
        return activeExecutions.get();
    }

    // Error handlers never break the engine or each other: one throwing
    // handler must not starve the rest, kill a worker or hang a future.
    private void notifyError(Throwable error) {
        for (Consumer<Throwable> handler : errorHandlers) {
            try {
                handler.accept(error);
            } catch (Throwable ignored) {
                // Nowhere left to report a failing error handler.
            }
        }
    }

    private ExecutorService nextBoss() {
        if (bossExecutorServices.length == 1) {
            return bossExecutorServices[0];
        }
        return bossExecutorServices[Math.floorMod(bossCursor.getAndIncrement(), bossExecutorServices.length)];
    }

    private static int anchorIndex(List<Link> links, String anchor) {
        for (int i = 0; i < links.size(); i++) {
            String name = switch (links.get(i)) {
                case Stage stage -> stage.name();
                case Background background -> background.name();
                case Recovery recovery -> recovery.name();
                case FanOut fanOut -> fanOut.name();
                case Batch batch -> batch.name();
                default -> null;
            };
            if (anchor.equals(name)) {
                return i;
            }
        }
        return -1;
    }

    // Highest Decision id in the chain, -1 with none: sizes the per-execution
    // decision bitset by chain content, not by the engine-wide id counter
    // (which grows forever under per-request forks).
    private static int maxDecisionId(List<Link> links) {
        int max = -1;
        for (int i = 0; i < links.size(); i++) {
            if (links.get(i) instanceof Decision decision && decision.id() > max) {
                max = decision.id();
            }
        }
        return max;
    }

    private static Throwable unwrap(Throwable error) {
        return error instanceof CompletionException && error.getCause() != null ? error.getCause() : error;
    }

    /**
     * Dispatch plan precomputed once per chain version (at seal() and after
     * each splice) instead of rescanned per execution. For every no-timeout
     * Stage it records the static fusion window [i, runEnds[i]) — bounded
     * conservatively at the first link that can never fuse (Decision,
     * Background, timeout Stage) — and, when NO link in the window carries
     * guards, the precollected runs[i] array: those dispatches do zero
     * scanning and zero allocation. Windows containing guarded links keep the
     * per-execution guard selection, just bounded to the window. It also
     * records the chain's highest Decision id so executions size their
     * decision bitset without rescanning.
     */
    private record CompiledChain(List<Link> links, Link[][] runs, int[] runEnds, int maxDecisionId) {

        static CompiledChain compile(List<Link> links) {
            int size = links.size();
            Link[][] runs = new Link[size][];
            int[] runEnds = new int[size];
            for (int i = 0; i < size; i++) {
                // No window starts at a sync stage: advance inlines it on the
                // boss and never dispatches there (validated chains can't
                // carry sync+timeout/retry). It still FUSES into a preceding
                // stage's window like any other no-timeout stage.
                if (!(links.get(i) instanceof Stage stage) || stage.timeout() != null || stage.sync()) {
                    continue;
                }
                int end = i + 1;
                while (end < size && extendsWindow(links.get(end))) {
                    end++;
                }
                runEnds[i] = end;
                if (unguarded(links, i, end)) {
                    runs[i] = links.subList(i, end).toArray(Link[]::new);
                }
            }
            return new CompiledChain(links, runs, runEnds, DefaultNioEngine.maxDecisionId(links));
        }

        private static boolean staticallyFusable(Link link) {
            return link instanceof Filter
                    || link instanceof Recovery
                    || (link instanceof Stage stage && stage.timeout() == null);
        }

        // Fusion across recorded decisions: a GUARDED non-fusable link (a
        // match() case's Decision, a lane's Background/FanOut) might be
        // skipped at runtime — its guards depend on decisions already
        // recorded — so the window extends through it and the per-execution
        // scan decides: guard-failed links are stepped over (a skipped
        // Decision records nothing), a passing one still ends the run there.
        // An UNGUARDED non-fusable link always executes: hard boundary, same
        // as before. This matches what the interpreted scan (no plan) already
        // does with its links.size() bound.
        private static boolean extendsWindow(Link link) {
            if (staticallyFusable(link)) {
                return true;
            }
            List<Guard> guards = link.guards();
            return guards != null && !guards.isEmpty();
        }

        private static boolean unguarded(List<Link> links, int from, int to) {
            for (int i = from; i < to; i++) {
                List<Guard> guards = links.get(i).guards();
                if (guards != null && !guards.isEmpty()) {
                    return false;
                }
            }
            return true;
        }
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
    private final class BatchGroup {

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
                            () -> flushWindow(expected));
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
            dispatchBulk(flushValues, flushContinuations);
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
    private final class Execution implements Runnable {

        // Bitset limit: ids 0..511 fit in 16 longs (128 bytes). Beyond that the
        // bitset would outgrow a small map, so decisions overflow into one.
        private static final int MAX_BITSET_DECISION_ID = 511;

        private final ExecutorService boss;
        private final List<Link> links;
        private final Object input;
        // Per-execution context: null until a contextual stage puts the first
        // entry (or the caller handed a map to call/inject). Stage
        // applications are serialized by the executor handoffs — one
        // continuation at a time, each hop a happens-before edge — so a plain
        // HashMap is enough. Only touched through ExecutionContext views.
        private Map<String, Object> context;
        // Exactly-once completion guard. Written on the boss; the only off-boss
        // writer is the resume-rejection path, which implies the boss is gone.
        private volatile boolean finished;
        // Decisions as a bitset, 2 bits per id (bit 0 = recorded, bit 1 = value),
        // sized to the chain's highest Decision id: recording and guard checks
        // are O(1) with zero allocation, and an unrecorded decision fails any
        // guard on it — the property match() first-match-wins relies on. null
        // when the chain has no decisions, or when its ids outgrew the limit
        // (then decisionsOverflow takes over, lazily).
        private final long[] decisionBits;
        private Map<Integer, Boolean> decisionsOverflow;
        private final CompletableFuture<Object> result = new CompletableFuture<>();
        // Precompiled dispatch plan for this exact chain version; null = interpret.
        private final CompiledChain plan;
        // Snapshot of the installed metrics for this execution; null = untimed.
        private final NioFlowMetrics metrics;
        private final long startNanos;

        private Execution(ExecutorService boss, List<Link> links, Map<String, Object> context,
                          CompiledChain plan, Object input) {
            this.boss = boss;
            this.links = links;
            this.context = context;
            this.plan = plan;
            this.input = input;
            int maxDecision = plan != null ? plan.maxDecisionId() : maxDecisionId(links);
            this.decisionBits = maxDecision >= 0 && maxDecision <= MAX_BITSET_DECISION_ID
                    ? new long[(maxDecision >>> 5) + 1]
                    : null;
            this.metrics = DefaultNioEngine.this.metrics;
            this.startNanos = metrics != null ? System.nanoTime() : 0;
        }

        @Override
        public void run() {
            advance(0, input);
        }

        /**
         * Exactly-once terminal completion. Bookkeeping (drain slot, metrics,
         * handlers) runs BEFORE the future completes so a joining caller
         * always observes it done — same ordering the old whenComplete
         * wrapper gave, without allocating a dependent future per call.
         */
        private void complete(Object value) {
            if (finished) {
                return;
            }
            finished = true;
            finishBookkeeping(value, null);
            result.complete(value);
        }

        private void fail(Throwable error) {
            if (finished) {
                return;
            }
            finished = true;
            finishBookkeeping(null, unwrap(error));
            result.completeExceptionally(error);
        }

        private void finishBookkeeping(Object value, Throwable error) {
            if (activeExecutions.decrementAndGet() == 0) {
                synchronized (drainLock) {
                    drainLock.notifyAll();
                }
            }
            if (metrics != null) {
                long elapsed = System.nanoTime() - startNanos;
                if (error != null) {
                    metrics.executionFailed(error, elapsed);
                } else if (value == FILTERED) {
                    metrics.executionFiltered(elapsed);
                } else {
                    metrics.executionCompleted(elapsed);
                }
            }
            // Hardened on purpose: this runs BEFORE the result future
            // completes, so a throwing handler must never escape — it would
            // leave the caller's future hanging forever. A failing complete
            // handler is reported through the error handlers instead.
            if (error != null) {
                notifyError(error);
            } else {
                Object exposed = value == FILTERED ? null : value;
                for (Consumer<Object> handler : completeHandlers) {
                    try {
                        handler.accept(exposed);
                    } catch (Throwable failure) {
                        notifyError(failure);
                    }
                }
            }
        }

        // Iterative, never recursive: a deep chain of cheap links is walked
        // entirely on the boss and must not depend on stack size.
        private void advance(int index, Object value) {
            Object current = value;
            while (index < links.size()) {
                Link link = links.get(index);
                if (passesGuards(link)) {
                    try {
                        switch (link) {
                            case Stage stage -> {
                                // Opt-in boss inline: a sync stage skips both thread
                                // hops. timeout/retry force the dispatch path (they
                                // need a worker); validation flags that combination.
                                if (stage.sync() && stage.timeout() == null && stage.retry() == null) {
                                    current = timedApply(stage, current);
                                } else {
                                    dispatch(index, current);
                                    return; // the worker resumes on the boss when done
                                }
                            }
                            case Decision decision -> recordDecision(decision.id(), decision.predicate().test(current));
                            case Filter filter -> {
                                if (!filter.predicate().test(current)) {
                                    complete(FILTERED);
                                    return;
                                }
                            }
                            case Background background -> {
                                Object snapshot = current;
                                workersExecutorService.execute(() -> runBackground(background, snapshot));
                            }
                            case FanOut fanOut -> {
                                dispatchFanOut(fanOut, index + 1, current);
                                return; // the join resumes on the boss when all branches finish
                            }
                            case Batch batch -> {
                                // Parks this execution in the link's shared group; the
                                // flush (size or window) resumes it on ITS boss with
                                // its own element of the bulk result.
                                joinBatch(batch, index + 1, current);
                                return;
                            }
                            case Recovery ignored -> {
                                // Only applies on the error path (see recover)
                            }
                        }
                    } catch (Throwable error) {
                        // A throwing Decision/Filter predicate fails the value, never
                        // the boss task — otherwise the request future hangs forever.
                        recover(index + 1, error);
                        return;
                    }
                }
                index++;
            }
            complete(current);
        }

        /**
         * Stage fusion: starting at index, take the maximal run of consecutive
         * no-timeout Stages, Filters and Recoveries (guard-skipped links inside
         * the run are stepped over — decisions cannot change until the next
         * passing Decision, which ends the run). The whole run travels
         * boss→worker→boss as ONE composed function: 2 thread hops per run
         * instead of 2 per link. Fused Filter predicates run on the worker; a
         * rejection returns the FILTERED sentinel and completes the flow with
         * null, same as a boss-side cut. Fused Recoveries preserve positional
         * semantics inside the run: a failure looks forward for the next
         * Recovery in the run and continues from there; with none left, the
         * failure escapes the run and recover(resume) scans the rest of the
         * chain — equivalent, because everything between the failure and the
         * run's end has already been searched.
         */
        private void dispatch(int index, Object value) {
            Stage first = (Stage) links.get(index);
            if (first.timeout() != null) {
                dispatchWithTimeout(first, index + 1, value);
                return;
            }
            Link[] run;
            int resume;
            if (plan != null && plan.runs()[index] != null) {
                // Precompiled unguarded run: zero scanning, zero allocation.
                run = plan.runs()[index];
                resume = plan.runEnds()[index];
            } else {
                // Interpreted scan (no plan, or the window contains guarded links
                // whose selection depends on this execution's decisions), bounded
                // to the precompiled window when one exists.
                int limit = plan != null ? plan.runEnds()[index] : links.size();
                List<Link> selected = null;
                int next = index + 1;
                while (next < limit) {
                    Link link = links.get(next);
                    if (!passesGuards(link)) {
                        next++;
                        continue;
                    }
                    boolean fusable = link instanceof Filter
                            || link instanceof Recovery
                            || (link instanceof Stage stage && stage.timeout() == null);
                    if (!fusable) {
                        break;
                    }
                    if (selected == null) {
                        selected = new ArrayList<>();
                        selected.add(first);
                    }
                    selected.add(link);
                    next++;
                }
                resume = next;
                run = selected == null ? null : selected.toArray(Link[]::new);
            }
            // Manual boss→worker→boss handoff: the CompletableFuture machinery
            // (async task, dependent future, composition nodes) buys nothing
            // here — no composition, no timeout — and costs several
            // allocations per dispatch. Two plain closures do the round trip.
            Link[] selectedRun = run;
            int resumeAt = resume;
            workersExecutorService.execute(() -> {
                Object outcome;
                try {
                    outcome = selectedRun == null || selectedRun.length == 1
                            ? applyStage(first, value)
                            : applyRun(selectedRun, value);
                } catch (Throwable error) {
                    resumeOnBoss(() -> recover(resumeAt, unwrap(error)));
                    return;
                }
                Object nextValue = outcome;
                resumeOnBoss(() -> {
                    if (nextValue == FILTERED) {
                        complete(FILTERED);
                    } else {
                        advance(resumeAt, nextValue);
                    }
                });
            });
        }

        // Orchestration may only resume on the boss. A rejection means the
        // engine-owned boss executor was shut down mid-flight: the execution
        // can only end exceptionally (never silently, never on this thread).
        private void resumeOnBoss(Runnable continuation) {
            try {
                boss.execute(continuation);
            } catch (RejectedExecutionException rejected) {
                fail(rejected);
            }
        }

        // The fused run, applied on the worker as one plain call chain.
        private Object applyRun(Link[] run, Object value) {
            Object current = value;
            for (int i = 0; i < run.length; i++) {
                try {
                    if (run[i] instanceof Stage stage) {
                        current = applyStage(stage, current);
                    } else if (run[i] instanceof Filter filter && !filter.predicate().test(current)) {
                        return FILTERED;
                    }
                    // Recovery: skipped on the happy path
                } catch (Throwable error) {
                    // Positional semantics inside the run: look forward for the
                    // next Recovery; a throwing recovery keeps scanning with the
                    // new failure; with none left, escape the run (the boss then
                    // scans the rest of the chain from the run's end).
                    Throwable pending = error;
                    int next = i + 1;
                    boolean recovered = false;
                    while (next < run.length) {
                        if (run[next] instanceof Recovery recovery) {
                            try {
                                current = recovery.function().apply(pending);
                                if (metrics != null) {
                                    metrics.recoveryApplied(recovery.name());
                                }
                                recovered = true;
                                break;
                            } catch (Throwable failure) {
                                pending = failure;
                            }
                        }
                        next++;
                    }
                    if (!recovered) {
                        throw new CompletionException(pending);
                    }
                    i = next;
                }
            }
            return current;
        }

        // Hands this execution to the batch group and stops advancing; the
        // group's flush calls the continuation, which hops back to THIS
        // execution's boss to resume (or recover) — affinity preserved.
        private void joinBatch(Batch batch, int resume, Object value) {
            batchGroups.computeIfAbsent(batch, BatchGroup::new)
                    .add(value, (element, error) -> resumeOnBoss(() -> {
                        if (error != null) {
                            recover(resume, error);
                        } else {
                            advance(resume, element);
                        }
                    }));
        }

        /**
         * Parallel split-join: every branch gets the same value and its own
         * worker; the join runs on a worker once all branches finish (user code
         * never touches the boss) and the combined value resumes on the boss.
         * A failing branch fails the whole fan-out through the recovery path.
         */
        private void dispatchFanOut(FanOut fanOut, int resume, Object value) {
            List<Function<Object, Object>> branches = fanOut.branches();
            @SuppressWarnings("unchecked")
            CompletableFuture<Object>[] tasks = new CompletableFuture[branches.size()];
            for (int i = 0; i < branches.size(); i++) {
                var branch = branches.get(i);
                tasks[i] = CompletableFuture.supplyAsync(() -> branch.apply(value), workersExecutorService);
            }
            CompletableFuture.allOf(tasks)
                    .thenApplyAsync(ignored -> {
                        List<Object> results = new ArrayList<>(tasks.length);
                        for (CompletableFuture<Object> task : tasks) {
                            results.add(task.join());
                        }
                        return fanOut.join().apply(results);
                    }, workersExecutorService)
                    .whenCompleteAsync((nextValue, error) -> {
                        if (error != null) {
                            recover(resume, unwrap(error));
                        } else {
                            advance(resume, nextValue);
                        }
                    }, boss);
        }

        private void dispatchWithTimeout(Stage stage, int resume, Object value) {
            attemptWithTimeout(stage, resume, value, 1);
        }

        /**
         * Timeout + retry composition: the budget applies to EACH attempt (a
         * hung attempt is cut externally, which an inline loop could not do),
         * the backoff is scheduled without parking anyone, and once attempts
         * are exhausted the last failure flows to the recovery path.
         *
         * The budget is armed on the shared TimerWheel — O(1) schedule and
         * cancel — instead of orTimeout's lock-guarded priority queue; the
         * attempt future's internal CAS arbitrates the completion/timeout
         * race exactly as before. Retry backoff keeps delayedExecutor: it
         * only runs after a failure (cold) and a sub-tick backoff would
         * degrade to wheel granularity.
         */
        private void attemptWithTimeout(Stage stage, int resume, Object value, int attempt) {
            CompletableFuture<Object> attemptResult = new CompletableFuture<>();
            workersExecutorService.execute(() -> {
                try {
                    attemptResult.complete(timedApply(stage, value));
                } catch (Throwable error) {
                    attemptResult.completeExceptionally(error);
                }
            });
            TimerWheel.Timeout budget = TimerWheel.shared().schedule(stage.timeout().toNanos(),
                    () -> attemptResult.completeExceptionally(
                            new TimeoutException("Stage '" + stage.name() + "' exceeded " + stage.timeout())));
            attemptResult.whenCompleteAsync((nextValue, error) -> {
                budget.cancel();
                if (error == null) {
                    advance(resume, nextValue);
                    return;
                }
                Retry retry = stage.retry();
                if (retry != null && attempt < retry.attempts()) {
                    if (metrics != null) {
                        metrics.stageRetried(stage.name());
                    }
                    CompletableFuture.delayedExecutor(retry.delayNanos(attempt), TimeUnit.NANOSECONDS, boss)
                            .execute(() -> attemptWithTimeout(stage, resume, value, attempt + 1));
                } else {
                    recover(resume, unwrap(error));
                }
            }, boss);
        }

        /**
         * Runs the stage on a worker honoring its retry policy: failed attempts
         * back off by parking the (virtual) worker thread — cheap — and the
         * last failure escapes to the caller's error path (in-run recovery or
         * recover()). Works unchanged inside fused runs: retrying never breaks
         * fusion. Timeout+retry stages do NOT go through here (a hung attempt
         * cannot be cut inline; see attemptWithTimeout).
         */
        private Object applyStage(Stage stage, Object value) {
            Retry retry = stage.retry();
            if (retry == null) {
                return timedApply(stage, value);
            }
            Throwable last = null;
            for (int attempt = 1; attempt <= retry.attempts(); attempt++) {
                if (attempt > 1) {
                    if (metrics != null) {
                        metrics.stageRetried(stage.name());
                    }
                    LockSupport.parkNanos(retry.delayNanos(attempt - 1));
                }
                try {
                    return timedApply(stage, value);
                } catch (Throwable error) {
                    last = error;
                }
            }
            throw new CompletionException(last);
        }

        // Runs the stage function, timing it when metrics are installed.
        // Called on a worker thread — except for boss-inlined sync stages.
        private Object timedApply(Stage stage, Object value) {
            if (metrics == null) {
                return invoke(stage, value);
            }
            long start = System.nanoTime();
            Object next = invoke(stage, value);
            metrics.stageCompleted(stage.name(), System.nanoTime() - start);
            return next;
        }

        // The single point where stage functions run: contextual stages get
        // the execution's Context here; plain stages pay one instanceof. The
        // view is stateless (it reads/writes the execution's map), so it is
        // allocated per contextual application instead of adding a field
        // that every context-free execution would pay for.
        private Object invoke(Stage stage, Object value) {
            return stage.function() instanceof ContextualFunction contextual
                    ? contextual.body().apply(value, new ExecutionContext())
                    : stage.function().apply(value);
        }

        private final class ExecutionContext implements Context {

            @Override
            @SuppressWarnings("unchecked")
            public <T> T get(Key<T> key) {
                Map<String, Object> map = context;
                return map == null ? null : (T) map.get(key.name());
            }

            @Override
            public <T> T getOrDefault(Key<T> key, T fallback) {
                T value = get(key);
                return value == null ? fallback : value;
            }

            @Override
            public <T> Context put(Key<T> key, T value) {
                if (context == null) {
                    context = new HashMap<>();
                }
                context.put(key.name(), value);
                return this;
            }
        }

        private void recover(int from, Throwable error) {
            for (int i = from; i < links.size(); i++) {
                if (links.get(i) instanceof Recovery recovery && passesGuards(recovery)) {
                    int next = i + 1;
                    CompletableFuture.supplyAsync(() -> applyRecovery(recovery, error), workersExecutorService)
                            .whenCompleteAsync((value, failure) -> {
                                if (failure != null) {
                                    recover(next, unwrap(failure));
                                } else {
                                    advance(next, value);
                                }
                            }, boss);
                    return;
                }
            }
            fail(error);
        }

        private Object applyRecovery(Recovery recovery, Throwable error) {
            Object value = recovery.function().apply(error);
            if (metrics != null) {
                metrics.recoveryApplied(recovery.name());
            }
            return value;
        }

        private void runBackground(Background background, Object value) {
            try {
                background.effect().accept(value);
            } catch (Throwable error) {
                notifyError(error);
            }
        }

        // Ids recorded here always fit the bitset: it was sized from the same
        // chain this Decision came from. Only ids past the limit go to the map.
        private void recordDecision(int id, boolean value) {
            if (decisionBits != null) {
                int shift = (id & 31) << 1;
                decisionBits[id >>> 5] = (decisionBits[id >>> 5] & ~(0b11L << shift))
                        | ((value ? 0b11L : 0b01L) << shift);
            } else {
                if (decisionsOverflow == null) {
                    decisionsOverflow = new HashMap<>();
                }
                decisionsOverflow.put(id, value);
            }
        }

        // Plain loop on purpose: this runs on the boss for every link of every
        // execution — no streams or allocations in the hot path.
        private boolean passesGuards(Link link) {
            List<Guard> guards = link.guards();
            if (guards == null || guards.isEmpty()) {
                return true;
            }
            for (int i = 0; i < guards.size(); i++) {
                Guard guard = guards.get(i);
                int id = guard.decision();
                if (decisionBits != null) {
                    // Out-of-range ids (dangling or negative guards — >>> maps
                    // negatives past any length) read as never recorded.
                    int word = id >>> 5;
                    if (word >= decisionBits.length) {
                        return false;
                    }
                    long bits = decisionBits[word] >>> ((id & 31) << 1);
                    if ((bits & 0b01L) == 0 || ((bits & 0b10L) != 0) != guard.expected()) {
                        return false;
                    }
                } else {
                    Boolean recorded = decisionsOverflow == null ? null : decisionsOverflow.get(id);
                    if (recorded == null || recorded != guard.expected()) {
                        return false;
                    }
                }
            }
            return true;
        }
    }
}
