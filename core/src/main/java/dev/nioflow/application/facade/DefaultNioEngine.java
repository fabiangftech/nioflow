package dev.nioflow.application.facade;

import dev.nioflow.core.facade.ChainValidationException;
import dev.nioflow.core.facade.NioEngine;
import dev.nioflow.core.facade.NioFlowMetrics;
import dev.nioflow.core.model.Background;
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

        private static final int BOSS_COUNT = Math.max(2, Runtime.getRuntime().availableProcessors());
        private static final ExecutorService[] BOSSES = createBosses();
        private static final ExecutorService WORKERS = Executors.newVirtualThreadPerTaskExecutor();

        private static ExecutorService[] createBosses() {
            ThreadFactory factory = Thread.ofPlatform().name(NIO_FLOW_BOSS, 0).daemon(true).factory();
            ExecutorService[] bosses = new ExecutorService[BOSS_COUNT];
            for (int i = 0; i < bosses.length; i++) {
                bosses[i] = Executors.newSingleThreadExecutor(factory);
            }
            return bosses;
        }
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
        inject(input, new ConcurrentHashMap<>());
    }

    @Override
    public void inject(Object input, Map<String, Object> context) {
        NioFlowMetrics metrics = this.metrics;
        if (closed) {
            errorHandlers.forEach(handler -> handler.accept(
                    new RejectedExecutionException("Engine is shut down; value rejected")));
            return;
        }
        if (!admit()) {
            // DROP: the value never runs; observable through the error handlers.
            RejectedExecutionException rejection = new RejectedExecutionException(
                    "In-flight capacity " + inFlightCapacity + " reached; value dropped");
            if (metrics != null) {
                metrics.valueDropped();
            }
            errorHandlers.forEach(handler -> handler.accept(rejection));
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
            errorHandlers.forEach(handler -> handler.accept(rejection));
            return CompletableFuture.failedFuture(rejection);
        }
        // The plan only applies to the exact chain version it was built for;
        // execution-local chains (identity mismatch) fall back to interpreting.
        CompiledChain plan = this.compiled;
        if (plan != null && plan.links() != chain) {
            plan = null;
        }
        Execution execution = new Execution(nextBoss(), plan != null ? chain : List.copyOf(chain),
                context != null ? context : new ConcurrentHashMap<>(), plan);
        activeExecutions.incrementAndGet();
        execution.boss.execute(() -> execution.advance(0, input));
        return execution.result.whenComplete((value, error) -> {
            if (activeExecutions.decrementAndGet() == 0) {
                synchronized (drainLock) {
                    drainLock.notifyAll();
                }
            }
            if (execution.metrics != null) {
                long elapsed = System.nanoTime() - execution.startNanos;
                if (error != null) {
                    execution.metrics.executionFailed(unwrap(error), elapsed);
                } else if (value == FILTERED) {
                    execution.metrics.executionFiltered(elapsed);
                } else {
                    execution.metrics.executionCompleted(elapsed);
                }
            }
            if (error != null) {
                errorHandlers.forEach(handler -> handler.accept(unwrap(error)));
            } else {
                Object exposed = value == FILTERED ? null : value;
                completeHandlers.forEach(handler -> handler.accept(exposed));
            }
        });
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
                if (!(links.get(i) instanceof Stage stage) || stage.timeout() != null) {
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
     * One execution per request: chain snapshot, decisions and result of its own,
     * pinned to one boss. Orchestration (advance/recover) always runs on that
     * boss; user code from Stage/Background/Recovery runs on the workers.
     */
    private final class Execution {

        // Bitset limit: ids 0..511 fit in 16 longs (128 bytes). Beyond that the
        // bitset would outgrow a small map, so decisions overflow into one.
        private static final int MAX_BITSET_DECISION_ID = 511;

        private final ExecutorService boss;
        private final List<Link> links;
        private final Map<String, Object> context;
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
                          CompiledChain plan) {
            this.boss = boss;
            this.links = links;
            this.context = context;
            this.plan = plan;
            int maxDecision = plan != null ? plan.maxDecisionId() : maxDecisionId(links);
            this.decisionBits = maxDecision >= 0 && maxDecision <= MAX_BITSET_DECISION_ID
                    ? new long[(maxDecision >>> 5) + 1]
                    : null;
            this.metrics = DefaultNioEngine.this.metrics;
            this.startNanos = metrics != null ? System.nanoTime() : 0;
        }

        // Iterative, never recursive: a deep chain of cheap links is walked
        // entirely on the boss and must not depend on stack size.
        private void advance(int index, Object value) {
            while (index < links.size()) {
                Link link = links.get(index);
                if (passesGuards(link)) {
                    try {
                        switch (link) {
                            case Stage ignored -> {
                                dispatch(index, value);
                                return; // the worker resumes on the boss when done
                            }
                            case Decision decision -> recordDecision(decision.id(), decision.predicate().test(value));
                            case Filter filter -> {
                                if (!filter.predicate().test(value)) {
                                    result.complete(FILTERED);
                                    return;
                                }
                            }
                            case Background background ->
                                    workersExecutorService.execute(() -> runBackground(background, value));
                            case FanOut fanOut -> {
                                dispatchFanOut(fanOut, index + 1, value);
                                return; // the join resumes on the boss when all branches finish
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
            result.complete(value);
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
            CompletableFuture<Object> task = run == null || run.length == 1
                    ? CompletableFuture.supplyAsync(() -> applyStage(first, value), workersExecutorService)
                    : fusedTask(run, value);
            task.whenCompleteAsync((nextValue, error) -> {
                if (error != null) {
                    recover(resume, unwrap(error));
                } else if (nextValue == FILTERED) {
                    result.complete(FILTERED);
                } else {
                    advance(resume, nextValue);
                }
            }, boss);
        }

        private CompletableFuture<Object> fusedTask(Link[] run, Object value) {
            return CompletableFuture.supplyAsync(() -> {
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
            }, workersExecutorService);
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
         * hung attempt is cut by orTimeout, which an inline loop could not do),
         * the backoff is scheduled without parking anyone, and once attempts
         * are exhausted the last failure flows to the recovery path.
         */
        private void attemptWithTimeout(Stage stage, int resume, Object value, int attempt) {
            CompletableFuture.supplyAsync(() -> timedApply(stage, value), workersExecutorService)
                    .orTimeout(stage.timeout().toMillis(), TimeUnit.MILLISECONDS)
                    .whenCompleteAsync((nextValue, error) -> {
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

        // Runs the stage function, timing it when metrics are installed. Always
        // called on a worker thread.
        private Object timedApply(Stage stage, Object value) {
            if (metrics == null) {
                return stage.function().apply(value);
            }
            long start = System.nanoTime();
            Object next = stage.function().apply(value);
            metrics.stageCompleted(stage.name(), System.nanoTime() - start);
            return next;
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
            result.completeExceptionally(error);
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
                errorHandlers.forEach(handler -> handler.accept(error));
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
