package dev.nioflow.application.facade;

import dev.nioflow.core.facade.NioEngine;
import dev.nioflow.core.model.Background;
import dev.nioflow.core.model.Decision;
import dev.nioflow.core.model.Filter;
import dev.nioflow.core.model.Guard;
import dev.nioflow.core.model.Link;
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
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

public class DefaultNioEngine implements NioEngine {

    private static final String NIO_FLOW_BOSS = "nio-flow-boss-";

    // Sentinel returned by a fused run when a Filter rejected the value; never
    // escapes the engine (the flow completes with null, as with a boss-side cut).
    private static final Object FILTERED = new Object();

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

    // Immutable list swapped atomically: in-flight calls keep their snapshot even
    // while the chain is being edited at runtime.
    private volatile List<Link> chain = List.of();
    private volatile boolean sealed;
    private final AtomicInteger decisionIds = new AtomicInteger();
    private final List<Consumer<Throwable>> errorHandlers = new CopyOnWriteArrayList<>();
    private final List<Consumer<Object>> completeHandlers = new CopyOnWriteArrayList<>();
    private final BlockingQueue<CompletableFuture<Object>> inFlight = new LinkedBlockingQueue<>();

    public DefaultNioEngine() {
        this.bossExecutorServices = SharedExecutors.BOSSES;
        this.workersExecutorService = SharedExecutors.WORKERS;
        this.ownsExecutors = false;
    }

    public DefaultNioEngine(ExecutorService bossExecutorService,
                            ExecutorService workersExecutorService) {
        this.bossExecutorServices = new ExecutorService[]{bossExecutorService};
        this.workersExecutorService = workersExecutorService;
        this.ownsExecutors = true;
    }

    @Override
    public void inject(Object input) {
        inject(input, new ConcurrentHashMap<>());
    }

    @Override
    public void inject(Object input, Map<String, Object> context) {
        inFlight.add(call(input, context));
    }

    @Override
    public CompletableFuture<Object> call(Object input, Map<String, Object> context) {
        return call(input, context, chain);
    }

    @Override
    public CompletableFuture<Object> call(Object input, Map<String, Object> context, List<Link> chain) {
        Execution execution = new Execution(nextBoss(), List.copyOf(chain),
                context != null ? context : new ConcurrentHashMap<>());
        execution.boss.execute(() -> execution.advance(0, input));
        return execution.result.whenComplete((value, error) -> {
            if (error != null) {
                errorHandlers.forEach(handler -> handler.accept(unwrap(error)));
            } else {
                completeHandlers.forEach(handler -> handler.accept(value));
            }
        });
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
    }

    @Override
    public void seal() {
        sealed = true;
    }

    @Override
    public void release() {
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
        chain = List.copyOf(next);
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
            return inFlight.take().join();
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
            return pending.get(Math.max(deadline - System.nanoTime(), 0), TimeUnit.NANOSECONDS);
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
    public void shutdown(Duration gracePeriod) {
        if (!ownsExecutors) {
            // JVM-shared executors outlive the engine: shutting one flow down must
            // never starve the others.
            return;
        }
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
                default -> null;
            };
            if (anchor.equals(name)) {
                return i;
            }
        }
        return -1;
    }

    private static Throwable unwrap(Throwable error) {
        return error instanceof CompletionException && error.getCause() != null ? error.getCause() : error;
    }

    /**
     * One execution per request: chain snapshot, decisions and result of its own,
     * pinned to one boss. Orchestration (advance/recover) always runs on that
     * boss; user code from Stage/Background/Recovery runs on the workers.
     */
    private final class Execution {

        private final ExecutorService boss;
        private final List<Link> links;
        private final Map<String, Object> context;
        private final Map<Integer, Boolean> decisions = new HashMap<>();
        private final CompletableFuture<Object> result = new CompletableFuture<>();

        private Execution(ExecutorService boss, List<Link> links, Map<String, Object> context) {
            this.boss = boss;
            this.links = links;
            this.context = context;
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
                            case Decision decision -> decisions.put(decision.id(), decision.predicate().test(value));
                            case Filter filter -> {
                                if (!filter.predicate().test(value)) {
                                    result.complete(null);
                                    return;
                                }
                            }
                            case Background background ->
                                    workersExecutorService.execute(() -> runBackground(background, value));
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
         * no-timeout Stages and Filters (guard-skipped links inside the run are
         * stepped over — decisions cannot change until the next passing Decision,
         * which ends the run). The whole run travels boss→worker→boss as ONE
         * composed function: 2 thread hops per run instead of 2 per link. Fused
         * Filter predicates run on the worker (off the boss); a rejection returns
         * the FILTERED sentinel and completes the flow with null, same as a
         * boss-side cut. A failure anywhere in the run recovers from the end of
         * the run, which is equivalent because a run contains no Recovery links
         * by construction.
         */
        private void dispatch(int index, Object value) {
            Stage first = (Stage) links.get(index);
            if (first.timeout() != null) {
                dispatchWithTimeout(first, index + 1, value);
                return;
            }
            List<Link> run = null;
            int next = index + 1;
            while (next < links.size()) {
                Link link = links.get(next);
                if (!passesGuards(link)) {
                    next++;
                    continue;
                }
                boolean fusable = link instanceof Filter
                        || (link instanceof Stage stage && stage.timeout() == null);
                if (!fusable) {
                    break;
                }
                if (run == null) {
                    run = new ArrayList<>();
                    run.add(first);
                }
                run.add(link);
                next++;
            }
            int resume = next;
            CompletableFuture<Object> task = run == null
                    ? CompletableFuture.supplyAsync(() -> first.function().apply(value), workersExecutorService)
                    : fusedTask(run, value);
            task.whenCompleteAsync((nextValue, error) -> {
                if (error != null) {
                    recover(resume, unwrap(error));
                } else if (nextValue == FILTERED) {
                    result.complete(null);
                } else {
                    advance(resume, nextValue);
                }
            }, boss);
        }

        private CompletableFuture<Object> fusedTask(List<Link> run, Object value) {
            return CompletableFuture.supplyAsync(() -> {
                Object current = value;
                for (int i = 0; i < run.size(); i++) {
                    if (run.get(i) instanceof Stage stage) {
                        current = stage.function().apply(current);
                    } else if (run.get(i) instanceof Filter filter && !filter.predicate().test(current)) {
                        return FILTERED;
                    }
                }
                return current;
            }, workersExecutorService);
        }

        private void dispatchWithTimeout(Stage stage, int resume, Object value) {
            CompletableFuture.supplyAsync(() -> stage.function().apply(value), workersExecutorService)
                    .orTimeout(stage.timeout().toMillis(), TimeUnit.MILLISECONDS)
                    .whenCompleteAsync((nextValue, error) -> {
                        if (error != null) {
                            recover(resume, unwrap(error));
                        } else {
                            advance(resume, nextValue);
                        }
                    }, boss);
        }

        private void recover(int from, Throwable error) {
            for (int i = from; i < links.size(); i++) {
                if (links.get(i) instanceof Recovery recovery && passesGuards(recovery)) {
                    int next = i + 1;
                    CompletableFuture.supplyAsync(() -> recovery.function().apply(error), workersExecutorService)
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

        private void runBackground(Background background, Object value) {
            try {
                background.effect().accept(value);
            } catch (Throwable error) {
                errorHandlers.forEach(handler -> handler.accept(error));
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
                Boolean recorded = decisions.get(guard.decision());
                if (recorded == null || recorded != guard.expected()) {
                    return false;
                }
            }
            return true;
        }
    }
}
