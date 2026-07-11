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
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

public class DefaultNioEngine implements NioEngine {

    private static final String NIO_FLOW_BOSS = "nio-flow-boss-";

    /**
     * Executors compartidos por todos los engines del JVM (mismo patrón que
     * ForkJoinPool.commonPool()): un solo boss y un solo pool de workers sin
     * importar cuántos DefaultNioEngine/DefaultNioFlow se creen. El holder
     * los inicializa perezosamente y el boss es daemon para no impedir el
     * apagado del JVM.
     */
    private static final class SharedExecutors {

        private static final ExecutorService BOSS = Executors.newSingleThreadExecutor(
                Thread.ofPlatform().name(NIO_FLOW_BOSS, 0).daemon(true).factory());
        private static final ExecutorService WORKERS = Executors.newVirtualThreadPerTaskExecutor();
    }

    private final ExecutorService bossExecutorService;
    private final ExecutorService workersExecutorService;
    private final boolean ownsExecutors;

    // Inmutable + swap atómico: los call() en vuelo conservan su snapshot aunque la chain se edite.
    private volatile List<Link> chain = List.of();
    private volatile boolean sealed;
    private final AtomicInteger decisionIds = new AtomicInteger();
    private final List<Consumer<Throwable>> errorHandlers = new CopyOnWriteArrayList<>();
    private final List<Consumer<Object>> completeHandlers = new CopyOnWriteArrayList<>();
    private final BlockingQueue<CompletableFuture<Object>> inFlight = new LinkedBlockingQueue<>();

    public DefaultNioEngine() {
        this.bossExecutorService = SharedExecutors.BOSS;
        this.workersExecutorService = SharedExecutors.WORKERS;
        this.ownsExecutors = false;
    }

    public DefaultNioEngine(ExecutorService bossExecutorService,
                            ExecutorService workersExecutorService) {
        this.bossExecutorService = bossExecutorService;
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
        Execution execution = new Execution(List.copyOf(chain),
                context != null ? context : new ConcurrentHashMap<>());
        bossExecutorService.execute(() -> execution.advance(0, input));
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
            // Los executors compartidos del JVM sobreviven al engine: apagar uno
            // no puede dejar sin threads a los demás flows.
            return;
        }
        bossExecutorService.shutdown();
        workersExecutorService.shutdown();
        try {
            if (!workersExecutorService.awaitTermination(gracePeriod.toMillis(), TimeUnit.MILLISECONDS)) {
                workersExecutorService.shutdownNow();
            }
            if (!bossExecutorService.awaitTermination(gracePeriod.toMillis(), TimeUnit.MILLISECONDS)) {
                bossExecutorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            workersExecutorService.shutdownNow();
            bossExecutorService.shutdownNow();
        }
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
     * Una ejecución por request: snapshot de la chain, decisiones y resultado propios.
     * La orquestación (advance/recover) corre SIEMPRE en el hilo boss; el código de
     * usuario de Stage/Background/Recovery corre en los workers.
     */
    private final class Execution {

        private final List<Link> links;
        private final Map<String, Object> context;
        private final Map<Integer, Boolean> decisions = new HashMap<>();
        private final CompletableFuture<Object> result = new CompletableFuture<>();

        private Execution(List<Link> links, Map<String, Object> context) {
            this.links = links;
            this.context = context;
        }

        private void advance(int index, Object value) {
            if (index >= links.size()) {
                result.complete(value);
                return;
            }
            Link link = links.get(index);
            if (!passesGuards(link)) {
                advance(index + 1, value);
                return;
            }
            switch (link) {
                case Stage stage -> dispatch(stage, index, value);
                case Decision decision -> {
                    decisions.put(decision.id(), decision.predicate().test(value));
                    advance(index + 1, value);
                }
                case Filter filter -> {
                    if (filter.predicate().test(value)) {
                        advance(index + 1, value);
                    } else {
                        result.complete(null);
                    }
                }
                case Background background -> {
                    workersExecutorService.execute(() -> runBackground(background, value));
                    advance(index + 1, value);
                }
                case Recovery ignored -> advance(index + 1, value);
            }
        }

        private void dispatch(Stage stage, int index, Object value) {
            CompletableFuture<Object> task =
                    CompletableFuture.supplyAsync(() -> stage.function().apply(value), workersExecutorService);
            if (stage.timeout() != null) {
                task = task.orTimeout(stage.timeout().toMillis(), TimeUnit.MILLISECONDS);
            }
            task.whenCompleteAsync((next, error) -> {
                if (error != null) {
                    recover(index + 1, unwrap(error));
                } else {
                    advance(index + 1, next);
                }
            }, bossExecutorService);
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
                            }, bossExecutorService);
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

        private boolean passesGuards(Link link) {
            List<Guard> guards = link.guards();
            if (guards == null || guards.isEmpty()) {
                return true;
            }
            return guards.stream()
                    .allMatch(guard -> Objects.equals(decisions.get(guard.decision()), guard.expected()));
        }
    }
}
