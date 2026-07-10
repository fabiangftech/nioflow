package dev.nioflow.application.facade;

import dev.nioflow.core.facade.*;
import dev.nioflow.core.model.*;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * {@link dev.nioflow.core.facade.NioFlow} implementation backed by an {@link ExecutorService} and two queues,
 * io_uring style: a submission queue with values ready to run their next stage and a
 * completion queue with reaped async results.
 *
 * <p>The chain of stages is declared once; each {@code just} injects a value that walks
 * the chain in order. Several values can be in flight at the same time, and blocking
 * anywhere — {@code handle} or {@code submit} — ties up only that value: by default a
 * boss event loop hands each value to a virtual handle worker, and async stages run on
 * the executor. Reach for {@code submit} when a stage needs a timeout with real
 * cancellation or should run on your own executor; a fixed handle-worker pool
 * ({@link #NioFlow(ExecutorService, int)}) is the knob for bounding CPU-heavy
 * chains, and only there must handles stay fast.
 *
 * <p>{@code when(predicate).then(lane -> ...)} forks the chain: each branch builds its
 * own visually nested sub-chain and a value only runs the stages of its lane. Stages
 * chained after the fork are back on the main line and run for every value.
 *
 * <p>Errors from any stage short-circuit only the failing value and are delivered to
 * {@code onError} handlers without blocking the builder thread. {@code join()} waits
 * until every injected value finished and returns the newest injected value's result;
 * a failure is rethrown by the first {@code join()} after it and then cleared, so the
 * nio-flow stays usable. A nio-flow instance is meant to be built from a single thread.
 *
 * @param <T> the type of the values flowing at this point of the chain; {@code adapt}
 *            and {@code fanOut} hand out a differently-typed view over the same engine
 */
public final class NioFlow<T> implements dev.nioflow.core.facade.NioFlow<T>, AutoCloseable {

    private final dev.nioflow.core.facade.NioEngine nioEngine;
    private final List<Guard> guards;

    /** All stages on virtual threads: blocking anywhere ties up only that value. */
    public NioFlow() {
        this(Backpressure.unbounded());
    }

    /**
     * Like {@link #NioFlow()} with admission control: {@code just} applies the
     * backpressure policy once {@code capacity} values are in flight.
     *
     * @param backpressure the admission policy applied by {@code just} at capacity
     */
    public NioFlow(Backpressure backpressure) {
        this(new NioEngine(Executors.newVirtualThreadPerTaskExecutor(), true,
                VIRTUAL_HANDLE_WORKERS, backpressure), List.of());
    }

    /**
     * Runs async stages on the given executor. The caller keeps ownership of its
     * lifecycle: {@link #close()} will not shut it down.
     *
     * @param executor runs every {@code submit} stage; any shape works — fixed,
     *                 cached, single-threaded or virtual-thread-per-task
     */
    public NioFlow(ExecutorService executor) {
        this(new NioEngine(executor, false, VIRTUAL_HANDLE_WORKERS, Backpressure.unbounded()), List.of());
    }

    /**
     * Like {@link #NioFlow(ExecutorService)} with a fixed handle-worker pool of
     * the given size, bounding sync parallelism — the tuning knob for CPU-heavy
     * chains. Blocking handles then tie up shared workers; the virtual default
     * (other constructors) has no such limit.
     *
     * @param executor      runs every {@code submit} stage; the caller keeps
     *                      ownership of its lifecycle
     * @param handleWorkers size of the fixed pool walking sync stages; keep handles
     *                      fast once bounded
     */
    public NioFlow(ExecutorService executor, int handleWorkers) {
        this(new NioEngine(executor, false, handleWorkers, Backpressure.unbounded()), List.of());
    }

    /**
     * Fully tuned nio-flow: executor for async stages, fixed handle-worker pool size
     * and backpressure admission control.
     *
     * @param executor      runs every {@code submit} stage; the caller keeps
     *                      ownership of its lifecycle
     * @param handleWorkers size of the fixed pool walking sync stages
     * @param backpressure  the admission policy applied by {@code just} at capacity
     */
    public NioFlow(ExecutorService executor, int handleWorkers, Backpressure backpressure) {
        this(new NioEngine(executor, false, handleWorkers, backpressure), List.of());
    }

    /** Sentinel for the engine: one virtual thread per dispatch, no fixed pool. */
    private static final int VIRTUAL_HANDLE_WORKERS = 0;

    /**
     * A view over an already running engine: same chain, same values, but links
     * declared through this view carry {@code guards} — how lanes and {@code adapt}
     * hand out scoped views without copying anything.
     */
    private NioFlow(dev.nioflow.core.facade.NioEngine nioEngine, List<Guard> guards) {
        this.nioEngine = nioEngine;
        this.guards = guards;
    }

    @Override
    public dev.nioflow.core.facade.NioFlow<T> just(T input) {
        nioEngine.inject(input);
        return this;
    }

    @Override
    public dev.nioflow.core.facade.NioFlow<T> just(T input, Map<String, Object> context) {
        nioEngine.inject(input, context);
        return this;
    }

    @Override
    public dev.nioflow.core.facade.NioFlow<T> justAll(Iterable<T> inputs) {
        inputs.forEach(nioEngine::inject);
        return this;
    }

    @Override
    public dev.nioflow.core.facade.NioFlow<T> handle(Function<T, T> function) {
        nioEngine.append(new Stage(null, untyped(function), false, null, guards));
        return this;
    }

    @Override
    public dev.nioflow.core.facade.NioFlow<T> handle(String name, Function<T, T> function) {
        nioEngine.append(new Stage(name, untyped(function), false, null, guards));
        return this;
    }

    @Override
    public dev.nioflow.core.facade.NioFlow<T> handle(Function<T, T> function, Resilience<T> resilience) {
        nioEngine.append(new Stage(null, untyped(resilience.decorate(function)), false, null, guards));
        return this;
    }

    @Override
    public dev.nioflow.core.facade.NioFlow<T> submit(Function<T, T> function) {
        nioEngine.append(new Stage(null, untyped(function), true, null, guards));
        return this;
    }

    @Override
    public dev.nioflow.core.facade.NioFlow<T> submit(String name, Function<T, T> function) {
        nioEngine.append(new Stage(name, untyped(function), true, null, guards));
        return this;
    }

    @Override
    public dev.nioflow.core.facade.NioFlow<T> submit(Function<T, T> function, Duration timeout) {
        nioEngine.append(new Stage(null, untyped(function), true, timeout, guards));
        return this;
    }

    @Override
    public dev.nioflow.core.facade.NioFlow<T> submit(Function<T, T> function, Resilience<T> resilience) {
        nioEngine.append(new Stage(null, untyped(resilience.decorate(function)), true, null, guards));
        return this;
    }

    @Override
    public dev.nioflow.core.facade.NioFlow<T> batch(int size, Duration maxWait, Function<List<T>, List<T>> function) {
        nioEngine.append(new Batch(untypedBatch(function), size, maxWait, guards));
        return this;
    }

    @Override
    public <N> dev.nioflow.core.facade.NioFlow<N> adapt(Function<T, N> function) {
        nioEngine.append(new Stage(null, untyped(function), false, null, guards));
        return new NioFlow<>(nioEngine, guards);
    }

    @Override
    public <N> dev.nioflow.core.facade.NioFlow<N> fanOut(Function<T, List<N>> function) {
        nioEngine.append(new FanOut(untypedFanOut(function), guards));
        return new NioFlow<>(nioEngine, guards);
    }

    @Override
    public dev.nioflow.core.facade.NioFlow<T> filter(Predicate<T> predicate) {
        nioEngine.append(new Filter(untypedPredicate(predicate), guards));
        return this;
    }

    @Override
    public Condition<T> when(Predicate<T> predicate) {
        int decision = decision(predicate);
        return thenLane -> {
            thenLane.apply(lane(decision, true));
            return new ForkBranch<>(this, decision);
        };
    }

    @Override
    public Cases<T> match() {
        return new MatchCases<>(this);
    }

    /**
     * Appends a decision link guarded by this view's lanes and reserves its id —
     * the shared first step of {@code when} and every {@code match} case.
     */
    int decision(Predicate<T> predicate) {
        int decision = nioEngine.nextDecision();
        nioEngine.append(new Decision(untypedPredicate(predicate), decision, guards));
        return decision;
    }

    @Override
    public dev.nioflow.core.facade.NioFlow<T> onError(Consumer<Throwable> handler) {
        nioEngine.addErrorHandler(handler);
        return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public dev.nioflow.core.facade.NioFlow<T> onComplete(Consumer<T> handler) {
        nioEngine.addCompleteHandler(value -> handler.accept((T) value));
        return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public dev.nioflow.core.facade.NioFlow<T> onErrorResume(Function<Throwable, T> fallback) {
        nioEngine.append(new Recovery((Function<Throwable, Object>) fallback, guards));
        return this;
    }

    @Override
    public dev.nioflow.core.facade.NioFlow<T> metrics(NioFlowMetrics metrics) {
        nioEngine.metrics(metrics);
        return this;
    }

    @Override
    public dev.nioflow.core.facade.NioFlow<T> trace(NioFlowTracer tracer) {
        nioEngine.trace(tracer);
        return this;
    }

    @Override
    public Diagnostics diagnostics() {
        return nioEngine.diagnostics();
    }

    @Override
    public String toString() {
        return nioEngine.diagnostics().toString();
    }

    @Override
    public dev.nioflow.core.facade.NioFlow<T> seal() {
        nioEngine.seal();
        return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public T join() {
        return (T) nioEngine.await();
    }

    @Override
    @SuppressWarnings("unchecked")
    public T join(Duration timeout) {
        return (T) nioEngine.await(timeout);
    }

    /**
     * Graceful close: drains in-flight values for up to 10 seconds, then stops the
     * engine. Use {@link #close(Duration)} to control the grace period. Idempotent.
     */
    @Override
    public void close() {
        close(Duration.ofSeconds(10));
    }

    /**
     * Like {@link #close()} with an explicit grace period: in-flight values get up
     * to {@code gracePeriod} to finish before the engine stops its loops and
     * releases its own resources — never an externally supplied executor.
     *
     * @param gracePeriod how long to let in-flight values finish before stopping
     */
    public void close(Duration gracePeriod) {
        nioEngine.shutdown(gracePeriod);
    }

    /**
     * The view for one lane of a fork: everything declared through it carries this
     * view's guards plus one for the given decision outcome, so those links only
     * run for values that took the lane.
     */
    NioFlow<T> lane(int decision, boolean expected) {
        List<Guard> laneGuards = new ArrayList<>(guards);
        laneGuards.add(new Guard(decision, expected));
        return new NioFlow<>(nioEngine, List.copyOf(laneGuards));
    }

    // The engine is untyped on purpose (adapt re-types the view over the same
    // chain), so user functions are erased once at declaration. Safe: each view
    // only accepts functions matching the values flowing at its point of the chain.
    @SuppressWarnings("unchecked")
    private static Function<Object, Object> untyped(Function<?, ?> function) {
        return (Function<Object, Object>) function;
    }

    @SuppressWarnings("unchecked")
    private static Predicate<Object> untypedPredicate(Predicate<?> predicate) {
        return (Predicate<Object>) predicate;
    }

    @SuppressWarnings("unchecked")
    private static Function<List<Object>, List<Object>> untypedBatch(Function<?, ?> function) {
        return (Function<List<Object>, List<Object>>) function;
    }

    @SuppressWarnings("unchecked")
    private static Function<Object, List<Object>> untypedFanOut(Function<?, ?> function) {
        return (Function<Object, List<Object>>) function;
    }
}
