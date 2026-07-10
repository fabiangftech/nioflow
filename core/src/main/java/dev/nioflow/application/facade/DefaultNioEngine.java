package dev.nioflow.application.facade;

import dev.nioflow.core.facade.NioFlowMetrics;
import dev.nioflow.core.facade.NioFlowTracer;
import dev.nioflow.core.model.Backpressure;
import dev.nioflow.core.model.*;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * {@link dev.nioflow.core.facade.NioEngine} implementation in io_uring style, with multiple values in flight:
 * every injected {@link FlowValue} walks the shared chain with its own cursor.
 * A single boss thread drains the submission queue and hands each value to a handle
 * worker — a virtual thread per dispatch by default, so a blocking handle ties up
 * only its own value; an optional fixed pool bounds sync parallelism instead. Async
 * stages (submit) are launched on the supplied executor <em>without waiting</em>, so
 * a value blocked on slow IO (JDBC, HTTP) never delays the values behind it. Async
 * results are reaped from the completion queue and the value re-enters the
 * submission queue for its next stage.
 *
 * <p>A value that reaches the end of the declared chain parks; appending a new link
 * resumes every parked value. {@code when} forks record the predicate outcome on the
 * value, and the value skips links whose guards don't match its recorded decisions.
 *
 * <p>The chain is versioned copy-on-write for structural edits: every value captures
 * the current version at injection and walks it to the end, so {@link #splice} never
 * disturbs a value in flight — it starts a new version that only values injected
 * afterwards walk. Appends mutate the current version in place, keeping the original
 * semantics: values still flowing on it, parked ones included, see the new link.
 *
 * <p>The two loops run on dedicated daemon threads and never borrow executor threads,
 * so any executor shape works: fixed, cached, single-threaded or virtual-thread-per-task.
 */
public final class DefaultNioEngine implements dev.nioflow.core.facade.NioEngine {

    private static final long POLL_MILLIS = 100;

    private final ExecutorService executor;
    private final boolean ownsExecutor;
    private final Backpressure backpressure;
    private final ExecutorService handleWorkers;
    private final Thread boss;
    private final Thread completer;
    private final BlockingQueue<FlowValue> submissionQueue = new LinkedBlockingQueue<>();
    private final BlockingQueue<Completion> completionQueue = new LinkedBlockingQueue<>();

    private final AtomicInteger decisionIds = new AtomicInteger();

    private final Object lock = new Object();
    /** The current chain version; {@link #splice} replaces it, {@link #append} grows it. */
    private List<Link> chain = new ArrayList<>();
    private final List<FlowValue> parked = new ArrayList<>();
    private final Map<Batch, BatchBuffer> batches = new HashMap<>();
    /** REPLACE-spliced segments by anchor name, so re-editing swaps whole regions. */
    private final Map<String, Region> regions = new HashMap<>();
    private long injected;
    private long lastResultSequence = -1;
    private Object lastResult;
    private Throwable lastFailure;
    private int active;
    private boolean closed;
    private boolean sealed;
    private boolean released;

    /** Replay history for late onError handlers — bounded, or a failure-prone
     *  long-running nio-flow would retain every throwable it ever saw. */
    private static final int FAILURE_HISTORY = 128;

    private final Object handlerLock = new Object();
    private final List<Consumer<Throwable>> errorHandlers = new ArrayList<>();
    private final ArrayDeque<Throwable> deliveredFailures = new ArrayDeque<>();
    private final List<Consumer<Object>> completeHandlers = new CopyOnWriteArrayList<>();

    private volatile boolean running = true;
    private volatile NioFlowMetrics metrics;
    private volatile NioFlowTracer tracer;

    /**
     * Builds the engine and starts its boss and completer loops immediately.
     *
     * @param executor      runs async (submit) stages and batch calls; any shape
     *                      works — the engine's loops never borrow its threads
     * @param ownsExecutor  when true the executor is shut down on {@link #shutdown};
     *                      externally supplied executors stay under the caller's control
     * @param handleWorkers size of the engine-owned fixed pool that walks sync stages,
     *                      or {@code <= 0} for one virtual thread per dispatch — the
     *                      default, where a blocking handle ties up only its own value
     * @param backpressure  admission control applied by {@link #inject(Object)}
     */
    public DefaultNioEngine(ExecutorService executor, boolean ownsExecutor, int handleWorkers,
                            Backpressure backpressure) {
        this.executor = executor;
        this.ownsExecutor = ownsExecutor;
        this.backpressure = backpressure;
        this.handleWorkers = workerPool(handleWorkers);
        this.boss = startLoop("nio-flow-boss", this::dispatchLoop);
        this.completer = startLoop("nio-flow-completer", this::completionLoop);
    }

    /** Starts an engine loop on its own named daemon thread. */
    private static Thread startLoop(String name, Runnable loop) {
        Thread thread = new Thread(loop, name);
        thread.setDaemon(true);
        thread.start();
        return thread;
    }

    /** The handle workers: a fixed daemon pool, or virtual-per-dispatch for {@code <= 0}. */
    private static ExecutorService workerPool(int workers) {
        if (workers <= 0) {
            return Executors.newThreadPerTaskExecutor(
                    Thread.ofVirtual().name("nio-flow-worker-", 0).factory());
        }
        AtomicInteger index = new AtomicInteger();
        return Executors.newFixedThreadPool(workers, task -> {
            Thread thread = new Thread(task, "nio-flow-worker-" + index.getAndIncrement());
            thread.setDaemon(true);
            return thread;
        });
    }

    @Override
    public void inject(Object input) {
        inject(input, Map.of());
    }

    @Override
    public void inject(Object input, Map<String, Object> context) {
        enqueue(input, context, null, null);
    }

    @Override
    public CompletableFuture<Object> call(Object input, Map<String, Object> context) {
        return call(input, context, null);
    }

    @Override
    public CompletableFuture<Object> call(Object input, Map<String, Object> context, List<Link> chain) {
        CompletableFuture<Object> reply = new CompletableFuture<>();
        try {
            enqueue(input, context, reply, chain);
        } catch (RuntimeException rejected) {
            // a caller holding a future expects its outcome there, not a throw
            reply.completeExceptionally(rejected);
        }
        return reply;
    }

    @Override
    public List<Link> chain() {
        synchronized (lock) {
            return List.copyOf(chain);
        }
    }

    /**
     * Shared admission path: a {@code call} attaches the caller's reply future, a
     * scoped call its private chain — null walks the shared one.
     */
    private void enqueue(Object input, Map<String, Object> context, CompletableFuture<Object> reply,
                         List<Link> version) {
        FlowValue flow;
        synchronized (lock) {
            rejectWhenClosed();
            while (active >= backpressure.capacity()) {
                switch (backpressure.policy()) {
                    case DROP -> {
                        if (reply != null) {
                            reply.cancel(false); // dropped on admission: nobody will reply
                        }
                        return;
                    }
                    case FAIL -> throw new RejectedExecutionException(
                            "nio-flow at capacity: " + backpressure.capacity() + " values in flight");
                    case BLOCK -> {
                        try {
                            lock.wait();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new CompletionException(e);
                        }
                        rejectWhenClosed(); // shutdown wakes blocked producers
                    }
                }
            }
            flow = new FlowValue(input, injected++);
            flow.context().putAll(context);
            flow.chain(version != null ? version : chain);
            flow.reply(reply);
            active++;
        }
        NioFlowMetrics sink = metrics;
        if (sink != null) {
            sink.injected();
        }
        NioFlowTracer trace = tracer;
        if (trace != null) {
            trace.injected(flow.sequence(), input);
        }
        submissionQueue.offer(flow);
    }

    @Override
    public void metrics(NioFlowMetrics metrics) {
        this.metrics = metrics;
    }

    @Override
    public void trace(NioFlowTracer tracer) {
        this.tracer = tracer;
    }

    @Override
    public Diagnostics diagnostics() {
        synchronized (lock) {
            return new Diagnostics(
                    chain.stream().map(DefaultNioEngine::describe).toList(),
                    submissionQueue.size(),
                    completionQueue.size(),
                    active,
                    parked.size(),
                    batches.values().stream().mapToInt(BatchBuffer::size).sum(),
                    injected,
                    sealed,
                    closed);
        }
    }

    /** One readable line per link, e.g. {@code submit[save] if{0=true}}. */
    private static String describe(Link link) {
        String shape = switch (link) {
            case Stage stage -> (stage.async() ? "submit" : "handle")
                    + (stage.name() == null ? "" : "[" + stage.name() + "]")
                    + (stage.timeout() == null ? "" : " timeout=" + stage.timeout());
            case Decision decision -> "when#" + decision.id();
            case Recovery ignored -> "onErrorResume";
            case Filter ignored -> "filter";
            case FanOut ignored -> "fanOut";
            case Batch batch -> "batch[size=" + batch.size() + ", maxWait=" + batch.maxWait() + "]";
        };
        if (link.guards().isEmpty()) {
            return shape;
        }
        StringBuilder guarded = new StringBuilder(shape).append(" if{");
        for (int i = 0; i < link.guards().size(); i++) {
            Guard guard = link.guards().get(i);
            if (i > 0) {
                guarded.append(", ");
            }
            guarded.append(guard.decision()).append('=').append(guard.expected());
        }
        return guarded.append('}').toString();
    }

    /** Must be called with the lock held. */
    private void rejectWhenClosed() {
        if (closed) {
            throw new RejectedExecutionException("nio-flow is closed");
        }
    }

    @Override
    public void append(Link link) {
        synchronized (lock) {
            if (sealed) {
                throw new IllegalStateException("nio-flow is sealed: the chain can no longer change");
            }
            chain.add(link);
            for (FlowValue flow : parked) {
                active++;
                submissionQueue.offer(flow);
            }
            parked.clear();
        }
    }

    @Override
    public void seal() {
        synchronized (lock) {
            sealed = true;
        }
    }

    @Override
    public void release() {
        synchronized (lock) {
            released = true;
        }
    }

    @Override
    public void splice(String anchor, Splice position, List<Link> links) {
        synchronized (lock) {
            if (sealed) {
                throw new IllegalStateException("nio-flow is sealed: the chain can no longer change");
            }
            List<Link> next = new ArrayList<>(chain);
            List<Guard> laneGuards;
            int index;
            Region region = regions.get(anchor);
            if (region != null) {
                laneGuards = region.laneGuards();
                index = spliceRegion(next, region, position);
            } else {
                index = indexOfStage(next, anchor);
                if (index >= 0) {
                    laneGuards = next.get(index).guards();
                    if (position == Splice.REPLACE) {
                        next.remove(index);
                    } else if (position == Splice.AFTER) {
                        index++;
                    }
                } else {
                    laneGuards = null;
                }
            }
            if (index < 0) {
                throw new IllegalArgumentException("no stage or region named '" + anchor + "' in the chain");
            }
            List<Link> guarded = links.stream().map(link -> inLane(link, laneGuards)).toList();
            next.addAll(index, guarded);
            if (position == Splice.REPLACE) {
                // remember the spliced links as the anchor's region, so the next
                // REPLACE with the same name swaps the whole segment, not one link
                if (guarded.isEmpty()) {
                    regions.remove(anchor);
                } else {
                    regions.put(anchor, new Region(laneGuards, guarded));
                }
            }
            chain = next;
            // values parked at the end of the previous version can never resume —
            // appends only grow the new version — so stop retaining them; their
            // completion was already delivered when they parked
            parked.clear();
        }
    }

    /** A REPLACE-spliced segment, remembered so later edits target the whole of it. */
    private record Region(List<Guard> laneGuards, List<Link> links) {
    }

    /**
     * Locates the edit point of a region in the next version — and, for REPLACE,
     * takes the region's links out. Region links are matched by identity: value
     * equality would confuse two segments declaring equal records.
     *
     * @return the insertion index, or -1 when none of the region's links remain
     */
    private static int spliceRegion(List<Link> next, Region region, Splice position) {
        int first = -1;
        int last = -1;
        for (Link link : region.links()) {
            for (int i = 0; i < next.size(); i++) {
                if (next.get(i) == link) {
                    if (first < 0 || i < first) {
                        first = i;
                    }
                    if (i > last) {
                        last = i;
                    }
                    break;
                }
            }
        }
        if (first < 0) {
            return -1;
        }
        return switch (position) {
            case BEFORE -> first;
            case AFTER -> last + 1;
            case REPLACE -> {
                for (Link link : region.links()) {
                    next.removeIf(candidate -> candidate == link);
                }
                yield first;
            }
        };
    }

    /** The index of the first stage so named, or -1. */
    private static int indexOfStage(List<Link> links, String anchor) {
        for (int i = 0; i < links.size(); i++) {
            if (links.get(i) instanceof Stage stage && anchor.equals(stage.name())) {
                return i;
            }
        }
        return -1;
    }

    /**
     * A copy of the link carrying the anchor's guards on top of its own, so a splice
     * next to a laned anchor stays inside that lane — including the segment's own
     * internal forks.
     */
    private static Link inLane(Link link, List<Guard> laneGuards) {
        if (laneGuards.isEmpty()) {
            return link;
        }
        List<Guard> merged = new ArrayList<>(laneGuards);
        merged.addAll(link.guards());
        List<Guard> guards = List.copyOf(merged);
        return switch (link) {
            case Stage stage -> new Stage(stage.name(), stage.function(), stage.async(),
                    stage.timeout(), guards);
            case Decision decision -> new Decision(decision.predicate(), decision.id(), guards);
            case Recovery recovery -> new Recovery(recovery.function(), guards);
            case Filter filter -> new Filter(filter.predicate(), guards);
            case FanOut fanOut -> new FanOut(fanOut.function(), guards);
            case Batch batch -> new Batch(batch.function(), batch.size(), batch.maxWait(), guards);
        };
    }

    @Override
    public int nextDecision() {
        return decisionIds.getAndIncrement();
    }

    @Override
    public void addErrorHandler(Consumer<Throwable> handler) {
        List<Throwable> replay;
        synchronized (handlerLock) {
            errorHandlers.add(handler);
            replay = List.copyOf(deliveredFailures);
        }
        replay.forEach(failure -> deliver(handler, failure));
    }

    @Override
    public void addCompleteHandler(Consumer<Object> handler) {
        completeHandlers.add(handler);
    }

    @Override
    public Object await() {
        synchronized (lock) {
            while (active > 0) {
                failWhenClosed();
                try {
                    lock.wait();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new CompletionException(e);
                }
            }
            return quiescentResult();
        }
    }

    @Override
    public Object await(Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        synchronized (lock) {
            while (active > 0) {
                failWhenClosed();
                long remainingMillis = remainingMillis(deadline);
                if (remainingMillis <= 0) {
                    throw new CompletionException(
                            new TimeoutException("nio-flow still busy after " + timeout));
                }
                try {
                    lock.wait(remainingMillis);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new CompletionException(e);
                }
            }
            return quiescentResult();
        }
    }

    /** Must be called with the lock held: a closed nio-flow can never quiesce again. */
    private void failWhenClosed() {
        if (closed) {
            throw new CompletionException(
                    new IllegalStateException("nio-flow was closed with values still in flight"));
        }
    }

    /** Must be called with the lock held and no active values left. */
    private Object quiescentResult() {
        if (lastFailure != null) {
            // a failure is thrown once and cleared, so the nio-flow recovers
            Throwable failure = lastFailure;
            lastFailure = null;
            throw new CompletionException(failure);
        }
        return lastResult;
    }

    @Override
    public void shutdown(Duration gracePeriod) {
        synchronized (lock) {
            if (closed) {
                return;
            }
            closed = true;
            lock.notifyAll(); // wake blocked producers and joiners so they fail fast
        }
        drain(gracePeriod);
        running = false;
        boss.interrupt();
        completer.interrupt();
        handleWorkers.shutdownNow();
        if (ownsExecutor) {
            executor.shutdownNow();
        }
    }

    /** Waits until every in-flight value finished, giving up after the grace period. */
    private void drain(Duration gracePeriod) {
        long deadline = System.nanoTime() + gracePeriod.toNanos();
        synchronized (lock) {
            while (active > 0) {
                long remainingMillis = remainingMillis(deadline);
                if (remainingMillis <= 0) {
                    return;
                }
                try {
                    lock.wait(remainingMillis);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
    }

    /** Rounded up, so a timed wait never gives up before its full deadline. */
    private static long remainingMillis(long deadline) {
        return (deadline - System.nanoTime() + 999_999) / 1_000_000;
    }

    /** Boss event loop: never runs user code, only hands values to the workers. */
    private void dispatchLoop() {
        while (running) {
            FlowValue flow = poll(submissionQueue);
            flushExpiredBatches(); // ~POLL_MILLIS granularity on top of each maxWait
            if (flow == null) {
                continue;
            }
            try {
                handleWorkers.execute(() -> advance(flow));
            } catch (RejectedExecutionException e) {
                return; // the worker pool was shut down
            }
        }
    }

    /** Flushes every batch whose oldest value waited past the batch's maxWait. */
    private void flushExpiredBatches() {
        List<Batch> expiredBatches = null;
        List<List<FlowValue>> expiredGroups = null;
        long now = System.nanoTime();
        synchronized (lock) {
            for (Map.Entry<Batch, BatchBuffer> entry : batches.entrySet()) {
                if (entry.getValue().expired(now)) {
                    if (expiredBatches == null) {
                        expiredBatches = new ArrayList<>();
                        expiredGroups = new ArrayList<>();
                    }
                    expiredBatches.add(entry.getKey());
                    expiredGroups.add(entry.getValue().drain());
                }
            }
        }
        if (expiredBatches != null) {
            for (int i = 0; i < expiredBatches.size(); i++) {
                launchBatch(expiredBatches.get(i), expiredGroups.get(i));
            }
        }
    }

    /** Walks a value through sync links until it hits an async stage, parks or fails. */
    private void advance(FlowValue flow) {
        List<Link> version = flow.chain();
        while (true) {
            Link link;
            synchronized (lock) {
                if (flow.cursor() >= version.size()) {
                    // sealed or releasing chains and superseded versions never
                    // resume finished values: release them instead of parking
                    if (!sealed && !released && version == chain) {
                        parked.add(flow);
                    }
                    if (flow.sequence() >= lastResultSequence) {
                        lastResult = flow.value();
                        lastResultSequence = flow.sequence();
                    }
                    break;
                }
                link = version.get(flow.cursor());
            }
            if (!flow.satisfies(link.guards())) {
                flow.advance();
                continue;
            }
            try {
                switch (link) {
                    case Decision decision -> {
                        boolean outcome = FlowContext.bound(flow.context(),
                                () -> decision.predicate().test(flow.value()));
                        flow.decide(decision.id(), outcome);
                        NioFlowTracer trace = tracer;
                        if (trace != null) {
                            trace.lane(flow.sequence(), decision.id(), outcome);
                        }
                        flow.advance();
                    }
                    case Recovery ignored -> flow.advance(); // only failing values enter it
                    case Batch batch -> {
                        List<FlowValue> group = null;
                        synchronized (lock) {
                            BatchBuffer buffer = batches.computeIfAbsent(batch, key -> new BatchBuffer());
                            buffer.add(flow, System.nanoTime() + batch.maxWait().toNanos());
                            if (buffer.isFull(batch.size())) {
                                group = buffer.drain();
                            }
                        }
                        if (group != null) {
                            launchBatch(batch, group);
                        }
                        return; // the value now waits in the buffer or flies with its group
                    }
                    case Filter filter -> {
                        if (!FlowContext.bound(flow.context(),
                                () -> filter.predicate().test(flow.value()))) {
                            discard(flow);
                            return;
                        }
                        flow.advance();
                    }
                    case FanOut fanOut -> {
                        List<Object> children = FlowContext.bound(flow.context(),
                                () -> fanOut.function().apply(flow.value()));
                        if (children == null) {
                            throw new IllegalStateException("fanOut function must not return null");
                        }
                        split(flow, children);
                        return;
                    }
                    case Stage stage when stage.async() -> {
                        launch(flow, stage);
                        return;
                    }
                    case Stage stage -> {
                        long start = System.nanoTime();
                        Object in = flow.value();
                        try {
                            flow.value(FlowContext.bound(flow.context(),
                                    () -> stage.function().apply(in)));
                        } catch (Throwable error) {
                            meterStage(stage, false, start, false);
                            traceStage(flow, stage, false, in, null, error);
                            throw error;
                        }
                        meterStage(stage, false, start, true);
                        traceStage(flow, stage, false, in, flow.value(), null);
                        flow.advance();
                    }
                }
            } catch (Throwable error) {
                completionQueue.offer(new Completion(flow, null, describe(link, error)));
                return;
            }
        }
        // parked: deliver the completion first, then release the value, so join()
        // cannot return while a complete handler is still running
        Object result = flow.value();
        if (flow.reply() != null) {
            flow.reply().complete(result); // the caller first, then the observers
        }
        FlowContext.bound(flow.context(), () -> {
            completeHandlers.forEach(handler -> deliver(handler, result));
            return null;
        });
        NioFlowMetrics sink = metrics;
        if (sink != null) {
            sink.completed();
        }
        NioFlowTracer trace = tracer;
        if (trace != null) {
            trace.completed(flow.sequence(), result);
        }
        synchronized (lock) {
            active--;
            lock.notifyAll();
        }
    }

    /** Reports a stage execution to the metrics sink, if one is registered. */
    private void meterStage(Stage stage, boolean async, long startNanos, boolean success) {
        NioFlowMetrics sink = metrics;
        if (sink != null) {
            sink.stage(stage.name(), async, System.nanoTime() - startNanos, success);
        }
    }

    /** Reports a stage transition to the tracer, if one is registered. */
    private void traceStage(FlowValue flow, Stage stage, boolean async, Object in, Object out,
                            Throwable error) {
        NioFlowTracer trace = tracer;
        if (trace != null) {
            trace.stage(flow.sequence(), stage.name(), async, in, out, error);
        }
    }

    /**
     * Replaces the parent with one independent value per child. Children are internal
     * re-offers, so they bypass backpressure admission on purpose.
     */
    private void split(FlowValue parent, List<Object> children) {
        NioFlowMetrics sink = metrics;
        if (sink != null) {
            sink.fannedOut(children.size());
        }
        NioFlowTracer trace = tracer;
        if (trace != null) {
            trace.fannedOut(parent.sequence(), children.size());
        }
        List<FlowValue> flows = new ArrayList<>(children.size());
        synchronized (lock) {
            active += children.size() - 1; // the parent is consumed
            for (Object value : children) {
                flows.add(parent.child(value));
            }
            if (children.isEmpty()) {
                lock.notifyAll(); // the parent vanished; a joiner may be waiting on it
            }
        }
        if (children.isEmpty() && parent.reply() != null) {
            parent.reply().cancel(false); // no child left to reply for the parent
        }
        flows.forEach(submissionQueue::offer);
    }

    /** A filtered value leaves the nio-flow: no handlers, it just stops counting. */
    private void discard(FlowValue flow) {
        if (flow.reply() != null) {
            flow.reply().cancel(false); // a caller must not wait on a value that left
        }
        NioFlowMetrics sink = metrics;
        if (sink != null) {
            sink.dropped();
        }
        NioFlowTracer trace = tracer;
        if (trace != null) {
            trace.dropped(flow.sequence(), flow.value());
        }
        synchronized (lock) {
            active--;
            lock.notifyAll();
        }
    }

    /** Fires the group's single async call; results are reaped one per value. */
    private void launchBatch(Batch batch, List<FlowValue> group) {
        List<Object> inputs = group.stream().map(FlowValue::value).toList();
        try {
            CompletableFuture<List<Object>> result = new CompletableFuture<>();
            executor.submit(() -> {
                try {
                    result.complete(batch.function().apply(inputs));
                } catch (Throwable error) {
                    result.completeExceptionally(error);
                }
            });
            result.whenComplete((results, error) -> completeBatch(group, results, unwrap(error)));
        } catch (Throwable error) {
            // e.g. RejectedExecutionException when the executor was shut down externally
            completeBatch(group, null, error);
        }
    }

    /** Routes one completion per grouped value, so errors stay individually recoverable. */
    private void completeBatch(List<FlowValue> group, List<Object> results, Throwable error) {
        if (error != null) {
            group.forEach(flow -> completionQueue.offer(new Completion(flow, null, error)));
            return;
        }
        if (results == null || results.size() != group.size()) {
            IllegalStateException mismatch = new IllegalStateException(
                    "batch function must return one result per input: got "
                            + (results == null ? "null" : results.size()) + " for " + group.size());
            group.forEach(flow -> completionQueue.offer(new Completion(flow, null, mismatch)));
            return;
        }
        for (int i = 0; i < group.size(); i++) {
            completionQueue.offer(new Completion(group.get(i), results.get(i), null));
        }
    }

    /** Fires an async stage on the executor and moves on; the result is reaped later. */
    private void launch(FlowValue flow, Stage stage) {
        Object input = flow.value();
        try {
            CompletableFuture<Object> result = new CompletableFuture<>();
            Future<?> worker = executor.submit(() -> {
                long start = System.nanoTime();
                try {
                    Object output = FlowContext.bound(flow.context(),
                            () -> stage.function().apply(input));
                    meterStage(stage, true, start, true);
                    traceStage(flow, stage, true, input, output, null);
                    result.complete(output);
                } catch (Throwable error) {
                    meterStage(stage, true, start, false);
                    traceStage(flow, stage, true, input, null, error);
                    result.completeExceptionally(error);
                }
            });
            CompletableFuture<Object> bounded = stage.timeout() == null
                    ? result
                    : result.orTimeout(stage.timeout().toMillis(), TimeUnit.MILLISECONDS);
            bounded.whenComplete((value, error) -> {
                if (error instanceof TimeoutException) {
                    worker.cancel(true); // interrupt the worker stuck in slow IO
                }
                completionQueue.offer(new Completion(flow, value, describe(stage, unwrap(error))));
            });
        } catch (Throwable error) {
            // e.g. RejectedExecutionException when the executor was shut down externally
            completionQueue.offer(new Completion(flow, null, error));
        }
    }

    /** Strips the {@code CompletionException} shell futures add around stage errors. */
    private static Throwable unwrap(Throwable error) {
        return error instanceof CompletionException && error.getCause() != null
                ? error.getCause()
                : error;
    }

    /** Failures of a named stage arrive wrapped so they say where they happened. */
    private static Throwable describe(Link link, Throwable error) {
        if (error != null && link instanceof Stage stage && stage.name() != null) {
            return new StageException(stage.name(), error);
        }
        return error;
    }

    /**
     * Completer loop: reaps async results, resuming successful values on the
     * submission queue and routing failed ones to recovery.
     */
    private void completionLoop() {
        while (running) {
            Completion completion = poll(completionQueue);
            if (completion == null) {
                continue;
            }
            if (completion.error() != null) {
                recoverOrFail(completion.flow(), completion.error());
            } else {
                FlowValue flow = completion.flow();
                flow.value(completion.result());
                flow.advance();
                submissionQueue.offer(flow);
            }
        }
    }

    /**
     * Hands the error to the first downstream recovery the value satisfies; the
     * fallback's result resumes the flow right after it. With no recovery left the
     * value fails for good. A throwing fallback sends its error to the next one.
     */
    private void recoverOrFail(FlowValue flow, Throwable error) {
        Throwable current = error;
        while (true) {
            Recovery recovery = null;
            List<Link> version = flow.chain();
            synchronized (lock) {
                for (int i = flow.cursor() + 1; i < version.size(); i++) {
                    if (version.get(i) instanceof Recovery candidate
                            && flow.satisfies(candidate.guards())) {
                        recovery = candidate;
                        flow.cursor(i);
                        break;
                    }
                }
            }
            if (recovery == null) {
                fail(flow, current);
                return;
            }
            try {
                Throwable failure = current;
                Recovery fallback = recovery;
                flow.value(FlowContext.bound(flow.context(),
                        () -> fallback.function().apply(failure)));
                NioFlowTracer trace = tracer;
                if (trace != null) {
                    trace.recovered(flow.sequence(), failure, flow.value());
                }
                flow.advance();
                submissionQueue.offer(flow);
                return;
            } catch (Throwable next) {
                current = next;
            }
        }
    }

    /**
     * Terminal failure of one value: recorded for the next {@code await}, added to
     * the bounded replay history and delivered to every error handler — without
     * ever taking down the engine loops.
     */
    private void fail(FlowValue flow, Throwable error) {
        if (flow.reply() != null) {
            flow.reply().completeExceptionally(error);
        }
        NioFlowMetrics sink = metrics;
        if (sink != null) {
            sink.failed(error);
        }
        NioFlowTracer trace = tracer;
        if (trace != null) {
            trace.failed(flow.sequence(), error);
        }
        // record the failure before releasing the value, so a join() that wakes on
        // this failure already sees it in the replay history
        List<Consumer<Throwable>> snapshot;
        synchronized (handlerLock) {
            deliveredFailures.addLast(error);
            if (deliveredFailures.size() > FAILURE_HISTORY) {
                deliveredFailures.removeFirst();
            }
            snapshot = List.copyOf(errorHandlers);
        }
        synchronized (lock) {
            lastFailure = error;
            active--;
            lock.notifyAll();
        }
        // like onComplete: handlers run with the failing value's context bound, so
        // they can tell which value failed (replays run unbound — no value anymore)
        FlowContext.bound(flow.context(), () -> {
            snapshot.forEach(handler -> deliver(handler, error));
            return null;
        });
    }

    /** Runs a user handler, swallowing anything it throws. */
    private static <V> void deliver(Consumer<V> handler, V payload) {
        try {
            handler.accept(payload);
        } catch (Throwable ignored) {
            // a handler must never take down the engine loops
        }
    }

    /**
     * Bounded poll shared by both loops: returns null on timeout or interrupt, so
     * the loop re-checks {@code running} (and batch deadlines) at least every
     * {@code POLL_MILLIS}.
     */
    private <E> E poll(BlockingQueue<E> queue) {
        try {
            return queue.poll(POLL_MILLIS, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        }
    }
}
