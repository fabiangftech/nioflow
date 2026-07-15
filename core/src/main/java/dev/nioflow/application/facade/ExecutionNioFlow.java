package dev.nioflow.application.facade;

import dev.nioflow.core.facade.Cancellable;
import dev.nioflow.core.facade.Context;
import dev.nioflow.core.facade.FlowResult;
import dev.nioflow.core.facade.NioEngine;
import dev.nioflow.core.facade.NioStep;
import dev.nioflow.core.facade.PreparedChain;
import dev.nioflow.core.facade.Segment;
import dev.nioflow.core.facade.StepCases;
import dev.nioflow.core.facade.StepCondition;
import dev.nioflow.core.model.FlowSignal;
import dev.nioflow.core.model.Guard;
import dev.nioflow.core.model.Link;
import dev.nioflow.core.model.RateLimit;
import dev.nioflow.core.model.Retry;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

/**
 * The per-request pipeline opened by just(): it uses the shared definition as
 * it is (without copying) until the first local link is added, and runs
 * through engine.call(input, context, chain) without sealing or mutating
 * anything shared.
 *
 * <p>T is the value's type right here — it starts at the flow's input type
 * (the shared chain is type-preserving) and adapt/fanOut/batch/use move it. O
 * is the flow's declared output, carried along for documentation. Lane views
 * (withGuards) share the same State, so links declared inside a fork land in
 * the same local chain.
 */
final class ExecutionNioFlow<T, O> extends AbstractChain<T> implements NioStep<T, O> {

    private final State state;
    private final List<Guard> guards;

    ExecutionNioFlow(NioEngine nioEngine, Object seed) {
        this(new State(nioEngine, seed), List.of());
    }

    private ExecutionNioFlow(State state, List<Guard> guards) {
        this.state = state;
        this.guards = guards;
    }

    @Override
    public NioStep<T, O> handle(UnaryOperator<T> function) {
        return handle(anonymousName("stage"), function);
    }

    @Override
    public NioStep<T, O> handle(String name, UnaryOperator<T> function) {
        stage(name, function);
        return this;
    }

    @Override
    public NioStep<T, O> handle(String name, UnaryOperator<T> function, Duration timeout) {
        stage(name, function, timeout);
        return this;
    }

    @Override
    public NioStep<T, O> handle(String name, UnaryOperator<T> function, Retry retry) {
        stage(name, function, retry);
        return this;
    }

    @Override
    public NioStep<T, O> handle(String name, UnaryOperator<T> function, Duration timeout, Retry retry) {
        stage(name, function, timeout, retry);
        return this;
    }

    @Override
    public NioStep<T, O> handle(String name, UnaryOperator<T> function, RateLimit rateLimit) {
        rateLimitedStage(name, function, rateLimit);
        return this;
    }

    @Override
    public NioStep<T, O> handleAsync(String name, Function<T, CompletionStage<T>> call) {
        return handleAsync(name, call, null, null);
    }

    @Override
    public NioStep<T, O> handleAsync(String name, Function<T, CompletionStage<T>> call, Duration timeout) {
        return handleAsync(name, call, timeout, null);
    }

    @Override
    public NioStep<T, O> handleAsync(String name, Function<T, CompletionStage<T>> call, Retry retry) {
        return handleAsync(name, call, null, retry);
    }

    @Override
    public NioStep<T, O> handleAsync(String name, Function<T, CompletionStage<T>> call,
                                     Duration timeout, Retry retry) {
        asyncStage(name, call, timeout, retry);
        return this;
    }

    @Override
    public <R> NioStep<R, O> adaptAsync(Function<T, CompletionStage<R>> call) {
        return adaptAsync(call, null);
    }

    @Override
    public <R> NioStep<R, O> adaptAsync(Function<T, CompletionStage<R>> call, Duration timeout) {
        adaptAsyncValue(call, timeout);
        return retyped();
    }

    @Override
    public NioStep<T, O> handleContextual(BiFunction<T, Context, T> function) {
        return handleContextual(anonymousName("stage"), function);
    }

    @Override
    public NioStep<T, O> handleContextual(String name, BiFunction<T, Context, T> function) {
        contextualStage(name, function);
        return this;
    }

    @Override
    public NioStep<T, O> handleSync(UnaryOperator<T> function) {
        return handleSync(anonymousName("sync"), function);
    }

    @Override
    public NioStep<T, O> handleSync(String name, UnaryOperator<T> function) {
        syncStage(name, function);
        return this;
    }

    @Override
    public NioStep<T, O> background(Consumer<T> effect) {
        return background(anonymousName("background"), effect);
    }

    @Override
    public NioStep<T, O> background(String name, Consumer<T> effect) {
        backgroundEffect(name, effect);
        return this;
    }

    // The four steps that re-type the value: the links go to the same chain,
    // and the builder re-parameterizes itself (the engine moves Objects).
    @Override
    public <R> NioStep<R, O> adapt(Function<T, R> function) {
        adaptValue(function);
        return retyped();
    }

    @Override
    public <R, C> NioStep<C, O> fanOut(List<Function<T, R>> branches, Function<List<R>, C> join) {
        return fanOut(anonymousName("fanout"), branches, join);
    }

    @Override
    public <R, C> NioStep<C, O> fanOut(String name, List<Function<T, R>> branches, Function<List<R>, C> join) {
        fanOutBranches(name, branches, join);
        return retyped();
    }

    @Override
    public <R> NioStep<R, O> batch(int size, Duration window, Function<List<T>, List<R>> bulk) {
        return batch(anonymousName("batch"), size, window, bulk);
    }

    @Override
    public <R> NioStep<R, O> batch(String name, int size, Duration window, Function<List<T>, List<R>> bulk) {
        batchValues(name, size, window, bulk);
        return retyped();
    }

    @Override
    public <R> NioStep<T, O> fork(Segment<T, R> sub) {
        return fork(anonymousName("fork"), sub);
    }

    // Detached: the sub-flow's type never reaches the main line, so T stays.
    @Override
    public <R> NioStep<T, O> fork(String name, Segment<T, R> sub) {
        forkSegment(name, sub);
        return this;
    }

    @Override
    public <R> NioStep<R, O> use(Segment<T, R> segment) {
        embed(segment);
        return retyped();
    }

    @SuppressWarnings("unchecked")
    private <R> NioStep<R, O> retyped() {
        return (NioStep<R, O>) this;
    }

    @Override
    public NioStep<T, O> filter(Predicate<T> predicate) {
        filterValues(predicate);
        return this;
    }

    @Override
    public NioStep<T, O> recover(Function<Throwable, T> function) {
        return recover(anonymousName("recovery"), function);
    }

    @Override
    public NioStep<T, O> recover(String name, Function<Throwable, T> function) {
        recovery(name, function);
        return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public NioStep<T, O> onComplete(Consumer<T> callback) {
        Consumer<Object> untyped = value -> callback.accept((T) value);
        state.onComplete = state.onComplete == null ? untyped : state.onComplete.andThen(untyped);
        return this;
    }

    @Override
    public NioStep<T, O> onError(Consumer<Throwable> callback) {
        state.onError = state.onError == null ? callback : state.onError.andThen(callback);
        return this;
    }

    @Override
    public NioStep<T, O> key(Object key) {
        state.key = key;
        return this;
    }

    @Override
    public <V> NioStep<T, O> with(Context.Key<V> key, V value) {
        state.putContext(key.name(), value);
        return this;
    }

    @Override
    public StepCondition<T, O> when(Predicate<T> predicate) {
        return new DefaultStepCondition<>(this, appendDecision(predicate));
    }

    @Override
    public StepCases<T, O> match() {
        return new DefaultStepCases<>(this);
    }

    @Override
    public T execute() {
        return Requests.typed(rawFuture().join());
    }

    @Override
    public CompletableFuture<T> executeAsync() {
        return rawFuture().thenApply(Requests::typed);
    }

    @Override
    public CompletableFuture<T> executeAsync(Map<String, Object> context) {
        return rawFuture(context).thenApply(Requests::typed);
    }

    @Override
    @SuppressWarnings("unchecked")
    public FlowResult<T> executeResult() {
        Object value = rawFuture().join();
        if (value == FlowSignal.FILTERED) {
            return new FlowResult.Filtered<>();
        }
        if (value == FlowSignal.CANCELLED) {
            return new FlowResult.Cancelled<>();
        }
        return new FlowResult.Completed<>((T) value);
    }

    @Override
    public Cancellable<T> executeCancellable() {
        return executeCancellable(null);
    }

    @Override
    public Cancellable<T> executeCancellable(Map<String, Object> runContext) {
        Map<String, Object> context = Requests.mergeContext(state.context, runContext);
        Cancellable<Object> raw = state.links == null
                ? state.nioEngine.callCancellable(state.seed, context, state.nioEngine.chain(), state.key)
                : state.nioEngine.callCancellable(state.seed, context, state.prepared(), state.key);
        return Requests.cancellable(raw, state.onComplete, state.onError);
    }

    private CompletableFuture<Object> rawFuture() {
        return rawFuture(null);
    }

    /**
     * The raw result future, decorated with any execution-scoped callbacks.
     * When local links were added this dispatches off a cached snapshot plan
     * (built and compiled once, reused by a re-subscribing Mono); with none, it
     * hands the engine its own live chain, which matches the compiled plan by
     * identity when the shared definition is sealed.
     */
    private CompletableFuture<Object> rawFuture(Map<String, Object> runContext) {
        Map<String, Object> context = Requests.mergeContext(state.context, runContext);
        CompletableFuture<Object> raw = state.links == null
                ? state.nioEngine.call(state.seed, context, state.nioEngine.chain(), state.key)
                : state.nioEngine.call(state.seed, context, state.prepared(), state.key);
        return Requests.decorate(raw, state.onComplete, state.onError);
    }

    @Override
    NioEngine engine() {
        return state.nioEngine;
    }

    @Override
    void appendLink(Link link) {
        state.localLinks().add(link);
    }

    /** Local links: this pipeline is one request's, never the engine's chain. */
    @Override
    boolean buildsSharedChain() {
        return false;
    }

    @Override
    List<Guard> guards() {
        return guards;
    }

    @Override
    AbstractChain<T> withGuards(List<Guard> guards) {
        return new ExecutionNioFlow<>(state, guards);
    }

    @Override
    String anonymousName(String prefix) {
        return prefix + "-" + state.anonymousLinks++;
    }

    /**
     * State of the execution, shared by the flow and its lane views. Built on
     * the request thread: it needs no synchronization.
     */
    private static final class State {

        private final NioEngine nioEngine;
        // null = the shared definition as it is; copied only when local links appear
        private List<Link> links;
        // Compiled snapshot of the local chain, built once on the first
        // terminal call and reused by every later run (a re-subscribing Mono, a
        // retry): the double copy and the per-dispatch fusion rescan are paid
        // once, not per request. null until the first terminal with local links.
        private PreparedChain prepared;
        private final Object seed;
        // Context seeded by with(): null until the first one, so a pipeline
        // that never seeds anything allocates nothing for it.
        private Map<String, Object> context;
        private int anonymousLinks;
        // Callbacks scoped to THIS execution (null = none), composed with andThen.
        private Consumer<Object> onComplete;
        private Consumer<Throwable> onError;
        // Ordering key (null = unordered): same key -> same boss, FIFO.
        private Object key;

        private State(NioEngine nioEngine, Object seed) {
            this.nioEngine = nioEngine;
            this.seed = seed;
        }

        private List<Link> localLinks() {
            if (links == null) {
                links = new ArrayList<>(nioEngine.chain());
            }
            return links;
        }

        /** The compiled snapshot of the local chain, built once and cached. */
        private PreparedChain prepared() {
            if (prepared == null) {
                prepared = nioEngine.planFor(List.copyOf(links));
            }
            return prepared;
        }

        private void putContext(String name, Object value) {
            if (context == null) {
                context = new HashMap<>();
            }
            context.put(name, value);
        }
    }
}
