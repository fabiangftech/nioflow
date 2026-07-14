package dev.nioflow.core.facade;

import dev.nioflow.core.model.RateLimit;
import dev.nioflow.core.model.Retry;

import java.time.Duration;
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
 * The per-request builder that {@link NioFlow#just(Object)} hands you.
 *
 * <p>{@code T} is the value's type <b>right here</b> — it starts at the flow's
 * input type and moves only when you re-type the value; {@code O} is the
 * output the flow declares, carried along so the pipeline documents itself.
 * The terminal operations return {@code T}, so the compiler tells you when the
 * pipeline has not reached the shape your method promised:
 *
 * <pre>
 * String credit(int cents) {
 *     return credits.just(cents)               // NioStep&lt;Integer, String&gt;
 *             .handle("charge", item -&gt; item)  // Integer in, Integer out
 *             .adapt(item -&gt; "EUR " + item)    // NioStep&lt;String, String&gt;
 *             .execute();                      // String
 * }
 * </pre>
 *
 * Drop the {@code adapt} and {@code execute()} yields an Integer, so the
 * method does not compile — the error names exactly what is missing.
 *
 * <p>{@code adapt}, {@code fanOut}, {@code batch} and {@code use} are the four
 * steps that re-type the value; everything else preserves it.
 */
public interface NioStep<T, O> {

    NioStep<T, O> handle(UnaryOperator<T> function);

    NioStep<T, O> handle(String name, UnaryOperator<T> function);

    /**
     * Stage with a time budget: if the function does not finish within the
     * timeout, the value fails with a TimeoutException — catchable downstream
     * by recover(), like any other stage failure.
     */
    NioStep<T, O> handle(String name, UnaryOperator<T> function, Duration timeout);

    /**
     * Stage with a retry policy: failed attempts back off on the worker and,
     * once exhausted, the last failure flows to the recovery path.
     */
    NioStep<T, O> handle(String name, UnaryOperator<T> function, Retry retry);

    NioStep<T, O> handle(String name, UnaryOperator<T> function, Duration timeout, Retry retry);

    /** Rate-limited stage; see NioFlow#handle(String, Function, RateLimit). */
    NioStep<T, O> handle(String name, UnaryOperator<T> function, RateLimit rateLimit);

    /**
     * The stage that does not park: the function returns a
     * {@link CompletionStage}, a worker invokes it and is released at once, and
     * the boss resumes when the call completes. See NioFlow#handleAsync for the
     * trade (it is a dispatch boundary — it does not fuse) and for the
     * cancellation the timeout buys.
     */
    NioStep<T, O> handleAsync(String name, Function<T, CompletionStage<T>> call);

    /** Same, with a per-attempt budget that CANCELS the CompletionStage on expiry. */
    NioStep<T, O> handleAsync(String name, Function<T, CompletionStage<T>> call, Duration timeout);

    NioStep<T, O> handleAsync(String name, Function<T, CompletionStage<T>> call, Retry retry);

    NioStep<T, O> handleAsync(String name, Function<T, CompletionStage<T>> call, Duration timeout, Retry retry);

    /**
     * The re-typing async stage: {@code adapt} to {@code handleAsync} what
     * {@code adapt} is to {@code handle}. No worker parks on the call.
     */
    <R> NioStep<R, O> adaptAsync(Function<T, CompletionStage<R>> call);

    /** Same, with a budget that cancels the call on expiry. */
    <R> NioStep<R, O> adaptAsync(Function<T, CompletionStage<R>> call, Duration timeout);

    /** Context-aware stage: the value plus the typed per-execution Context. */
    NioStep<T, O> handleContextual(BiFunction<T, Context, T> function);

    NioStep<T, O> handleContextual(String name, BiFunction<T, Context, T> function);

    /** Boss-inlined stage for pure-CPU, sub-microsecond functions. */
    NioStep<T, O> handleSync(UnaryOperator<T> function);

    NioStep<T, O> handleSync(String name, UnaryOperator<T> function);

    NioStep<T, O> background(Consumer<T> effect);

    NioStep<T, O> background(String name, Consumer<T> effect);

    /**
     * Re-types the value: this is the step that moves the pipeline from one
     * type to the next, and the compiler checks it.
     */
    <R> NioStep<R, O> adapt(Function<T, R> function);

    /** Deliberate cut: execute() maps it to null, executeResult() reports it. */
    NioStep<T, O> filter(Predicate<T> predicate);

    /**
     * Parallel split-join: every branch receives the current value and runs
     * concurrently on the workers; join combines the branch results (in
     * declaration order) into the value that continues down the chain.
     */
    <R, C> NioStep<C, O> fanOut(List<Function<T, R>> branches, Function<List<R>, C> join);

    <R, C> NioStep<C, O> fanOut(String name, List<Function<T, R>> branches, Function<List<R>, C> join);

    /** Coalescing point; see NioFlow#batch. Per-request batches may re-type. */
    <R> NioStep<R, O> batch(int size, Duration window, Function<List<T>, List<R>> bulk);

    <R> NioStep<R, O> batch(String name, int size, Duration window, Function<List<T>, List<R>> bulk);

    /**
     * Detached sub-flow: the value is handed to a child execution and this
     * pipeline continues immediately — execute() returns without waiting for
     * it, and a failure the fork does not recover() reaches onError, never this
     * execution's result. See NioFlow#fork.
     */
    <R> NioStep<T, O> fork(Segment<T, R> sub);

    <R> NioStep<T, O> fork(String name, Segment<T, R> sub);

    /** Embeds a reusable segment inline; it may re-type the value. */
    <R> NioStep<R, O> use(Segment<T, R> segment);

    /**
     * Positional error handling: catches failures from links declared upstream
     * of it; execution continues after it with the recovered value.
     */
    NioStep<T, O> recover(Function<Throwable, T> function);

    NioStep<T, O> recover(String name, Function<Throwable, T> function);

    /** Scoped to THIS execution; fires before execute() returns. */
    NioStep<T, O> onComplete(Consumer<T> callback);

    NioStep<T, O> onError(Consumer<Throwable> callback);

    /**
     * Orders this execution by business key, Kafka-partition style: executions
     * sharing a key run strictly one at a time, in submission order, pinned to
     * the same boss; distinct keys keep full parallelism.
     */
    NioStep<T, O> key(Object key);

    /**
     * Seeds the per-execution {@link Context} before the pipeline runs, so a
     * {@code handleContextual} stage can read what the CALLER knew — a trace
     * id, the authenticated principal, a tenant. Without it the context can
     * only be written from inside a stage, which is too late for anything the
     * caller had and the pipeline needs.
     *
     * <p>Pay for what you use: the backing map is created on the first
     * {@code with()}, so a pipeline that never seeds anything allocates
     * nothing for it.
     */
    <V> NioStep<T, O> with(Context.Key<V> key, V value);

    StepCondition<T, O> when(Predicate<T> predicate);

    StepCases<T, O> match();

    /**
     * Runs the execution and blocks until the result is ready. Equivalent to
     * executeAsync().join(). Returns the value's CURRENT type: if that is not
     * what your method promised, add the adapt the compiler is asking for.
     */
    T execute();

    /**
     * Runs the execution and returns immediately with the promise of the
     * result — the caller's thread never blocks. Return it from a controller
     * for a non-blocking endpoint.
     */
    CompletableFuture<T> executeAsync();

    /**
     * Same, with the context of THIS run — entries the pipeline itself did not
     * declare, keyed by {@link Context.Key#name()} exactly like the map handed
     * to {@code engine.call(input, map)}.
     *
     * <p>Why this exists next to {@link #with(Context.Key, Object)}: {@code with}
     * is a builder step, so it writes into the pipeline and every run of that
     * pipeline sees what it wrote. That is right for what the CALLER knew when
     * it opened the pipeline, and wrong for what a SUBSCRIPTION knows: two
     * subscriptions of the same Mono would race on it, and a key one of them
     * carries would linger for the other. This overload keeps a run's entries in
     * the run — the map is merged with the pipeline's own seed into a fresh
     * context, and {@code with()} wins on a name they share (it is the one the
     * pipeline declared).
     *
     * <p>Null or empty is exactly {@link #executeAsync()}: no map is created.
     * The reactive bridge ({@code ReactiveFlow.propagate}) is the caller this was
     * written for.
     */
    CompletableFuture<T> executeAsync(Map<String, Object> context);

    /**
     * Like execute(), but the outcome distinguishes a deliberate Filter cut
     * (Filtered) from a completed value (Completed) — including a genuinely
     * null one, which execute() cannot tell apart from a cut — and both from an
     * execution stopped from the outside (Cancelled).
     */
    FlowResult<T> executeResult();

    /**
     * Runs the execution like {@link #executeAsync()}, and hands back the handle
     * to stop it — for the caller who may stop caring about the answer: an HTTP
     * client that disconnected, a request that lost its race, a disposed Mono
     * (which is what {@code executeMono()} is built on).
     *
     * <p>Cancellation is <b>cooperative</b>: it stops the chain from advancing
     * and cancels the in-flight call of an async stage (handleAsync / adaptAsync
     * / handleMonoAsync — the CompletionStage is cancelled, so the connection is
     * released), but it does NOT interrupt a blocking stage already running on a
     * worker, and it does not touch a fork. The guarantee is the precise one:
     * the card is not charged because {@code charge} is the stage that never
     * gets invoked. See {@link Cancellable}.
     */
    Cancellable<T> executeCancellable();

    /** Same, with the context of THIS run — see {@link #executeAsync(Map)}. */
    Cancellable<T> executeCancellable(Map<String, Object> context);
}
