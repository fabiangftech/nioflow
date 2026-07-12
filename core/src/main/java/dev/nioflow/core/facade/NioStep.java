package dev.nioflow.core.facade;

import dev.nioflow.core.model.RateLimit;
import dev.nioflow.core.model.Retry;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

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

    NioStep<T, O> handle(Function<T, T> function);

    NioStep<T, O> handle(String name, Function<T, T> function);

    /**
     * Stage with a time budget: if the function does not finish within the
     * timeout, the value fails with a TimeoutException — catchable downstream
     * by recover(), like any other stage failure.
     */
    NioStep<T, O> handle(String name, Function<T, T> function, Duration timeout);

    /**
     * Stage with a retry policy: failed attempts back off on the worker and,
     * once exhausted, the last failure flows to the recovery path.
     */
    NioStep<T, O> handle(String name, Function<T, T> function, Retry retry);

    NioStep<T, O> handle(String name, Function<T, T> function, Duration timeout, Retry retry);

    /** Rate-limited stage; see NioFlow#handle(String, Function, RateLimit). */
    NioStep<T, O> handle(String name, Function<T, T> function, RateLimit rateLimit);

    /** Context-aware stage: the value plus the typed per-execution Context. */
    NioStep<T, O> handleContextual(BiFunction<T, Context, T> function);

    NioStep<T, O> handleContextual(String name, BiFunction<T, Context, T> function);

    /** Boss-inlined stage for pure-CPU, sub-microsecond functions. */
    NioStep<T, O> handleSync(Function<T, T> function);

    NioStep<T, O> handleSync(String name, Function<T, T> function);

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
     * Like execute(), but the outcome distinguishes a deliberate Filter cut
     * (Filtered) from a completed value (Completed) — including a genuinely
     * null one, which execute() cannot tell apart from a cut.
     */
    FlowResult<T> executeResult();
}
