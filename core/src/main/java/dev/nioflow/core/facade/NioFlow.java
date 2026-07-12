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
 * End-to-end typed pipeline: I is the input type (what just accepts), T is
 * the value's type at the current point of the chain. adapt() is the only
 * step that changes T; everything else preserves it. Lifecycle (close) is
 * NOT part of this contract: it belongs to the root implementation that owns
 * the engine, never to branches or executions.
 */
public interface NioFlow<I, T> {

    NioFlow<I, T> just(I input);

    NioFlow<I, T> justAll(Iterable<I> inputs);

    NioFlow<I, T> handle(Function<T, T> function);

    NioFlow<I, T> handle(String name, Function<T, T> function);

    /**
     * Stage with a time budget: if the function does not finish within the
     * timeout, the value fails with a TimeoutException — catchable downstream
     * by recover(), like any other stage failure.
     */
    NioFlow<I, T> handle(String name, Function<T, T> function, Duration timeout);

    /**
     * Stage with a retry policy: failed attempts back off on the worker and,
     * once exhausted, the last failure flows to the recovery path. Composes
     * in layers: timeout per attempt → retry over attempts → recover() as the
     * final net.
     */
    NioFlow<I, T> handle(String name, Function<T, T> function, Retry retry);

    NioFlow<I, T> handle(String name, Function<T, T> function, Duration timeout, Retry retry);

    /**
     * Rate-limited stage: acquires a token from the bucket before each
     * application, parking the (virtual) worker until one is due — the boss
     * never waits, fusion is preserved, and the wait shows up in the stage's
     * latency metric. Pass the SAME RateLimit instance to several stages to
     * protect one downstream dependency behind all of them.
     */
    NioFlow<I, T> handle(String name, Function<T, T> function, RateLimit rateLimit);

    /**
     * Context-aware stage: besides the value it receives the typed
     * per-execution Context (Context.Key accessors) — scratch state shared
     * by this execution's stages without threading it through the value
     * type. Plain stages never pay for the context; the backing map is
     * created on the first put.
     */
    NioFlow<I, T> handleContextual(BiFunction<T, Context, T> function);

    NioFlow<I, T> handleContextual(String name, BiFunction<T, Context, T> function);

    /**
     * Opt-in for pure-CPU, sub-microsecond functions: runs inline on the
     * boss thread, skipping both thread hops (boss→worker→boss). Same
     * contract as when()/match() predicates — cheap and never blocking; a
     * throw fails the value through the recovery path, never the engine.
     * Deliberately has no timeout/retry variants: nothing can cut a
     * boss-inlined call, and retry backoff would park the boss.
     */
    NioFlow<I, T> handleSync(Function<T, T> function);

    NioFlow<I, T> handleSync(String name, Function<T, T> function);

    NioFlow<I, T> background(Consumer<T> effect);

    NioFlow<I, T> background(String name, Consumer<T> effect);

    <R> NioFlow<I, R> adapt(Function<T, R> function);

    /**
     * Parallel split-join: every branch receives the current value and runs
     * concurrently on the workers; join combines the branch results (in
     * declaration order) into the value that continues down the chain. Any
     * branch failure fails the fan-out — recoverable downstream.
     */
    <R, C> NioFlow<I, C> fanOut(List<Function<T, R>> branches, Function<List<R>, C> join);

    <R, C> NioFlow<I, C> fanOut(String name, List<Function<T, R>> branches, Function<List<R>, C> join);

    /**
     * Coalescing point for bulk work: executions reaching this link park
     * until `size` of them accumulated or `window` elapsed since the first,
     * then ONE bulk call receives all their values and must return one
     * result per value, positionally. Each execution continues its own
     * chain with its own element and each caller's future completes with
     * its individual result — the batch is invisible to callers. A bulk
     * failure (or a result of the wrong size) fails every batched
     * execution, recoverable downstream per execution. Belongs on the
     * shared definition: only executions of the same flow pool together.
     */
    <R> NioFlow<I, R> batch(int size, Duration window, Function<List<T>, List<R>> bulk);

    <R> NioFlow<I, R> batch(String name, int size, Duration window, Function<List<T>, List<R>> bulk);

    /**
     * Embeds a reusable segment inline: its links join this chain as if they
     * had been declared here, and the pipeline continues at the segment's
     * output type.
     */
    <R> NioFlow<I, R> use(Segment<T, R> segment);

    NioFlow<I, T> filter(Predicate<T> predicate);

    /**
     * Positional error handling: catches failures (exceptions and stage
     * timeouts) from links declared upstream of it, and the flow continues
     * after it with the recovered value. Failures downstream are not caught.
     */
    NioFlow<I, T> recover(Function<Throwable, T> function);

    NioFlow<I, T> recover(String name, Function<Throwable, T> function);

    /**
     * Observes successful outcomes: the callback receives the flow's terminal
     * value (null for a filtered cut, like execute()). On the shared
     * definition it fires for EVERY execution of the flow — inject/justAll
     * included; on a just() execution it is scoped to that execution only.
     * Declare it after the last re-typing step (adapt), since it observes the
     * terminal value. Callbacks run on engine threads: keep them fast and
     * never throw (a throwing complete callback is reported to onError).
     */
    NioFlow<I, T> onComplete(Consumer<T> callback);

    /**
     * Observes failures: the callback receives the unwrapped terminal error
     * of an execution no recover() caught. On the shared definition it is the
     * engine's error tap — it also sees non-execution errors (rejected or
     * dropped values, failing background effects); on a just() execution it
     * is scoped to that execution only.
     */
    NioFlow<I, T> onError(Consumer<Throwable> callback);

    /**
     * Orders this execution by business key, Kafka-partition style:
     * executions sharing a (non-null) key run strictly one at a time in
     * submission order, pinned to the same boss; distinct keys keep full
     * parallelism. Only meaningful on a just() execution — the shared
     * definition has no execution to key. A slow execution delays the
     * NEXT ones of its own key only (head-of-line by design; a stage
     * timeout bounds it).
     */
    NioFlow<I, T> key(Object key);

    Condition<I, T> when(Predicate<T> predicate);

    Cases<I, T> match();

    /**
     * Runs the execution and blocks the caller until the result is ready.
     * Equivalent to executeAsync().join().
     */
    T execute();

    /**
     * Runs the execution and returns immediately with the promise of the
     * result — the caller's thread never blocks. Returning this future from
     * a Spring controller yields a non-blocking endpoint. It completes
     * exceptionally with the terminal failure when no recover() caught it.
     */
    CompletableFuture<T> executeAsync();

    /**
     * Like execute(), but the outcome distinguishes a deliberate Filter cut
     * (Filtered) from a completed value (Completed) — including a genuinely
     * null one, which execute() cannot tell apart from a cut.
     */
    FlowResult<T> executeResult();
}
