package dev.nioflow.core.facade;

import dev.nioflow.core.model.RateLimit;
import dev.nioflow.core.model.Retry;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

/**
 * The shared definition — the thing you declare once and execute per request.
 *
 * <p>The two type parameters are a promise: <b>{@code I} is what goes in
 * ({@link #just(Object)} accepts it) and {@code O} is what comes out</b> of the
 * pipeline you build for each request. So a
 * {@code NioFlow<Integer, String>} takes an Integer and answers a String:
 *
 * <pre>
 * NioFlow&lt;Integer, String&gt; credits = DefaultNioFlow.from(Integer.class);
 *
 * String credit(int cents) {
 *     return credits.just(cents)               // the pipeline starts at the INPUT type
 *             .handle("charge", item -&gt; item)  // item is an Integer here
 *             .adapt(item -&gt; "EUR " + item)    // adapt is what re-types it
 *             .execute();                      // returns String
 * }
 * </pre>
 *
 * <p>Because the value that reaches your first per-request step is the one the
 * shared chain left behind, <b>the shared definition is type-preserving</b>:
 * everything you declare here takes an {@code I} and leaves an {@code I}. That
 * is why there is no {@code adapt} on this interface — re-typing belongs to
 * {@link NioStep}, the per-request builder that {@code just()} hands you, where
 * the compiler tracks the value's type step by step.
 *
 * <p>{@code O} states the contract of the pipelines you write; the compiler
 * holds you to it through the return type of your own method (forget the
 * {@code adapt} above and {@code credit()} will not compile). Lifecycle
 * ({@code close}) is not part of this contract: it belongs to the root
 * implementation that owns the engine.
 */
public interface NioFlow<I, O> {

    /**
     * Opens an isolated execution over a snapshot of the shared chain, and
     * hands back a builder that starts at the INPUT type. Any number of
     * concurrent requests can do this on the same flow: they share nothing.
     */
    NioStep<I, O> just(I input);

    /** Fire-and-forget through the shared chain; collect with engine.await(). */
    NioFlow<I, O> justAll(Iterable<I> inputs);

    NioFlow<I, O> handle(UnaryOperator<I> function);

    NioFlow<I, O> handle(String name, UnaryOperator<I> function);

    /**
     * Stage with a time budget: if the function does not finish within the
     * timeout, the value fails with a TimeoutException — catchable downstream
     * by recover(), like any other stage failure.
     */
    NioFlow<I, O> handle(String name, UnaryOperator<I> function, Duration timeout);

    /**
     * Stage with a retry policy: failed attempts back off on the worker and,
     * once exhausted, the last failure flows to the recovery path. Composes
     * in layers: rate limit → timeout per attempt → retry over attempts →
     * recover() as the final net.
     */
    NioFlow<I, O> handle(String name, UnaryOperator<I> function, Retry retry);

    NioFlow<I, O> handle(String name, UnaryOperator<I> function, Duration timeout, Retry retry);

    /**
     * Rate-limited stage: acquires a token before each application, parking
     * the (virtual) worker until one is due — the boss never waits. Pass the
     * SAME RateLimit instance to several stages to protect one downstream.
     */
    NioFlow<I, O> handle(String name, UnaryOperator<I> function, RateLimit rateLimit);

    /**
     * The stage that does not park: the function returns a
     * {@link CompletionStage}, a worker invokes it and is released at once, and
     * the boss resumes when the call completes. An in-flight request retains no
     * parked thread — which is the whole difference from a {@code handle} that
     * blocks on a remote call.
     *
     * <pre>
     * orders.handleAsync("quote", order -&gt; http.sendAsync(request(order), ofString())
     *         .thenApply(response -&gt; order.withQuote(response.body())));
     * </pre>
     *
     * <p><b>The trade is fusion.</b> An async stage is a dispatch boundary: four
     * of them are four boss→worker round trips, where four consecutive
     * {@code handle}s fuse into one. Hops are microseconds against a remote call
     * measured in milliseconds, so the axis that moves is HEAP — pick this one
     * when in-flight concurrency is high, or when you need the call cancelled.
     *
     * <p><b>Cancellation.</b> With a timeout, the engine does not merely abandon
     * the call: it cancels the {@link CompletionStage} (a
     * {@code mono.toFuture()} cancels the subscription and reactor-netty
     * releases the connection; an {@code HttpClient} future aborts the exchange).
     * A {@code handle(name, fn, timeout)} cannot do that — it can only stop
     * waiting.
     *
     * <p><b>No RateLimit overload, on purpose:</b> {@code acquire()} parks the
     * worker for the admission wait, which is precisely what this step exists to
     * avoid. Rate-limit an upstream {@code handle} instead — the wait is then
     * where it belongs, and the remote call still holds no thread.
     *
     * @throws IllegalStateException at runtime if the call returns null (a
     *         missing CompletionStage is a bug, not an empty result)
     */
    NioFlow<I, O> handleAsync(String name, Function<I, CompletionStage<I>> call);

    /**
     * Same, with a time budget per attempt — and on expiry the
     * {@link CompletionStage} is <b>cancelled</b>, not just abandoned. The
     * failure is an ordinary {@link java.util.concurrent.TimeoutException} that
     * {@code recover()} catches like any other stage failure.
     */
    NioFlow<I, O> handleAsync(String name, Function<I, CompletionStage<I>> call, Duration timeout);

    /**
     * Retry over the async call: a failed attempt re-invokes the function on a
     * worker, backing off without parking anyone (there is no parked worker to
     * back off on). Composes in the documented layers.
     */
    NioFlow<I, O> handleAsync(String name, Function<I, CompletionStage<I>> call, Retry retry);

    NioFlow<I, O> handleAsync(String name, Function<I, CompletionStage<I>> call, Duration timeout, Retry retry);

    /**
     * Context-aware stage: besides the value it receives the typed
     * per-execution Context. Plain stages never pay for it; the backing map is
     * created on the first put.
     */
    NioFlow<I, O> handleContextual(BiFunction<I, Context, I> function);

    NioFlow<I, O> handleContextual(String name, BiFunction<I, Context, I> function);

    /**
     * Opt-in for pure-CPU, sub-microsecond functions: runs inline on the event
     * loop, skipping both thread hops. Same contract as when()/match()
     * predicates — cheap and never blocking.
     */
    NioFlow<I, O> handleSync(UnaryOperator<I> function);

    NioFlow<I, O> handleSync(String name, UnaryOperator<I> function);

    NioFlow<I, O> background(Consumer<I> effect);

    NioFlow<I, O> background(String name, Consumer<I> effect);

    NioFlow<I, O> filter(Predicate<I> predicate);

    /**
     * Parallel split-join. The branches may compute anything, but the join
     * gives the value back as an {@code I}: the shared chain preserves the
     * type. (Need the fan-out to re-type? Do it per request — see
     * {@link NioStep#fanOut}.)
     */
    <R> NioFlow<I, O> fanOut(List<Function<I, R>> branches, Function<List<R>, I> join);

    <R> NioFlow<I, O> fanOut(String name, List<Function<I, R>> branches, Function<List<R>, I> join);

    /**
     * Coalescing point for bulk work: executions park here until `size` of them
     * accumulated or `window` elapsed, then ONE bulk call receives all their
     * values and must return one result per value, positionally. Each caller
     * still gets its own result — the batch is invisible to them. Belongs on
     * the shared definition: only executions of the same flow pool together.
     */
    NioFlow<I, O> batch(int size, Duration window, UnaryOperator<List<I>> bulk);

    NioFlow<I, O> batch(String name, int size, Duration window, UnaryOperator<List<I>> bulk);

    /**
     * Detached sub-flow: the value is handed to a child execution and the main
     * line continues immediately — the request does NOT wait for it, and a
     * failure the fork does not recover() reaches onError, never the caller's
     * future. The sub-flow is a full pipeline (handle, background, adapt,
     * filter, recover, fanOut, batch, when/match, nested forks), so its body is
     * a {@link Segment} and may re-type freely: nothing comes back.
     * Type-preserving on the main line, which is why it belongs here.
     *
     * <p>Need the result on the main line? That is {@link #fanOut}.
     */
    <R> NioFlow<I, O> fork(Segment<I, R> sub);

    <R> NioFlow<I, O> fork(String name, Segment<I, R> sub);

    /** Embeds a reusable segment inline, with the shared chain's type. */
    NioFlow<I, O> use(Segment<I, I> segment);

    /**
     * Embeds and remembers the span as a named REGION, so spliceRegion (or
     * DefaultNioFlow.replaceRegion) can swap the whole thing atomically at
     * runtime.
     */
    NioFlow<I, O> use(String region, Segment<I, I> segment);

    /**
     * Positional error handling: catches failures (exceptions and stage
     * timeouts) from links declared upstream of it, and the flow continues
     * after it with the recovered value.
     */
    NioFlow<I, O> recover(Function<Throwable, I> function);

    NioFlow<I, O> recover(String name, Function<Throwable, I> function);

    /**
     * Observes successful outcomes of EVERY execution of this flow — the
     * terminal value, which the pipelines you write are meant to leave at
     * {@code O}. Callbacks run on engine threads: keep them fast and never
     * throw (a throwing complete callback is reported to onError).
     */
    NioFlow<I, O> onComplete(Consumer<O> callback);

    /**
     * The engine's error tap: terminal failures no recover() caught, plus
     * rejected or dropped values and failing background effects.
     */
    NioFlow<I, O> onError(Consumer<Throwable> callback);

    Condition<I, O> when(Predicate<I> predicate);

    Cases<I, O> match();
}
