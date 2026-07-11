package dev.nioflow.core.facade;


import dev.nioflow.core.model.Diagnostics;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

/**
 * A nio-flow: a chain of stages declared once, through which many injected values
 * flow concurrently and independently. Declare the chain fluently — {@code handle}
 * and {@code submit} stages, {@code filter} drops, {@code when}/{@code match} forks,
 * {@code batch} groupings, {@code fanOut} splits and {@code onErrorResume}
 * recoveries — then inject values with {@code just} and wait with {@code join()}.
 *
 * <p>Order is preserved per value, never across values: a fast value may finish
 * before a slower one injected earlier, and a value blocked on slow IO never delays
 * the values behind it. An error short-circuits only the failing value; every other
 * value keeps flowing.
 *
 * <p>Declare the whole chain — recoveries and observers included — before injecting
 * values, and {@code seal()} it once complete: links only affect values that reach
 * them, so late declarations can miss values already past that point.
 *
 * @param <T> the type of the values flowing at this point of the chain;
 *            {@link #adapt(Function)} and {@link #fanOut(Function)} hand out a
 *            differently-typed view over the same running chain
 */
public interface NioFlow<T> extends AutoCloseable {

    /**
     * Injects a value: it starts walking the declared chain immediately, concurrent
     * with any values already in flight, subject to the nio-flow's backpressure
     * policy once capacity is reached.
     *
     * @param input the value to inject
     * @return this nio-flow, for chaining
     */
    NioFlow<T> just(T input);

    /**
     * Like {@link #just(Object)} with seed metadata (trace id, tenant, ...) that
     * travels with the value: any stage of it can read or extend it through
     * {@code FlowContext}, no matter which thread runs the stage.
     *
     * @param input   the value to inject
     * @param context initial metadata copied into the value's {@code FlowContext}
     * @return this nio-flow, for chaining
     */
    NioFlow<T> just(T input, Map<String, Object> context);

    /**
     * Injects every value in iteration order, honoring backpressure per value.
     *
     * @param inputs the values to inject, one flowing value each
     * @return this nio-flow, for chaining
     */
    NioFlow<T> justAll(Iterable<T> inputs);

    /**
     * Request/response injection: injects the value like {@link #just(Object)} and
     * returns a future resolved with that value's — and only that value's — own
     * outcome, concurrent with every other call. Completed with the value's
     * end-of-chain result; failed when the value fails past every recovery, or when
     * admission rejects it (closed nio-flow, FAIL backpressure) — never thrown;
     * cancelled when the value leaves deliberately (a {@code filter} drop, an empty
     * {@code fanOut}, a DROP backpressure policy). A {@code fanOut} into several
     * values resolves the future with the first of them to finish.
     *
     * <p>This is the natural fit for request-driven callers — a web handler serving
     * each request from one shared, long-lived nio-flow: declare the chain once,
     * {@code release()} (or {@code seal()}) it, and serve every request with a
     * bounded {@code call}. Prefer the {@code timeout} variants there: they put an
     * upper bound on the caller's wait, whatever happens to the value.
     *
     * @param <R>   the type flowing at the end of the chain; the engine is untyped,
     *              so the caller states what the chain delivers
     * @param input the value to inject
     * @return a future delivering this value's own outcome
     */
    <R> CompletableFuture<R> call(T input);

    /**
     * Like {@link #call(Object)} with seed metadata (trace id, tenant, ...) copied
     * into the value's {@code FlowContext}.
     *
     * @param <R>     the type flowing at the end of the chain
     * @param input   the value to inject
     * @param context initial metadata copied into the value's {@code FlowContext}
     * @return a future delivering this value's own outcome
     */
    <R> CompletableFuture<R> call(T input, Map<String, Object> context);

    /**
     * Like {@link #call(Object)} but bounded: if the value has not finished when the
     * timeout expires, the future fails with a {@code TimeoutException}; the value
     * itself keeps flowing and its late outcome is ignored.
     *
     * @param <R>     the type flowing at the end of the chain
     * @param input   the value to inject
     * @param timeout how long to wait for the value's outcome before giving up
     * @return a future delivering this value's own outcome
     */
    <R> CompletableFuture<R> call(T input, Duration timeout);

    /**
     * Like {@link #call(Object, Map)} and {@link #call(Object, Duration)} combined:
     * seed metadata plus a bound on how long the caller waits.
     *
     * @param <R>     the type flowing at the end of the chain
     * @param input   the value to inject
     * @param context initial metadata copied into the value's {@code FlowContext}
     * @param timeout how long to wait for the value's outcome before giving up
     * @return a future delivering this value's own outcome
     */
    <R> CompletableFuture<R> call(T input, Map<String, Object> context, Duration timeout);

    /**
     * Appends a synchronous stage: the function transforms the value in place on a
     * handle worker. With the default virtual workers, blocking here ties up only
     * this value; only on a fixed handle-worker pool must handles stay fast — reach
     * for {@link #submit(Function)} there when the stage does IO.
     *
     * @param function the transformation applied to each flowing value
     * @return this nio-flow, for chaining
     */
    NioFlow<T> handle(Function<T, T> function);

    /**
     * Like {@link #handle(Function)} but named: a failure arrives wrapped in a
     * {@code StageException} carrying the stage name, so errors say where they
     * happened. Unnamed stages deliver the thrown exception untouched.
     *
     * @param name     the stage name reported by failures, diagnostics and metrics
     * @param function the transformation applied to each flowing value
     * @return this nio-flow, for chaining
     */
    NioFlow<T> handle(String name, Function<T, T> function);

    /**
     * Like {@link #handle(Function)} with the stage decorated by a {@link Resilience}
     * policy. With the default virtual handle workers any policy works. On a fixed
     * worker pool ({@code NioFlow(executor, handleWorkers)}) prefer non-blocking
     * policies (e.g. a circuit breaker): a waiting policy ties up a shared worker.
     *
     * @param function   the transformation applied to each flowing value
     * @param resilience the policy decorating the function (retry, circuit breaker, ...)
     * @return this nio-flow, for chaining
     */
    NioFlow<T> handle(Function<T, T> function, Resilience<T> resilience);

    /**
     * Appends an asynchronous stage: the function runs on the nio-flow's executor
     * and the engine moves on without waiting, so a value blocked on slow IO (JDBC,
     * HTTP, ...) never delays the values behind it. The result is reaped later and
     * the value resumes with its next stage.
     *
     * @param function the transformation applied to each flowing value
     * @return this nio-flow, for chaining
     */
    NioFlow<T> submit(Function<T, T> function);

    /**
     * Like {@link #submit(Function)} but named: a failure arrives wrapped in a
     * {@code StageException} carrying the stage name.
     *
     * @param name     the stage name reported by failures, diagnostics and metrics
     * @param function the transformation applied to each flowing value
     * @return this nio-flow, for chaining
     */
    NioFlow<T> submit(String name, Function<T, T> function);

    /**
     * Like {@link #submit(Function)} but bounded: when the timeout expires the worker
     * is interrupted and only that value fails, with a {@code TimeoutException}.
     *
     * @param function the transformation applied to each flowing value
     * @param timeout  how long a single execution may run before it is cancelled
     * @return this nio-flow, for chaining
     */
    NioFlow<T> submit(Function<T, T> function, Duration timeout);

    /**
     * Like {@link #submit(Function)} with the stage decorated by a {@link Resilience}
     * policy (retries, circuit breaker, ...). Exhausted policies fail only that value,
     * like any other stage error.
     *
     * @param function   the transformation applied to each flowing value
     * @param resilience the policy decorating the function (retry, circuit breaker, ...)
     * @return this nio-flow, for chaining
     */
    NioFlow<T> submit(Function<T, T> function, Resilience<T> resilience);

    /**
     * Appends a fire-and-forget effect: the consumer is launched on the nio-flow's
     * executor with the value's payload as of this point, and the value moves on
     * <em>immediately</em> — neither the value nor any caller waiting on it
     * ({@code join()}, {@code call}) ever waits for the effect. The shape for
     * side-effect tails — notifications, logging, audit — that must not delay the
     * caller's answer; use {@link #submit(Function)} instead when the async work
     * produces the value's result.
     *
     * <p>The effect sees a snapshot: the payload as of this link and a copy of the
     * {@code FlowContext}. A throwing effect is reported to {@code onError}
     * handlers and metrics, but never fails the value — it already moved on.
     *
     * @param effect the side effect, receiving the payload as of this link
     * @return this nio-flow, for chaining
     */
    NioFlow<T> background(Consumer<T> effect);

    /**
     * Like {@link #background(Consumer)} but named: a failure is reported wrapped
     * in a {@code StageException} carrying the name, and metrics count it under it.
     *
     * @param name   the effect name reported by failures and metrics
     * @param effect the side effect, receiving the payload as of this link
     * @return this nio-flow, for chaining
     */
    NioFlow<T> background(String name, Consumer<T> effect);

    /**
     * Groups values into lists of up to {@code size}, flushing a partial group after
     * its oldest value waited {@code maxWait}, and runs one async call for the whole
     * group — ideal for bulk IO like JDBC batch writes. The function must return
     * exactly one result per input, matched by index; each result continues down the
     * chain as its own value. A failure fails every value of the group, each one
     * individually recoverable by a downstream {@code onErrorResume}.
     *
     * @param size     the group size that triggers an immediate flush
     * @param maxWait  how long the oldest value of a partial group may wait before
     *                 the group flushes anyway
     * @param function the bulk call; receives the group in arrival order and must
     *                 return one result per input, matched by index
     * @return this nio-flow, for chaining
     */
    NioFlow<T> batch(int size, Duration maxWait, Function<List<T>, List<T>> function);

    /**
     * Appends a type-changing stage and returns a view of the same running chain
     * typed to the new values. Runs like a {@link #handle(Function)}. Register
     * {@code onComplete} on the final view, after the last {@code adapt}.
     *
     * @param <N>      the type flowing after this stage
     * @param function the conversion applied to each flowing value
     * @return a view of this nio-flow typed to the converted values
     */
    <N> NioFlow<N> adapt(Function<T, N> function);

    /**
     * Splits a value into many: each element of the returned list continues down the
     * chain as its own independent value, inheriting the parent's lanes; the parent
     * is consumed. An empty list drops the value like a {@code filter}. Runs like a
     * {@code handle} — keep it fast; produce the elements with a {@code submit}
     * first when IO is involved.
     *
     * @param <N>      the type of the split-off values
     * @param function maps the parent value to its children; must not return null
     * @return a view of this nio-flow typed to the split-off values
     */
    <N> NioFlow<N> fanOut(Function<T, List<N>> function);

    /**
     * Splices a reusable segment — a function over the nio-flow itself — into the
     * chain at this point, so sub-nio-flows can be defined once and composed
     * anywhere, including inside lanes. The segment may change the type.
     *
     * <pre>{@code
     * static Function<NioFlow<Order>, NioFlow<Invoice>> billing() {
     *     return p -> p
     *             .submit(o -> price(o))
     *             .adapt(o -> invoice(o));
     * }
     *
     * nio-flow.via(billing())
     *         .submit(invoice -> send(invoice));
     * }</pre>
     *
     * @param <N>     the type flowing after the segment
     * @param segment the reusable chain fragment to splice in at this point
     * @return the view returned by the segment, to keep chaining from
     */
    default <N> NioFlow<N> via(Function<? super NioFlow<T>, ? extends NioFlow<N>> segment) {
        return segment.apply(this);
    }

    /**
     * Keeps only values matching the predicate. Dropped values leave deliberately:
     * they fire neither {@code onComplete} nor {@code onError}, stop counting toward
     * {@code join()} and free their backpressure slot. A throwing predicate is a
     * normal stage failure.
     *
     * @param predicate decides which values keep flowing
     * @return this nio-flow, for chaining
     */
    NioFlow<T> filter(Predicate<T> predicate);

    /**
     * Two-way fork: values matching the predicate take the {@code then} lane, the
     * rest take the optional {@code otherwise} lane, and stages chained after the
     * fork are back on the main line, running for every value. See {@link Condition}
     * for the full shape and an example.
     *
     * @param predicate routes each value into the then or otherwise lane
     * @return the fork under construction; declare its lanes with
     *         {@link Condition#then(UnaryOperator)}
     */
    Condition<T> when(Predicate<T> predicate);

    /**
     * Multi-way fork, switch-style: cases are tried in declaration order, the first
     * matching predicate routes the value into its lane, and the remaining predicates
     * are not evaluated. Unmatched values take {@code otherwise} — or pass through
     * unchanged without it. Stages chained after the match are back on the main line.
     *
     * <pre>{@code
     * nio-flow.match()
     *         .is(x -> x > 100, lane -> lane
     *                 .submit(x -> big(x)))
     *         .is(x -> x > 10, lane -> lane
     *                 .handle(x -> medium(x)))
     *         .otherwise(lane -> lane
     *                 .handle(x -> small(x)))
     *         .handle(x -> audit(x));   // main line: runs for every value
     * }</pre>
     *
     * @return the fork under construction; declare its lanes with
     *         {@link Cases#is(Predicate, UnaryOperator)}
     */
    Cases<T> match();

    /**
     * Registers a non-blocking handler for terminal failures (recovered values never
     * reach it). The handler runs with the failing value's {@code FlowContext} bound,
     * so it can tell which value failed. Handlers registered after failures happened
     * get the most recent ones replayed — a bounded history, so a long-running
     * nio-flow does not retain every throwable it ever saw; replays run unbound.
     *
     * @param handler receives each terminal failure; must be fast and never throw
     * @return this nio-flow, for chaining
     */
    NioFlow<T> onError(Consumer<Throwable> handler);

    /**
     * Recovery link: when a value fails at any upstream link, the fallback turns the
     * error into a replacement value and the flow resumes from here — instead of the
     * value being dropped. Values flowing normally skip it. If the fallback itself
     * throws, the error moves on to the next recovery downstream. Like any link, it
     * only catches failures that happen after it was declared — declare the chain,
     * recoveries included, before injecting values.
     *
     * @param fallback turns the upstream failure into the replacement value
     * @return this nio-flow, for chaining
     */
    NioFlow<T> onErrorResume(Function<Throwable, T> fallback);

    /**
     * Registers a handler invoked every time a value finishes the chain. Failed
     * values go to {@code onError} instead. Register it on the final view of the
     * chain (after any {@code adapt}) and before injecting values.
     *
     * @param handler receives each finished value; must be fast and never throw
     * @return this nio-flow, for chaining
     */
    NioFlow<T> onComplete(Consumer<T> handler);

    /**
     * Registers the observability sink for lifecycle counters and per-stage latency;
     * register it before injecting values. A second call replaces the first.
     *
     * @param metrics the sink the engine reports to; must be fast and never throw
     * @return this nio-flow, for chaining
     */
    NioFlow<T> metrics(NioFlowMetrics metrics);

    /**
     * Opt-in trace mode: the tracer sees every transition of every value — stage
     * in/out, lanes taken, drops, splits, recoveries, failures and completions.
     * Register it before injecting values; a second call replaces the first.
     *
     * @param tracer the sink for per-value transitions; must be fast and never throw
     * @return this nio-flow, for chaining
     */
    NioFlow<T> trace(NioFlowTracer tracer);

    /**
     * A point-in-time snapshot of the nio-flow: chain shape, queue depths, active,
     * parked and batched counts. Its {@code toString()} renders the full dump.
     *
     * @return a consistent snapshot of the chain and the engine's counters
     */
    Diagnostics diagnostics();

    /**
     * Structural edit: starts a new version of the chain without the first stage
     * named {@code name} — or, when the name was previously {@code replace}d, without
     * that whole region. Editing never disturbs values in flight — they finish on
     * the version they were injected into, with their recoveries and lanes intact —
     * while values injected after the edit walk the new chain. Values parked at the
     * end of the old version are released: appending later does not resume them.
     * Edits are engine-locked, so they are safe from any thread.
     *
     * @param name the name of the stage to remove
     * @return this nio-flow, for chaining
     * @throws IllegalArgumentException when no stage is named {@code name}
     * @throws IllegalStateException    when the nio-flow is sealed
     */
    NioFlow<T> remove(String name);

    /**
     * Structural edit: starts a new version of the chain where the segment's links
     * take the place of the first stage named {@code name}. The segment builds its
     * replacement with the full fluent API — stages, filters, forks, recoveries —
     * but only declares structure: injecting, joining or registering handlers inside
     * it throws. The spliced links are remembered as the name's <em>region</em>: a
     * later {@code replace} with the same name swaps the whole segment again, and
     * {@code remove} takes all of it out — the idiom for one long-lived instance
     * serving an evolving set of flows. Versioning semantics are those of
     * {@link #remove(String)}.
     *
     * <pre>{@code
     * flow.replace("routes", f -> f.match()
     *         .is(v -> isBilling(v), lane -> lane.handle(v -> billing(v)))
     *         .is(v -> isAudit(v),   lane -> lane.handle(v -> audit(v))));
     * }</pre>
     *
     * @param name    the name of the stage the segment replaces
     * @param segment declares the replacement links on a detached view of this type
     * @return this nio-flow, for chaining
     * @throws IllegalArgumentException when no stage is named {@code name}
     * @throws IllegalStateException    when the nio-flow is sealed
     */
    NioFlow<T> replace(String name, UnaryOperator<NioFlow<T>> segment);

    /**
     * Structural edit: starts a new version of the chain with the segment's links
     * spliced right before the first stage named {@code anchor}. The spliced links
     * inherit the anchor's lane, so editing inside a fork stays inside that fork.
     * Versioning and segment semantics are those of {@link #replace(String, UnaryOperator)}.
     *
     * @param anchor  the name of the stage the segment goes before
     * @param segment declares the links to insert on a detached view of this type
     * @return this nio-flow, for chaining
     * @throws IllegalArgumentException when no stage is named {@code anchor}
     * @throws IllegalStateException    when the nio-flow is sealed
     */
    NioFlow<T> insertBefore(String anchor, UnaryOperator<NioFlow<T>> segment);

    /**
     * Structural edit: starts a new version of the chain with the segment's links
     * spliced right after the first stage named {@code anchor}. The spliced links
     * inherit the anchor's lane, so editing inside a fork stays inside that fork.
     * Versioning and segment semantics are those of {@link #replace(String, UnaryOperator)}.
     *
     * @param anchor  the name of the stage the segment goes after
     * @param segment declares the links to insert on a detached view of this type
     * @return this nio-flow, for chaining
     * @throws IllegalArgumentException when no stage is named {@code anchor}
     * @throws IllegalStateException    when the nio-flow is sealed
     */
    NioFlow<T> insertAfter(String anchor, UnaryOperator<NioFlow<T>> segment);

    /**
     * An ephemeral, caller-private scope over this nio-flow: the returned view
     * starts from a snapshot of the current chain and every link declared on it is
     * the scope's own — the shared chain never changes, and concurrent scopes never
     * see each other. Injections on the scope are buffered, so links and values may
     * be declared in any order; {@code join()} then processes the buffered values
     * through the scope's chain and waits for those values only, and {@code call}
     * processes one value immediately. Scope values ride this nio-flow's engine —
     * threads, executor, backpressure — and are released on finish, keeping memory
     * flat with no {@code seal()} needed.
     *
     * <p>This is the pattern for one empty, long-lived nio-flow shared by many
     * callers, each declaring its own stages at the call site:
     *
     * <pre>{@code
     * flow.scoped()                                  // the shared bean stays empty
     *     .just("Hello")
     *     .handle("greeting", s -> s + ", World!")
     *     .join();                                   // "Hello, World!", every time
     * }</pre>
     *
     * <p>{@code onComplete}/{@code onError} on the scope observe only the scope's
     * values. Global concerns stay on the shared nio-flow: structural edits,
     * {@code metrics} and {@code trace} throw on a scope, and closing a scope never
     * stops the shared engine. Scopes are single-caller and cheap — create one per
     * use, let it go.
     *
     * @return a detached view declaring an ephemeral private chain over this engine
     */
    NioFlow<T> scoped();

    /**
     * Stops parking finished values: from now on a value reaching the end of the
     * chain is released — like under {@link #seal()} — but the chain stays open to
     * appends and structural edits; those simply no longer resume already-finished
     * values. The switch for a long-lived, request-driven nio-flow that must stay
     * editable at runtime without retaining every finished value. Idempotent, not
     * reversible; {@code seal()} additionally freezes the chain.
     *
     * @return this nio-flow, for chaining
     */
    NioFlow<T> release();

    /**
     * Freezes the chain: declaring any further link ({@code handle}, {@code submit},
     * {@code filter}, {@code when}, {@code adapt}, {@code onErrorResume}) or
     * structural edit ({@code remove}, {@code replace}, {@code insertBefore},
     * {@code insertAfter}) throws {@code IllegalStateException}. Values keep flowing, and {@code onError}/
     * {@code onComplete} observers may still be registered. Sealing also releases
     * finished values instead of parking them — nothing can resume them — so a
     * sealed, long-running nio-flow does not retain completed values. Seal every
     * stream-style nio-flow: loud failures on late mutation and flat memory.
     *
     * @return this nio-flow, for chaining
     */
    NioFlow<T> seal();

    /**
     * Waits until the nio-flow is quiescent — every injected value completed, failed
     * or was dropped — and returns the result of the newest injected value that
     * finished. If any value failed since the last call, the first {@code join()}
     * after the failure rethrows it wrapped in a {@code CompletionException} and
     * clears it, so the nio-flow stays usable.
     *
     * @return the result of the newest injected value that finished the chain
     */
    T join();

    /**
     * Like {@link #join()} but bounded: if the nio-flow is not quiescent when the
     * timeout expires, throws a {@code CompletionException} wrapping a
     * {@code TimeoutException}. In-flight work keeps running and a later join can
     * still complete normally.
     *
     * @param timeout how long to wait for quiescence before giving up
     * @return the result of the newest injected value that finished the chain
     */
    T join(Duration timeout);

    /**
     * Graceful close: drains in-flight values for a default grace period, then
     * stops the engine and releases its own resources — never an externally
     * supplied executor. Idempotent; declared without checked exceptions so
     * try-with-resources needs no catch. All views over the same running chain
     * (forks, {@code adapt}, {@code fanOut}) close the same engine.
     */
    @Override
    void close();

    /**
     * Like {@link #close()} with an explicit grace period: in-flight values get up
     * to {@code gracePeriod} to finish before the engine stops its loops.
     *
     * @param gracePeriod how long to let in-flight values finish before stopping
     */
    void close(Duration gracePeriod);

    /**
     * Fork with visually nested lanes: each branch receives its own sub-chain, so the
     * IDE indents the lane's stages one level deeper than the main line.
     *
     * <pre>{@code
     * nio-flow.when(x -> x > 10)
     *         .then(lane -> lane
     *                 .handle(x -> x * 2)
     *                 .submit(x -> slowIo(x)))
     *         .otherwise(lane -> lane
     *                 .handle(x -> x - 1))
     *         .handle(x -> audit(x));   // main line: runs for both lanes
     * }</pre>
     *
     * <p>A value only runs the stages of its own lane; stages chained after the fork
     * belong to the main line again and run for every value. {@code otherwise} is
     * optional: without it, false values skip the true lane unchanged.
     *
     * @param <T> the type of the values flowing through the fork
     */
    interface Condition<T> {

        /**
         * Builds the lane taken by values whose predicate was true.
         *
         * @param lane receives the lane's own sub-chain and declares its stages
         * @return the fork: declare the false lane with
         *         {@link Branch#otherwise(UnaryOperator)} or keep chaining on the
         *         main line
         */
        Branch<T> then(UnaryOperator<NioFlow<T>> lane);
    }

    /**
     * A fork whose true lane is already declared. Either declare the false lane with
     * {@link #otherwise(UnaryOperator)} or chain the next stage directly — every
     * {@code NioFlow} method continues on the main line, running for both lanes.
     *
     * @param <T> the type of the values flowing through the fork
     */
    interface Branch<T> extends NioFlow<T> {

        /**
         * Builds the lane taken by values whose predicate was false.
         *
         * @param lane receives the lane's own sub-chain and declares its stages
         * @return the main line, whose stages run for every value again
         */
        NioFlow<T> otherwise(UnaryOperator<NioFlow<T>> lane);
    }

    /**
     * A multi-way fork under construction: declare one lane per case with
     * {@link #is(Predicate, UnaryOperator)}, optionally a default lane with
     * {@link #otherwise(UnaryOperator)} — or chain the next stage directly and
     * unmatched values pass through unchanged.
     *
     * @param <T> the type of the values flowing through the fork
     */
    interface Cases<T> extends NioFlow<T> {

        /**
         * Declares the next case: values matching the predicate — and no earlier
         * case — take this lane, and later predicates are not evaluated for them.
         *
         * @param predicate matches the values this case takes
         * @param lane      receives the lane's own sub-chain and declares its stages
         * @return this fork, to declare the next case
         */
        Cases<T> is(Predicate<T> predicate, UnaryOperator<NioFlow<T>> lane);

        /**
         * Builds the default lane, taken by values no case matched.
         *
         * @param lane receives the lane's own sub-chain and declares its stages
         * @return the main line, whose stages run for every value again
         */
        NioFlow<T> otherwise(UnaryOperator<NioFlow<T>> lane);
    }
}
