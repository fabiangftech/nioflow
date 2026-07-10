package dev.nioflow.core.facade;


import dev.nioflow.core.model.Diagnostics;

import java.time.Duration;
import java.util.List;
import java.util.Map;
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
public interface NioFlow<T> {

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
     * Freezes the chain: declaring any further link ({@code handle}, {@code submit},
     * {@code filter}, {@code when}, {@code adapt}, {@code onErrorResume}) throws
     * {@code IllegalStateException}. Values keep flowing, and {@code onError}/
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
