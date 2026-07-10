package dev.nioflow.core.facade;


import dev.nioflow.core.model.Diagnostics;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

public interface NioFlow<T> {

    NioFlow<T> just(T input);

    /**
     * Like {@link #just(Object)} with seed metadata (trace id, tenant, ...) that
     * travels with the value: any stage of it can read or extend it through
     * {@code FlowContext}, no matter which thread runs the stage.
     */
    NioFlow<T> just(T input, Map<String, Object> context);

    /** Injects every value in iteration order, honoring backpressure per value. */
    NioFlow<T> justAll(Iterable<T> inputs);

    NioFlow<T> handle(Function<T, T> function);

    /**
     * Like {@link #handle(Function)} but named: a failure arrives wrapped in a
     * {@code StageException} carrying the stage name, so errors say where they
     * happened. Unnamed stages deliver the thrown exception untouched.
     */
    NioFlow<T> handle(String name, Function<T, T> function);

    /**
     * Like {@link #handle(Function)} with the stage decorated by a {@link Resilience}
     * policy. With the default virtual handle workers any policy works. On a fixed
     * worker pool ({@code IOPipeline(executor, handleWorkers)}) prefer non-blocking
     * policies (e.g. a circuit breaker): a waiting policy ties up a shared worker.
     */
    NioFlow<T> handle(Function<T, T> function, Resilience<T> resilience);

    NioFlow<T> submit(Function<T, T> function);

    /**
     * Like {@link #submit(Function)} but named: a failure arrives wrapped in a
     * {@code StageException} carrying the stage name.
     */
    NioFlow<T> submit(String name, Function<T, T> function);

    /**
     * Like {@link #submit(Function)} but bounded: when the timeout expires the worker
     * is interrupted and only that value fails, with a {@code TimeoutException}.
     */
    NioFlow<T> submit(Function<T, T> function, Duration timeout);

    /**
     * Like {@link #submit(Function)} with the stage decorated by a {@link Resilience}
     * policy (retries, circuit breaker, ...). Exhausted policies fail only that value,
     * like any other stage error.
     */
    NioFlow<T> submit(Function<T, T> function, Resilience<T> resilience);

    /**
     * Groups values into lists of up to {@code size}, flushing a partial group after
     * its oldest value waited {@code maxWait}, and runs one async call for the whole
     * group — ideal for bulk IO like JDBC batch writes. The function must return
     * exactly one result per input, matched by index; each result continues down the
     * chain as its own value. A failure fails every value of the group, each one
     * individually recoverable by a downstream {@code onErrorResume}.
     */
    NioFlow<T> batch(int size, Duration maxWait, Function<List<T>, List<T>> function);

    <N> NioFlow<N> adapt(Function<T, N> function);

    /**
     * Splits a value into many: each element of the returned list continues down the
     * chain as its own independent value, inheriting the parent's lanes; the parent
     * is consumed. An empty list drops the value like a {@code filter}. Runs like a
     * {@code handle} — keep it fast; produce the elements with a {@code submit}
     * first when IO is involved.
     */
    <N> NioFlow<N> fanOut(Function<T, List<N>> function);

    /**
     * Splices a reusable segment — a function over the nio-flow itself — into the
     * chain at this point, so sub-nio-flows can be defined once and composed
     * anywhere, including inside lanes. The segment may change the type.
     *
     * <pre>{@code
     * static Function<Pipeline<Order>, Pipeline<Invoice>> billing() {
     *     return p -> p
     *             .submit(o -> price(o))
     *             .adapt(o -> invoice(o));
     * }
     *
     * nio-flow.via(billing())
     *         .submit(invoice -> send(invoice));
     * }</pre>
     */
    default <N> NioFlow<N> via(Function<? super NioFlow<T>, ? extends NioFlow<N>> segment) {
        return segment.apply(this);
    }

    /**
     * Keeps only values matching the predicate. Dropped values leave deliberately:
     * they fire neither {@code onComplete} nor {@code onError}, stop counting toward
     * {@code join()} and free their backpressure slot. A throwing predicate is a
     * normal stage failure.
     */
    NioFlow<T> filter(Predicate<T> predicate);

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
     */
    Cases<T> match();

    /**
     * Registers a non-blocking handler for terminal failures (recovered values never
     * reach it). Handlers registered after failures happened get the most recent
     * ones replayed — a bounded history, so a long-running nio-flow does not retain
     * every throwable it ever saw.
     */
    NioFlow<T> onError(Consumer<Throwable> handler);

    /**
     * Recovery link: when a value fails at any upstream link, the fallback turns the
     * error into a replacement value and the flow resumes from here — instead of the
     * value being dropped. Values flowing normally skip it. If the fallback itself
     * throws, the error moves on to the next recovery downstream. Like any link, it
     * only catches failures that happen after it was declared — declare the chain,
     * recoveries included, before injecting values.
     */
    NioFlow<T> onErrorResume(Function<Throwable, T> fallback);

    /**
     * Registers a handler invoked every time a value finishes the chain. Failed
     * values go to {@code onError} instead. Register it on the final view of the
     * chain (after any {@code adapt}) and before injecting values.
     */
    NioFlow<T> onComplete(Consumer<T> handler);

    /**
     * Registers the observability sink for lifecycle counters and per-stage latency;
     * register it before injecting values. A second call replaces the first.
     */
    NioFlow<T> metrics(NioFlowMetrics metrics);

    /**
     * Opt-in trace mode: the tracer sees every transition of every value — stage
     * in/out, lanes taken, drops, splits, recoveries, failures and completions.
     * Register it before injecting values; a second call replaces the first.
     */
    NioFlow<T> trace(NioFlowTracer tracer);

    /**
     * A point-in-time snapshot of the nio-flow: chain shape, queue depths, active,
     * parked and batched counts. Its {@code toString()} renders the full dump.
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
     */
    NioFlow<T> seal();

    T join();

    /**
     * Like {@link #join()} but bounded: if the nio-flow is not quiescent when the
     * timeout expires, throws a {@code CompletionException} wrapping a
     * {@code TimeoutException}. In-flight work keeps running and a later join can
     * still complete normally.
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
     */
    interface Condition<T> {

        Branch<T> then(UnaryOperator<NioFlow<T>> lane);
    }

    interface Branch<T> extends NioFlow<T> {

        NioFlow<T> otherwise(UnaryOperator<NioFlow<T>> lane);
    }

    interface Cases<T> extends NioFlow<T> {

        Cases<T> is(Predicate<T> predicate, UnaryOperator<NioFlow<T>> lane);

        NioFlow<T> otherwise(UnaryOperator<NioFlow<T>> lane);
    }
}
