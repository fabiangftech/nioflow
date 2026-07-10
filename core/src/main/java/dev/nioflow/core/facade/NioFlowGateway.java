package dev.nioflow.core.facade;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Request/response bridge over a nio-flow: each {@code call} injects one value and
 * returns a future completed with that value's own result — unlike {@code join()},
 * which waits for the whole nio-flow, and {@code onComplete}, which observes every
 * value. This is the natural fit for request-driven callers (a web handler, an RPC
 * endpoint): declare and seal the chain once at startup, then serve each request
 * with a {@code call}.
 *
 * <p>Calls inherit the nio-flow's semantics: many calls are in flight concurrently,
 * a value blocked on slow IO never delays the values behind it, and a failure —
 * after every {@code onErrorResume} had its chance — completes only that call's
 * future exceptionally.
 *
 * <p>A value dropped by a {@code filter} fires no handlers, so its future would
 * never complete: prefer the {@code timeout} variants whenever the chain can drop
 * values — request-driven callers should bound every call anyway. A {@code fanOut}
 * produces many results for one call; the first one to finish completes the future
 * and the rest are ignored.
 *
 * @param <T> the type of the values injected by {@code call}
 * @param <R> the type flowing at the end of the chain, delivered by the futures
 */
public interface NioFlowGateway<T, R> {

    /**
     * Injects the value and returns its future, completed when this value — and
     * only this value — finishes the chain, or completed exceptionally when it
     * fails past every recovery. Rejected admission (a closed nio-flow, a failing
     * backpressure policy) completes the future exceptionally instead of throwing.
     *
     * @param input the value to inject
     * @return a future delivering this value's own end-of-chain result
     */
    CompletableFuture<R> call(T input);

    /**
     * Like {@link #call(Object)} with seed metadata (trace id, tenant, ...) copied
     * into the value's {@code FlowContext}. The {@code "nioflow.gateway.id"} key is
     * reserved for the gateway's own correlation.
     *
     * @param input   the value to inject
     * @param context initial metadata copied into the value's {@code FlowContext}
     * @return a future delivering this value's own end-of-chain result
     */
    CompletableFuture<R> call(T input, Map<String, Object> context);

    /**
     * Like {@link #call(Object)} but bounded: if the value has not finished when
     * the timeout expires — slow stages, or dropped by a {@code filter} — the
     * future completes exceptionally with a {@code TimeoutException} and the
     * gateway forgets the call. The value itself keeps flowing.
     *
     * @param input   the value to inject
     * @param timeout how long to wait for the value's result before giving up
     * @return a future delivering this value's own end-of-chain result
     */
    CompletableFuture<R> call(T input, Duration timeout);

    /**
     * Like {@link #call(Object, Map)} and {@link #call(Object, Duration)} combined:
     * seed metadata plus a bound on how long the caller waits.
     *
     * @param input   the value to inject
     * @param context initial metadata copied into the value's {@code FlowContext}
     * @param timeout how long to wait for the value's result before giving up
     * @return a future delivering this value's own end-of-chain result
     */
    CompletableFuture<R> call(T input, Map<String, Object> context, Duration timeout);

    /**
     * How many calls are still waiting for their result — in-flight values plus
     * values dropped by a {@code filter} whose unbounded futures will never
     * complete.
     *
     * @return the number of futures handed out and not yet completed
     */
    int pending();
}
