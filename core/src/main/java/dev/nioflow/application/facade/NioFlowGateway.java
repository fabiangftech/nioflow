package dev.nioflow.application.facade;

import dev.nioflow.core.model.FlowContext;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * {@link dev.nioflow.core.facade.NioFlowGateway} implementation correlating each call through
 * {@link FlowContext}: {@code call} injects the value with a unique correlation id
 * seeded into its context, and one {@code onComplete}/{@code onError} pair —
 * registered once, on the exit view — completes the matching future when that
 * value finishes or fails. Values injected around the gateway (plain {@code just})
 * carry no id and are ignored.
 *
 * <p>Build it with {@link #of(dev.nioflow.core.facade.NioFlow, dev.nioflow.core.facade.NioFlow)}
 * after the whole chain is declared, passing the injection view and the final view
 * (they differ when the chain re-types through {@code adapt} or {@code fanOut}):
 *
 * <pre>{@code
 * NioFlow<Order> flow = new NioFlow<>();
 * NioFlow<Invoice> exit = flow
 *         .submit(order -> price(order))
 *         .adapt(order -> invoice(order));
 * NioFlowGateway<Order, Invoice> gateway = NioFlowGateway.of(flow, exit);
 * flow.seal();
 *
 * CompletableFuture<Invoice> invoice = gateway.call(order, Duration.ofSeconds(2));
 * }</pre>
 *
 * @param <T> the type of the values injected by {@code call}
 * @param <R> the type flowing at the end of the chain, delivered by the futures
 */
public final class NioFlowGateway<T, R> implements dev.nioflow.core.facade.NioFlowGateway<T, R> {

    /** Reserved context key carrying the correlation id of a gateway call. */
    public static final String CORRELATION_KEY = "nioflow.gateway.id";

    private final dev.nioflow.core.facade.NioFlow<T> entry;
    private final ConcurrentMap<Long, CompletableFuture<R>> pending = new ConcurrentHashMap<>();
    private final AtomicLong ids = new AtomicLong();

    private NioFlowGateway(dev.nioflow.core.facade.NioFlow<T> entry) {
        this.entry = entry;
    }

    /**
     * Bridges a nio-flow whose chain does not change type: the injection view is
     * also the final view.
     *
     * @param <T>  the type flowing through the whole chain
     * @param flow the nio-flow to bridge, its chain fully declared
     * @return the gateway; each {@code call} resolves to that value's own result
     */
    public static <T> NioFlowGateway<T, T> of(dev.nioflow.core.facade.NioFlow<T> flow) {
        return of(flow, flow);
    }

    /**
     * Bridges a re-typed chain: values enter through {@code entry} and results are
     * observed on {@code exit} — the final view, after the last {@code adapt}.
     * Registers one {@code onComplete} and one {@code onError} handler on the exit
     * view; create the gateway after declaring the whole chain, before calling.
     *
     * @param <T>   the type of the values injected by {@code call}
     * @param <R>   the type flowing at the end of the chain
     * @param entry the view values are injected through
     * @param exit  the final view of the same running chain
     * @return the gateway; each {@code call} resolves to that value's own result
     */
    public static <T, R> NioFlowGateway<T, R> of(dev.nioflow.core.facade.NioFlow<T> entry,
                                                 dev.nioflow.core.facade.NioFlow<R> exit) {
        NioFlowGateway<T, R> gateway = new NioFlowGateway<>(entry);
        exit.onComplete(gateway::complete);
        exit.onError(gateway::fail);
        return gateway;
    }

    @Override
    public CompletableFuture<R> call(T input) {
        return call(input, Map.of(), null);
    }

    @Override
    public CompletableFuture<R> call(T input, Map<String, Object> context) {
        return call(input, context, null);
    }

    @Override
    public CompletableFuture<R> call(T input, Duration timeout) {
        return call(input, Map.of(), timeout);
    }

    @Override
    public CompletableFuture<R> call(T input, Map<String, Object> context, Duration timeout) {
        long id = ids.incrementAndGet();
        CompletableFuture<R> future = new CompletableFuture<>();
        pending.put(id, future);
        if (timeout != null) {
            future.orTimeout(timeout.toNanos(), TimeUnit.NANOSECONDS);
        }
        // however the future completes — result, failure, timeout — the call is forgotten
        future.whenComplete((result, error) -> pending.remove(id));
        Map<String, Object> seeded = new HashMap<>(context);
        seeded.put(CORRELATION_KEY, id);
        try {
            entry.just(input, seeded);
        } catch (RuntimeException rejected) {
            future.completeExceptionally(rejected);
        }
        return future;
    }

    @Override
    public int pending() {
        return pending.size();
    }

    /** Exit handler: resolves the finished value's future, if it was a gateway call. */
    private void complete(R value) {
        CompletableFuture<R> future = claim();
        if (future != null) {
            future.complete(value);
        }
    }

    /** Error handler: fails the value's future, if it was a gateway call. */
    private void fail(Throwable error) {
        CompletableFuture<R> future = claim();
        if (future != null) {
            future.completeExceptionally(error);
        }
    }

    /**
     * The pending future of the value bound to this thread, claimed at most once —
     * fan-out siblings after the first, replayed failures and non-gateway values
     * all resolve to null.
     */
    private CompletableFuture<R> claim() {
        return FlowContext.get(CORRELATION_KEY) instanceof Long id ? pending.remove(id) : null;
    }
}
