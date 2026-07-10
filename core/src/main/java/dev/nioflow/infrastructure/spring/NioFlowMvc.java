package dev.nioflow.infrastructure.spring;

import dev.nioflow.core.facade.NioFlowGateway;
import org.springframework.web.context.request.async.DeferredResult;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * Spring WebMVC adapter over a {@link NioFlowGateway}: turns one gateway call into
 * the {@link DeferredResult} an async MVC controller returns, so the servlet thread
 * is released while the value walks the chain. Requires {@code spring-web} on the
 * classpath — a {@code compileOnly} dependency of nio-flow itself.
 *
 * <pre>{@code
 * @RestController
 * class OrderController {
 *
 *     private final NioFlowGateway<Order, Invoice> gateway;
 *
 *     @PostMapping("/orders")
 *     DeferredResult<Invoice> place(@RequestBody Order order) {
 *         return NioFlowMvc.deferred(gateway, order, Duration.ofSeconds(2));
 *     }
 * }
 * }</pre>
 *
 * <p>Controllers may equally return the gateway's {@code CompletableFuture}
 * directly — Spring MVC handles it natively; this adapter exists for codebases
 * standardized on {@code DeferredResult} and for its container-level timeout.
 */
public final class NioFlowMvc {

    private NioFlowMvc() {
    }

    /**
     * Injects the value and returns the {@code DeferredResult} resolved with that
     * value's own result, or errored with its terminal failure. Unbounded: prefer
     * {@link #deferred(NioFlowGateway, Object, Duration)} whenever the chain can
     * drop values — and in web handlers generally.
     *
     * @param <T>     the type of the values the gateway injects
     * @param <R>     the type flowing at the end of the chain
     * @param gateway the bridge into the running nio-flow
     * @param input   the value to inject
     * @return the async MVC result, resolved when this value finishes the chain
     */
    public static <T, R> DeferredResult<R> deferred(NioFlowGateway<T, R> gateway, T input) {
        return adapt(new DeferredResult<>(), gateway.call(input));
    }

    /**
     * Like {@link #deferred(NioFlowGateway, Object)} but bounded on both sides:
     * the gateway call times out and the {@code DeferredResult} carries the same
     * timeout for the servlet container.
     *
     * @param <T>     the type of the values the gateway injects
     * @param <R>     the type flowing at the end of the chain
     * @param gateway the bridge into the running nio-flow
     * @param input   the value to inject
     * @param timeout how long to wait for the value's result before giving up
     * @return the async MVC result, resolved when this value finishes the chain
     */
    public static <T, R> DeferredResult<R> deferred(NioFlowGateway<T, R> gateway, T input,
                                                    Duration timeout) {
        return adapt(new DeferredResult<>(timeout.toMillis()), gateway.call(input, timeout));
    }

    /** Resolves the deferred result from the future, whichever way it completes. */
    private static <R> DeferredResult<R> adapt(DeferredResult<R> deferred,
                                               CompletableFuture<R> future) {
        future.whenComplete((value, error) -> {
            if (error != null) {
                deferred.setErrorResult(error);
            } else {
                deferred.setResult(value);
            }
        });
        return deferred;
    }
}
