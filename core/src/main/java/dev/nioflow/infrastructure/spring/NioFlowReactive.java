package dev.nioflow.infrastructure.spring;

import dev.nioflow.core.facade.NioFlowGateway;
import reactor.core.publisher.Mono;

import java.time.Duration;

/**
 * Spring WebFlux adapter over a {@link NioFlowGateway}: turns one gateway call into
 * the {@link Mono} a reactive handler returns. Requires {@code reactor-core} on the
 * classpath — a {@code compileOnly} dependency of nio-flow itself.
 *
 * <pre>{@code
 * @RestController
 * class OrderController {
 *
 *     private final NioFlowGateway<Order, Invoice> gateway;
 *
 *     @PostMapping("/orders")
 *     Mono<Invoice> place(@RequestBody Order order) {
 *         return NioFlowReactive.mono(gateway, order, Duration.ofSeconds(2));
 *     }
 * }
 * }</pre>
 *
 * <p>The monos are cold: nothing is injected until subscription, so a
 * {@code retry} re-injects a fresh value. Cancelling the subscription abandons the
 * result but does not stop the value — it keeps flowing to the end of the chain.
 */
public final class NioFlowReactive {

    private NioFlowReactive() {
    }

    /**
     * A cold mono that, on subscribe, injects the value and emits that value's own
     * result — or errors with its terminal failure. Unbounded: prefer
     * {@link #mono(NioFlowGateway, Object, Duration)} whenever the chain can drop
     * values — and in web handlers generally.
     *
     * @param <T>     the type of the values the gateway injects
     * @param <R>     the type flowing at the end of the chain
     * @param gateway the bridge into the running nio-flow
     * @param input   the value to inject on each subscription
     * @return a mono emitting this value's end-of-chain result
     */
    public static <T, R> Mono<R> mono(NioFlowGateway<T, R> gateway, T input) {
        return Mono.defer(() -> Mono.fromCompletionStage(gateway.call(input)));
    }

    /**
     * Like {@link #mono(NioFlowGateway, Object)} but bounded: when the timeout
     * expires — slow stages, or a value dropped by a {@code filter} — the mono
     * errors with a {@code TimeoutException} and the gateway forgets the call.
     *
     * @param <T>     the type of the values the gateway injects
     * @param <R>     the type flowing at the end of the chain
     * @param gateway the bridge into the running nio-flow
     * @param input   the value to inject on each subscription
     * @param timeout how long to wait for the value's result before giving up
     * @return a mono emitting this value's end-of-chain result
     */
    public static <T, R> Mono<R> mono(NioFlowGateway<T, R> gateway, T input, Duration timeout) {
        return Mono.defer(() -> Mono.fromCompletionStage(gateway.call(input, timeout)));
    }
}
