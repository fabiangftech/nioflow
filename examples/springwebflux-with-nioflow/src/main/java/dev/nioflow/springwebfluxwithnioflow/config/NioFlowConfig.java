package dev.nioflow.springwebfluxwithnioflow.config;

import dev.nioflow.application.facade.DefaultNioFlow;
import dev.nioflow.infrastructure.reactive.Reactive;
import dev.nioflow.infrastructure.reactive.ReactiveFlow;
import dev.nioflow.springwebfluxwithnioflow.model.Receipt;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;

/**
 * The shared definition, as a singleton bean.
 *
 * <p>{@code ReactiveFlow<String, Receipt>} IS a {@code NioFlow<String, Receipt>}
 * — the reactive facade is a subinterface, not a wrapper — so the type still
 * states the contract: an order id goes in, a Receipt comes out, and the
 * compiler holds every pipeline to it.
 *
 * <p>Singleton on purpose: a flow is the shared definition (that is what makes
 * shared chains, batching and runtime edits possible), and it serves any number
 * of concurrent requests because every {@code just()} opens an isolated
 * execution. destroyMethod = "close" drains the engine on shutdown.
 *
 * <p>The {@code defaultBudget} is not decoration. This flow talks to the network
 * on nearly every step, and a reactive stage parks a virtual worker on its Mono:
 * without a budget, one hung connection parks that worker for the life of the
 * JVM (the engine has no cancellation to take it back). Three seconds is the
 * backstop for every reactive step that declares nothing — the fraud and charge
 * calls set tighter ones of their own, and the notification inside the fork,
 * which declares none, is now covered too.
 *
 * <p>{@code propagate(TRACE)} is the context bridge, declared once: the trace id
 * that {@link TraceWebFilter} put in Reactor's subscriber context is lifted into
 * the per-execution Context on every {@code executeMono()}. This line is the
 * reason no controller method and no service method takes a {@code traceId}
 * parameter — and the reason a reader of this config can see exactly what
 * crosses the boundary, which is the whole point of declaring it rather than
 * discovering it.
 */
@Configuration
public class NioFlowConfig {

    @Bean(destroyMethod = "close")
    public ReactiveFlow<String, Receipt> orders() {
        return Reactive.<String, Receipt>flow(DefaultNioFlow.from(String.class))
                .defaultBudget(Duration.ofSeconds(3))
                .propagate(FlowKeys.TRACE);
    }
}
