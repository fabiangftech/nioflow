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
 */
@Configuration
public class NioFlowConfig {

    @Bean(destroyMethod = "close")
    public ReactiveFlow<String, Receipt> orders() {
        return Reactive.<String, Receipt>flow(DefaultNioFlow.from(String.class))
                .defaultBudget(Duration.ofSeconds(3));
    }
}
