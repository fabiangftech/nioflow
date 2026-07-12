package dev.nioflow.springbootwithnioflow.config;

import dev.nioflow.application.facade.DefaultNioFlow;
import dev.nioflow.core.facade.NioFlow;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

/**
 * A flow bean states its contract: {@code NioFlow<I, T>} takes an I in
 * {@code just()} and gives a T back from {@code execute()}.
 * {@code DefaultNioFlow.from(Type.class)} is the only way to open one and it
 * always starts at {@code <I, I>} — only {@code adapt()} moves the output
 * type, and the compiler checks that step.
 *
 * <p>A single {@code NioFlow<?, ?>} bean injected everywhere would break that
 * promise: the wildcards let the container hand the same flow to a
 * {@code NioFlow<Integer, String>} field, whose types would then be fiction.
 * Declare one bean per contract instead; Spring matches several beans of the
 * same type by field name.
 *
 * <p>Singletons on purpose: a flow IS the shared definition — that is what
 * makes shared chains, batching and runtime edits possible — and it serves any
 * number of concurrent requests, because every {@code just()} opens an
 * isolated execution.
 */
@Configuration
public class NioFlowConfig {

    @Bean
    @Scope("prototype")
    public NioFlow<?, ?> nioFlow() {
        return DefaultNioFlow.create();
    }
}
