package dev.nioflow.springbootwithnioflow.config;

import dev.nioflow.application.facade.DefaultNioFlow;
import dev.nioflow.core.facade.NioFlow;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

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
    public NioFlow<String, String> greetingFlow() {
        return DefaultNioFlow.from(String.class);
    }

    @Bean
    public NioFlow<String, String> textFlow() {
        return DefaultNioFlow.from(String.class);
    }

    @Bean
    public NioFlow<String, String> gatewayFlow() {
        return DefaultNioFlow.from(String.class);
    }

    @Bean
    public NioFlow<String, String> enrichFlow() {
        return DefaultNioFlow.from(String.class);
    }

    /** Level 8: SampleService declares the batch link on THIS shared definition. */
    @Bean
    public NioFlow<String, String> bulkFlow() {
        return DefaultNioFlow.from(String.class);
    }

    @Bean
    public NioFlow<Integer, Integer> numberFlow() {
        return DefaultNioFlow.from(Integer.class);
    }

    @Bean
    public NioFlow<Integer, Integer> orderedFlow() {
        return DefaultNioFlow.from(Integer.class);
    }

    @Bean
    public NioFlow<Integer, Integer> reportFlow() {
        return DefaultNioFlow.from(Integer.class);
    }

    /**
     * Input and output of different types — the normal case, not an exception:
     * the shared chain takes an Integer and leaves a String, so the bean IS a
     * {@code NioFlow<Integer, String>}. The type is not claimed, it is earned:
     * {@code from(Integer.class)} starts at {@code <Integer, Integer>} and the
     * {@code adapt} moves the output type, with the compiler watching.
     * Everything chained after {@code just()} then starts from String.
     */
    @Bean
    public NioFlow<Integer, String> invoiceFlow() {
        return DefaultNioFlow.from(Integer.class)
                .handle("apply-vat", cents -> cents * 121 / 100)
                .adapt(cents -> "EUR " + (cents / 100) + "." + String.format("%02d", cents % 100));
    }
}
