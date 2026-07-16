package dev.nioflow.springbootwithnioflow.config;

import dev.nioflow.application.facade.DefaultNioFlow;
import dev.nioflow.core.facade.NioFlow;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * One typed bean per contract. A flow bean states its contract:
 * {@code NioFlow<I, O>} takes an I in {@code just()} and gives an O back from
 * {@code execute()}. {@code DefaultNioFlow.from(Type.class)} is the only way to
 * open one, and the {@code Class} token is checked at runtime: {@code just}
 * rejects an input that is not an I with a clear {@code IllegalArgumentException}.
 *
 * <p>Typed beans, NOT a single {@code NioFlow<?, ?>} injected everywhere: the
 * wildcards would let the container hand the same flow to a
 * {@code NioFlow<Integer, String>} field, whose types would then be fiction —
 * the exact hole the {@code from(Class)} design closes. Spring matches several
 * beans of the same type (the five {@code NioFlow<String, String>} here) by the
 * injection point's NAME, which Lombok's {@code @RequiredArgsConstructor} takes
 * from the field name, so each bean method is named after the field it feeds.
 * (The wildcard-bean style is still shown, deliberately, in
 * {@code WildcardFlowBeanTest} — as the alternative and its limit, not the
 * recommendation.)
 *
 * <p>Singletons on purpose: a flow IS the shared definition — that is what makes
 * shared chains, batching and runtime edits possible — and it serves any number
 * of concurrent requests, because every {@code just()} opens an isolated
 * execution. {@code destroyMethod = "close"} shuts the engine each flow owns
 * when the context closes.
 */
@Configuration
public class NioFlowConfig {

    // ── String in, String out ────────────────────────────────────────────

    @Bean(destroyMethod = "close")
    public NioFlow<String, String> greetingFlow() {
        return DefaultNioFlow.from(String.class);
    }

    @Bean(destroyMethod = "close")
    public NioFlow<String, String> textFlow() {
        return DefaultNioFlow.from(String.class);
    }

    @Bean(destroyMethod = "close")
    public NioFlow<String, String> gatewayFlow() {
        return DefaultNioFlow.from(String.class);
    }

    @Bean(destroyMethod = "close")
    public NioFlow<String, String> enrichFlow() {
        return DefaultNioFlow.from(String.class);
    }

    @Bean(destroyMethod = "close")
    public NioFlow<String, String> bulkFlow() {
        return DefaultNioFlow.from(String.class);
    }

    // ── Integer in, Integer out ──────────────────────────────────────────

    @Bean(destroyMethod = "close")
    public NioFlow<Integer, Integer> numberFlow() {
        return DefaultNioFlow.from(Integer.class);
    }

    @Bean(destroyMethod = "close")
    public NioFlow<Integer, Integer> orderedFlow() {
        return DefaultNioFlow.from(Integer.class);
    }

    @Bean(destroyMethod = "close")
    public NioFlow<Integer, Integer> reportFlow() {
        return DefaultNioFlow.from(Integer.class);
    }

    // ── Integer in, String out (adapt moves I → O per request) ───────────

    @Bean(destroyMethod = "close")
    public NioFlow<Integer, String> invoiceFlow() {
        return DefaultNioFlow.from(Integer.class);
    }

    @Bean(destroyMethod = "close")
    public NioFlow<Integer, String> creditFlow() {
        return DefaultNioFlow.from(Integer.class);
    }
}
