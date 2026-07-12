package dev.nioflow.springbootwithnioflow.config;

import dev.nioflow.application.facade.DefaultNioFlow;
import dev.nioflow.core.facade.NioFlow;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;

/**
 * The wildcard-bean style: ONE prototype bean, no Class token, injected into
 * typed fields. DefaultNioFlow.create() is what makes it possible — and it
 * still cannot lie: its signature only produces <I, I>, so an empty flow's
 * output type always equals its input type.
 *
 * <p>Its own context on purpose: this configuration is an alternative to the
 * typed beans in {@link NioFlowConfig}, not an addition to them (two beans
 * matching the same injection point would be ambiguous).
 */
class WildcardFlowBeanTest {

    @Configuration
    static class WildcardFlowConfig {

        /** One bean for every flow: each injection point gets its own instance. */
        @Bean
        @Scope("prototype")
        public NioFlow<?, ?> nioFlow() {
            return DefaultNioFlow.create();
        }
    }

    @Component
    static class WildcardConsumer {

        private final NioFlow<String, String> textFlow;
        private final NioFlow<Integer, Integer> numberFlow;

        WildcardConsumer(NioFlow<String, String> textFlow, NioFlow<Integer, Integer> numberFlow) {
            this.textFlow = textFlow;
            this.numberFlow = numberFlow;
        }

        String shout(String value) {
            return textFlow.just(value)
                    .handle(item -> item.toUpperCase())
                    .execute();
        }

        String describe(int value) {
            return numberFlow.just(value)
                    .handle(item -> item * 2)
                    .adapt(item -> "n=" + item)     // the per-request adapt re-types it
                    .execute();
        }
    }

    @Test
    void oneWildcardBeanServesEveryTypedInjectionPoint() {
        try (var context = new AnnotationConfigApplicationContext(
                WildcardFlowConfig.class, WildcardConsumer.class)) {

            WildcardConsumer consumer = context.getBean(WildcardConsumer.class);

            assertEquals("HELLO", consumer.shout("hello"));
            assertEquals("n=14", consumer.describe(7));
        }
    }

    @Test
    void prototypeScopeGivesEachInjectionPointItsOwnFlow() {
        try (var context = new AnnotationConfigApplicationContext(
                WildcardFlowConfig.class, WildcardConsumer.class)) {

            assertNotSame(context.getBean(NioFlow.class), context.getBean(NioFlow.class),
                    "prototype: a new flow per lookup, so two fields never share a chain");
        }
    }
}
