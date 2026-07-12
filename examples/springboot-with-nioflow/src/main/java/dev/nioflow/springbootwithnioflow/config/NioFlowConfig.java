package dev.nioflow.springbootwithnioflow.config;

import dev.nioflow.application.facade.DefaultNioFlow;
import dev.nioflow.core.facade.NioFlow;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

@Configuration
public class NioFlowConfig {

    @Bean
    @Scope("prototype")
    public NioFlow<?, ?> nioFlow() {
        return new DefaultNioFlow<>();
    }
}
