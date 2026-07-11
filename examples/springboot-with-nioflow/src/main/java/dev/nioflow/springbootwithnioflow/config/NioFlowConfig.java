package dev.nioflow.springbootwithnioflow.config;

import dev.nioflow.application.facade.DefaultNioFlow;
import dev.nioflow.core.facade.NioFlow;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class NioFlowConfig {

    @Bean(destroyMethod = "close")
    public NioFlow nioFlow() {
        return new DefaultNioFlow();
    }
}
