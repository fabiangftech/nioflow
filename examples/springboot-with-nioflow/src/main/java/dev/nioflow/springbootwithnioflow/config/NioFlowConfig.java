package dev.nioflow.springbootwithnioflow.config;

import dev.nioflow.application.facade.DefaultNioFlow;
import dev.nioflow.core.facade.NioFlow;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


@Configuration
public class NioFlowConfig {

    @Bean(name = "nioFlow")
    public NioFlow<String> nioFlow() {
        return new DefaultNioFlow<String>()
                .handle(input -> input + " World!")
                .seal();
    }
}
