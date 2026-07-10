package dev.nioflow.springbootwithnioflow.config;

import dev.nioflow.application.facade.DefaultNioFlow;
import dev.nioflow.core.facade.NioFlow;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;


@Configuration
public class NioFlowConfig {

    @Bean(name = "nioFlow", destroyMethod = "close")
    public NioFlow<String> nioFlow() {
        return new DefaultNioFlow<>();
    }
}
