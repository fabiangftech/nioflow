package dev.nioflow.springbootwithnioflow.config;

import dev.nioflow.application.facade.DefaultNioEngine;
import dev.nioflow.application.facade.DefaultNioFlow;
import dev.nioflow.core.facade.NioEngine;
import dev.nioflow.core.facade.NioFlow;
import dev.nioflow.springbootwithnioflow.model.Order;
import dev.nioflow.springbootwithnioflow.model.OrderRequest;
import dev.nioflow.springbootwithnioflow.service.AuditService;
import dev.nioflow.springbootwithnioflow.service.PricingService;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class NioFlowConfig {

    @Bean(destroyMethod = "close")
    public NioFlow<String, String> greetingFlow() {
        return DefaultNioFlow.from(String.class);
    }

    @Bean
    public NioEngine orderEngine() {
        return new DefaultNioEngine();
    }

    /**
     * Shared definition, built once: every request inherits validation, pricing,
     * tax and audit. The "tax" stage is named so it can be replaced at runtime
     * (see AdminController) without touching in-flight requests.
     */
    @Bean(destroyMethod = "close")
    public NioFlow<OrderRequest, Order> orderFlow(NioEngine orderEngine,
                                                  PricingService pricingService,
                                                  AuditService auditService) {
        return DefaultNioFlow.from(OrderRequest.class, orderEngine)
                .filter(pricingService::isValid)
                .adapt(pricingService::price)
                .handle("tax", pricingService::withTax)
                .background("audit", auditService::record);
    }
}
