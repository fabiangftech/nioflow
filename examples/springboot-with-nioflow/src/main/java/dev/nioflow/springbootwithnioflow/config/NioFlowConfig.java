package dev.nioflow.springbootwithnioflow.config;

import dev.nioflow.application.facade.DefaultNioEngine;
import dev.nioflow.application.facade.DefaultNioFlow;
import dev.nioflow.core.facade.NioEngine;
import dev.nioflow.core.facade.NioFlow;
import dev.nioflow.springbootwithnioflow.model.Order;
import dev.nioflow.springbootwithnioflow.model.OrderRequest;
import dev.nioflow.springbootwithnioflow.service.AuditService;
import dev.nioflow.springbootwithnioflow.service.InventoryService;
import dev.nioflow.springbootwithnioflow.service.PricingService;
import dev.nioflow.springbootwithnioflow.service.ValidationService;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;

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
     * Platform pipeline, defined once: every order inherits validation,
     * normalization, pricing, stock control, tax and audit. Convention for
     * large flows: every step is a method reference to a service — the flow
     * reads as the business spec and the logic stays testable in plain classes.
     * Named stages ("tax", "reserve") are the anchors for runtime splices
     * (see AdminController).
     */
    @Bean(destroyMethod = "close")
    public NioFlow<OrderRequest, Order> orderFlow(NioEngine orderEngine,
                                                  ValidationService validationService,
                                                  InventoryService inventoryService,
                                                  PricingService pricingService,
                                                  AuditService auditService) {
        return DefaultNioFlow.from(OrderRequest.class, orderEngine)
                .filter(validationService::isValid)
                .handle("normalize", validationService::normalize)
                .adapt(pricingService::price)
                .filter(inventoryService::hasStock)
                // Time budget: if the inventory system hangs, the order fails
                // fast with a TimeoutException instead of stalling the request.
                .handle("reserve", inventoryService::reserve, Duration.ofSeconds(2))
                .handle("tax", pricingService::withTax)
                .background("audit", auditService::record);
    }
}
