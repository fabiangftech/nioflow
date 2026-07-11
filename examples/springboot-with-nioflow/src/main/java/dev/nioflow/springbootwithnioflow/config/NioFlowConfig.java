package dev.nioflow.springbootwithnioflow.config;

import dev.nioflow.application.facade.DefaultNioEngine;
import dev.nioflow.application.facade.DefaultNioFlow;
import dev.nioflow.core.facade.NioEngine;
import dev.nioflow.core.facade.NioFlow;
import dev.nioflow.springbootwithnioflow.model.OrderReceipt;
import dev.nioflow.springbootwithnioflow.model.OrderRequest;
import dev.nioflow.springbootwithnioflow.service.AuditService;
import dev.nioflow.springbootwithnioflow.service.InventoryService;
import dev.nioflow.springbootwithnioflow.service.NotificationService;
import dev.nioflow.springbootwithnioflow.service.PricingService;
import dev.nioflow.springbootwithnioflow.service.RiskService;
import dev.nioflow.springbootwithnioflow.service.ShippingService;
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
     * The COMPLETE order pipeline lives in the shared definition and is sealed
     * at startup: seal() compiles the chain once (fusion runs, guard windows)
     * and every request follows the precompiled plan — the controller becomes
     * a one-liner. Runtime splices (AdminController) recompile once per edit.
     * Convention for large flows: every step is a method reference to a
     * service; the flow reads as the business spec.
     */
    @Bean(destroyMethod = "close")
    public NioFlow<OrderRequest, OrderReceipt> orderFlow(NioEngine orderEngine,
                                                         ValidationService validationService,
                                                         InventoryService inventoryService,
                                                         PricingService pricingService,
                                                         RiskService riskService,
                                                         ShippingService shippingService,
                                                         NotificationService notificationService,
                                                         AuditService auditService) {
        NioFlow<OrderRequest, OrderReceipt> flow = DefaultNioFlow.from(OrderRequest.class, orderEngine)

                // Platform: validation, pricing, stock, tax, audit.
                .filter(validationService::isValid)
                .handle("normalize", validationService::normalize)
                .adapt(pricingService::price)
                .filter(inventoryService::hasStock)
                .handle("reserve", inventoryService::reserve, Duration.ofSeconds(2))
                .handle("tax", pricingService::withTax)
                .background("audit", auditService::record)

                // Fraud gate: risky orders go on hold and ops is alerted;
                // trusted orders earn discounts (VIP, and bulk on top).
                .when(riskService::isRisky)
                    .then(lane -> lane
                            .handle("hold", riskService::hold)
                            .background("risk-alert", riskService::alert))
                    .otherwise(lane -> lane
                            .handle("loyalty", pricingService::loyaltyDiscount)
                            .when(pricingService::qualifiesForBulkDiscount)
                                .then(inner -> inner
                                        .handle("bulk", pricingService::bulkDiscount)))

                // Shipping tier: first match wins.
                .match()
                    .is(shippingService::isInternational, lane -> lane
                            .handle("customs", shippingService::addCustoms)
                            .handle("intl", shippingService::international))
                    .is(shippingService::qualifiesForExpress, lane -> lane
                            .handle("express", shippingService::express))
                    .is(shippingService::qualifiesForStandard, lane -> lane
                            .handle("standard", shippingService::standard))
                    .otherwise(lane -> lane
                            .handle("economy", shippingService::economy))

                // Side effects off the critical path.
                .background("notify-customer", notificationService::notifyCustomer)
                .background("notify-warehouse", notificationService::notifyWarehouse)

                .adapt(OrderReceipt::from);

        // Freeze and compile: from here on, every request runs the plan.
        orderEngine.seal();
        return flow;
    }
}
