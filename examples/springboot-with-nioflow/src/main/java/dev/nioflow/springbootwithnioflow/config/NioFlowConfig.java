package dev.nioflow.springbootwithnioflow.config;

import dev.nioflow.application.facade.DefaultNioEngine;
import dev.nioflow.application.facade.DefaultNioFlow;
import dev.nioflow.core.facade.NioEngine;
import dev.nioflow.core.facade.NioFlow;
import dev.nioflow.core.facade.Segment;
import dev.nioflow.core.model.Retry;
import dev.nioflow.springbootwithnioflow.model.Order;
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

    @Bean
    public NioFlow<String, String> greetingFlow() {
        return DefaultNioFlow.from(String.class);
    }

    @Bean
    public NioEngine orderEngine() {
        return new DefaultNioEngine();
    }

    /**
     * The pipeline is composed from reusable, independently testable segments
     * — each one a named chapter of the business spec — then sealed at
     * startup so every request runs the compiled plan. The controller stays a
     * one-liner; runtime splices (AdminController) recompile once per edit.
     */
    @Bean
    public NioFlow<OrderRequest, OrderReceipt> orderFlow(NioEngine orderEngine,
                                                         ValidationService validationService,
                                                         InventoryService inventoryService,
                                                         PricingService pricingService,
                                                         RiskService riskService,
                                                         ShippingService shippingService,
                                                         NotificationService notificationService,
                                                         AuditService auditService) {
        NioFlow<OrderRequest, OrderReceipt> flow = DefaultNioFlow.from(OrderRequest.class, orderEngine)
                .use(platform(validationService, inventoryService, pricingService, auditService))
                .use(fraudGate(riskService, pricingService))
                .use(shippingTier(shippingService))
                .use(notifications(notificationService))
                .adapt(OrderReceipt::from);

        // Freeze and compile: from here on, every request runs the plan.
        orderEngine.seal();
        return flow;
    }

    /** Platform: validation, pricing, stock control, tax and audit. */
    private static Segment<OrderRequest, Order> platform(ValidationService validationService,
                                                         InventoryService inventoryService,
                                                         PricingService pricingService,
                                                         AuditService auditService) {
        return lane -> lane
                .filter(validationService::isValid)
                .handle("normalize", validationService::normalize)
                .adapt(pricingService::price)
                .filter(inventoryService::hasStock)
                // Transient inventory hiccups retry with backoff; each attempt
                // has its own 2s budget; persistent failure fails the order fast.
                .handle("reserve", inventoryService::reserve,
                        Duration.ofSeconds(2), Retry.of(3, Duration.ofMillis(100)))
                .handle("tax", pricingService::withTax)
                .background("audit", auditService::record);
    }

    /**
     * Fraud gate: risky orders go on hold and ops is alerted; trusted orders
     * earn discounts (VIP, and bulk on top).
     */
    private static Segment<Order, Order> fraudGate(RiskService riskService,
                                                   PricingService pricingService) {
        return lane -> lane
                .when(riskService::isRisky)
                    .then(risky -> risky
                            .handle("hold", riskService::hold)
                            .background("risk-alert", riskService::alert))
                    .otherwise(trusted -> trusted
                            .handle("loyalty", pricingService::loyaltyDiscount)
                            .when(pricingService::qualifiesForBulkDiscount)
                                .then(bulk -> bulk
                                        .handle("bulk", pricingService::bulkDiscount)));
    }

    /** Shipping tier: first match wins. */
    private static Segment<Order, Order> shippingTier(ShippingService shippingService) {
        return lane -> lane
                .match()
                    .is(shippingService::isInternational, intl -> intl
                            .handle("customs", shippingService::addCustoms)
                            .handle("intl", shippingService::international))
                    .is(shippingService::qualifiesForExpress, express -> express
                            .handle("express", shippingService::express))
                    .is(shippingService::qualifiesForStandard, standard -> standard
                            .handle("standard", shippingService::standard))
                    .otherwise(economy -> economy
                            .handle("economy", shippingService::economy));
    }

    /** Side effects off the critical path. */
    private static Segment<Order, Order> notifications(NotificationService notificationService) {
        return lane -> lane
                .background("notify-customer", notificationService::notifyCustomer)
                .background("notify-warehouse", notificationService::notifyWarehouse);
    }
}
