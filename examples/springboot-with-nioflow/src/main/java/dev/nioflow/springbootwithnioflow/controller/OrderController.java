package dev.nioflow.springbootwithnioflow.controller;

import dev.nioflow.core.facade.NioFlow;
import dev.nioflow.springbootwithnioflow.model.Order;
import dev.nioflow.springbootwithnioflow.model.OrderReceipt;
import dev.nioflow.springbootwithnioflow.model.OrderRequest;
import dev.nioflow.springbootwithnioflow.service.NotificationService;
import dev.nioflow.springbootwithnioflow.service.PricingService;
import dev.nioflow.springbootwithnioflow.service.RiskService;
import dev.nioflow.springbootwithnioflow.service.ShippingService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class OrderController {

    private final NioFlow<OrderRequest, Order> orderFlow;
    private final PricingService pricingService;
    private final RiskService riskService;
    private final ShippingService shippingService;
    private final NotificationService notificationService;

    public OrderController(NioFlow<OrderRequest, Order> orderFlow,
                           PricingService pricingService,
                           RiskService riskService,
                           ShippingService shippingService,
                           NotificationService notificationService) {
        this.orderFlow = orderFlow;
        this.pricingService = pricingService;
        this.riskService = riskService;
        this.shippingService = shippingService;
        this.notificationService = notificationService;
    }

    /**
     * Business routing per request, on top of the shared platform pipeline
     * (validation → normalize → pricing → stock → tax → audit). The flow only
     * references service methods: each lane reads as a business rule and the
     * whole pipeline as the spec. Fully isolated from concurrent requests.
     */
    @PostMapping("/orders")
    public ResponseEntity<OrderReceipt> create(@RequestBody OrderRequest request) {
        OrderReceipt receipt = orderFlow.just(request)

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

                .adapt(OrderReceipt::from)
                .execute();

        // Null means a shared filter cut the flow: invalid request or no stock.
        return receipt == null ? ResponseEntity.unprocessableContent().build() : ResponseEntity.ok(receipt);
    }
}
