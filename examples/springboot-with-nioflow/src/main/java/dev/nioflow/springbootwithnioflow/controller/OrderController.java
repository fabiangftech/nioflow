package dev.nioflow.springbootwithnioflow.controller;

import dev.nioflow.core.facade.NioFlow;
import dev.nioflow.springbootwithnioflow.model.Order;
import dev.nioflow.springbootwithnioflow.model.OrderReceipt;
import dev.nioflow.springbootwithnioflow.model.OrderRequest;
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

    public OrderController(NioFlow<OrderRequest, Order> orderFlow,
                           PricingService pricingService,
                           RiskService riskService,
                           ShippingService shippingService) {
        this.orderFlow = orderFlow;
        this.pricingService = pricingService;
        this.riskService = riskService;
        this.shippingService = shippingService;
    }

    /**
     * Per-request pipeline on top of the shared definition (validation → pricing
     * → tax → audit). Each request composes its own fork lanes and adapts the
     * result — fully isolated from concurrent requests.
     */
    @PostMapping("/orders")
    public ResponseEntity<OrderReceipt> create(@RequestBody OrderRequest request) {
        OrderReceipt receipt = orderFlow.just(request)
                .when(riskService::isRisky)
                .then(lane -> lane.handle("hold", riskService::hold))
                .otherwise(lane -> lane.handle("discount", pricingService::loyaltyDiscount))
                .match()
                .is(order -> order.subtotal() >= 300, lane -> lane.handle(shippingService::express))
                .is(order -> order.subtotal() >= 100, lane -> lane.handle(shippingService::standard))
                .otherwise(lane -> lane.handle(shippingService::economy))
                .adapt(OrderReceipt::from)
                .execute();

        // A null result means the shared filter cut the flow: invalid request.
        return receipt == null ? ResponseEntity.unprocessableEntity().build() : ResponseEntity.ok(receipt);
    }
}
