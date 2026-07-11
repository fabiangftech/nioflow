package dev.nioflow.springbootwithnioflow.controller;

import dev.nioflow.core.facade.NioFlow;
import dev.nioflow.springbootwithnioflow.model.OrderReceipt;
import dev.nioflow.springbootwithnioflow.model.OrderRequest;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class OrderController {

    private final NioFlow<OrderRequest, OrderReceipt> orderFlow;

    public OrderController(NioFlow<OrderRequest, OrderReceipt> orderFlow) {
        this.orderFlow = orderFlow;
    }

    /**
     * The whole pipeline (validation, pricing, fraud fork, shipping match,
     * notifications) lives sealed and compiled in the shared definition, so
     * each request just runs the plan — no per-request composition at all.
     */
    @PostMapping("/orders")
    public ResponseEntity<OrderReceipt> create(@RequestBody OrderRequest request) {
        OrderReceipt receipt = orderFlow.just(request).execute();

        // Null means a shared filter cut the flow: invalid request or no stock.
        return receipt == null ? ResponseEntity.unprocessableContent().build() : ResponseEntity.ok(receipt);
    }
}
