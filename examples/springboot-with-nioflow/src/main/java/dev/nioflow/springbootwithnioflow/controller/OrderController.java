package dev.nioflow.springbootwithnioflow.controller;

import dev.nioflow.core.facade.FlowResult.Completed;
import dev.nioflow.core.facade.FlowResult.Filtered;
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
        // executeResult() tells a deliberate filter cut (invalid request or no
        // stock) apart from a completed receipt — no null ambiguity.
        return switch (orderFlow.just(request).executeResult()) {
            case Completed<OrderReceipt>(OrderReceipt receipt) -> ResponseEntity.ok(receipt);
            case Filtered<OrderReceipt>() -> ResponseEntity.unprocessableContent().build();
        };
    }
}
