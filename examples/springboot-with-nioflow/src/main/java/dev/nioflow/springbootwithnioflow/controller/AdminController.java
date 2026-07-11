package dev.nioflow.springbootwithnioflow.controller;

import dev.nioflow.core.facade.NioEngine;
import dev.nioflow.core.model.Background;
import dev.nioflow.core.model.Decision;
import dev.nioflow.core.model.Filter;
import dev.nioflow.core.model.Recovery;
import dev.nioflow.core.model.Splice;
import dev.nioflow.core.model.Stage;
import dev.nioflow.springbootwithnioflow.model.Order;
import dev.nioflow.springbootwithnioflow.service.AuditService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * Runtime editing of the shared order pipeline: replaces the "tax" stage with
 * a new rate WITHOUT redeploying and WITHOUT touching in-flight requests
 * (each execution runs on the chain snapshot it started with).
 */
@RestController
@RequestMapping("/admin")
public class AdminController {

    private final NioEngine orderEngine;
    private final AuditService auditService;

    public AdminController(NioEngine orderEngine, AuditService auditService) {
        this.orderEngine = orderEngine;
        this.auditService = auditService;
    }

    @PutMapping("/tax/{rate}")
    public String updateTaxRate(@PathVariable double rate) {
        orderEngine.splice("tax", Splice.REPLACE, List.of(new Stage("tax", value -> {
            Order order = (Order) value;
            return order.withTax(order.subtotal() * rate);
        }, false, null, List.of())));
        return "tax stage replaced at runtime: rate=" + rate;
    }

    @GetMapping("/chain")
    public List<String> chain() {
        return orderEngine.chain().stream()
                .map(link -> switch (link) {
                    case Stage stage -> "stage:" + stage.name();
                    case Background background -> "background:" + background.name();
                    case Filter ignored -> "filter";
                    case Decision decision -> "decision:" + decision.id();
                    case Recovery ignored -> "recovery";
                })
                .toList();
    }

    @GetMapping("/audit-count")
    public long auditCount() {
        return auditService.count();
    }
}
