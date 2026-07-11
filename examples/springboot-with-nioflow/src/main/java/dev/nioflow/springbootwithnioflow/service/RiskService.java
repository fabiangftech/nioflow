package dev.nioflow.springbootwithnioflow.service;

import dev.nioflow.springbootwithnioflow.model.Order;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class RiskService {

    private static final double RISK_THRESHOLD = 1_000;

    public boolean isRisky(Order order) {
        return order.subtotal() > RISK_THRESHOLD && !order.vip();
    }

    public Order hold(Order order) {
        return order.withStatus("ON_HOLD");
    }

    // Background link: fire-and-forget, never blocks the flow.
    public void alert(Order order) {
        log.warn("risk: order for {} flagged for manual review (subtotal {})",
                order.customer(), order.subtotal());
    }
}
