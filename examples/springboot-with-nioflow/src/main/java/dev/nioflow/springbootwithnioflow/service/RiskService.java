package dev.nioflow.springbootwithnioflow.service;

import dev.nioflow.springbootwithnioflow.model.Order;
import org.springframework.stereotype.Service;

@Service
public class RiskService {

    private static final double RISK_THRESHOLD = 1_000;

    public boolean isRisky(Order order) {
        return order.subtotal() > RISK_THRESHOLD && !order.vip();
    }

    public Order hold(Order order) {
        return order.withStatus("ON_HOLD");
    }
}
