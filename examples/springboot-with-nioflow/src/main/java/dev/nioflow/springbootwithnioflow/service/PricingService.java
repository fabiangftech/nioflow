package dev.nioflow.springbootwithnioflow.service;

import dev.nioflow.springbootwithnioflow.model.Order;
import dev.nioflow.springbootwithnioflow.model.OrderRequest;
import org.springframework.stereotype.Service;

@Service
public class PricingService {

    private static final double TAX_RATE = 0.19;
    private static final double VIP_DISCOUNT = 0.05;

    public boolean isValid(OrderRequest request) {
        return request.customer() != null && !request.customer().isBlank()
                && request.items() > 0 && request.amount() > 0;
    }

    public Order price(OrderRequest request) {
        return new Order(request.customer(), request.vip(), request.amount(), 0, 0, null, 0, "CREATED");
    }

    public Order withTax(Order order) {
        return order.withTax(order.subtotal() * TAX_RATE);
    }

    public Order loyaltyDiscount(Order order) {
        return order.vip() ? order.withDiscount(order.subtotal() * VIP_DISCOUNT) : order;
    }
}
