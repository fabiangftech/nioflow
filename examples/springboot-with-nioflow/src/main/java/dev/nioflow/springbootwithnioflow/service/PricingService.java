package dev.nioflow.springbootwithnioflow.service;

import dev.nioflow.springbootwithnioflow.model.Order;
import dev.nioflow.springbootwithnioflow.model.OrderRequest;
import org.springframework.stereotype.Service;

@Service
public class PricingService {

    private static final double TAX_RATE = 0.19;
    private static final double VIP_DISCOUNT = 0.05;
    private static final double BULK_DISCOUNT = 0.03;
    private static final int BULK_THRESHOLD = 10;

    public Order price(OrderRequest request) {
        return new Order(request.customer(), request.country(), request.vip(), request.items(),
                request.amount(), 0, 0, 0, null, 0, "CREATED");
    }

    public Order withTax(Order order) {
        return order.withTax(order.subtotal() * TAX_RATE);
    }

    public Order loyaltyDiscount(Order order) {
        return order.vip() ? order.withDiscount(order.discount() + order.subtotal() * VIP_DISCOUNT) : order;
    }

    public boolean qualifiesForBulkDiscount(Order order) {
        return order.items() >= BULK_THRESHOLD;
    }

    public Order bulkDiscount(Order order) {
        return order.withDiscount(order.discount() + order.subtotal() * BULK_DISCOUNT);
    }
}
