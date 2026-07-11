package dev.nioflow.springbootwithnioflow.model;

public record OrderReceipt(String customer, String status, String shipping, double subtotal,
                           double tax, double discount, double shippingCost, double total) {

    public static OrderReceipt from(Order order) {
        return new OrderReceipt(order.customer(), order.status(), order.shipping(), order.subtotal(),
                order.tax(), order.discount(), order.shippingCost(), order.total());
    }
}
