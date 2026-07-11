package dev.nioflow.springbootwithnioflow.model;

public record OrderReceipt(String customer, String country, String status, String shipping,
                           int items, double subtotal, double tax, double discount,
                           double customsFee, double shippingCost, double total) {

    public static OrderReceipt from(Order order) {
        return new OrderReceipt(order.customer(), order.country(), order.status(), order.shipping(),
                order.items(), order.subtotal(), order.tax(), order.discount(),
                order.customsFee(), order.shippingCost(), order.total());
    }
}
