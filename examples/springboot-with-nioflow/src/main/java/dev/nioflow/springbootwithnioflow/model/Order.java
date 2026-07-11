package dev.nioflow.springbootwithnioflow.model;

public record Order(String customer, boolean vip, double subtotal, double tax, double discount,
                    String shipping, double shippingCost, String status) {

    public Order withTax(double tax) {
        return new Order(customer, vip, subtotal, tax, discount, shipping, shippingCost, status);
    }

    public Order withDiscount(double discount) {
        return new Order(customer, vip, subtotal, tax, discount, shipping, shippingCost, status);
    }

    public Order withShipping(String shipping, double shippingCost) {
        return new Order(customer, vip, subtotal, tax, discount, shipping, shippingCost, status);
    }

    public Order withStatus(String status) {
        return new Order(customer, vip, subtotal, tax, discount, shipping, shippingCost, status);
    }

    public double total() {
        return subtotal + tax + shippingCost - discount;
    }
}
