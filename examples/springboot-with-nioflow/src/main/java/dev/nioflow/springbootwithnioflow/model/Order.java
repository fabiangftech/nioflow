package dev.nioflow.springbootwithnioflow.model;

public record Order(String customer, String country, boolean vip, int items, double subtotal,
                    double tax, double discount, double customsFee, String shipping,
                    double shippingCost, String status) {

    public Order withTax(double tax) {
        return new Order(customer, country, vip, items, subtotal, tax, discount, customsFee,
                shipping, shippingCost, status);
    }

    public Order withDiscount(double discount) {
        return new Order(customer, country, vip, items, subtotal, tax, discount, customsFee,
                shipping, shippingCost, status);
    }

    public Order withCustomsFee(double customsFee) {
        return new Order(customer, country, vip, items, subtotal, tax, discount, customsFee,
                shipping, shippingCost, status);
    }

    public Order withShipping(String shipping, double shippingCost) {
        return new Order(customer, country, vip, items, subtotal, tax, discount, customsFee,
                shipping, shippingCost, status);
    }

    public Order withStatus(String status) {
        return new Order(customer, country, vip, items, subtotal, tax, discount, customsFee,
                shipping, shippingCost, status);
    }

    public double total() {
        return subtotal + tax + customsFee + shippingCost - discount;
    }
}
