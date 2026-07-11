package dev.nioflow.springbootwithnioflow.model;

public record OrderRequest(String customer, String country, int items, double amount, boolean vip) {

    public OrderRequest withCustomer(String customer) {
        return new OrderRequest(customer, country, items, amount, vip);
    }

    public OrderRequest withCountry(String country) {
        return new OrderRequest(customer, country, items, amount, vip);
    }
}
