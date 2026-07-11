package dev.nioflow.springbootwithnioflow.model;

public record OrderRequest(String customer, int items, double amount, boolean vip) {
}
