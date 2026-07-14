package dev.nioflow.springwebfluxwithnioflow.model;

public record Receipt(String orderId, String status, int chargedCents) {
}
