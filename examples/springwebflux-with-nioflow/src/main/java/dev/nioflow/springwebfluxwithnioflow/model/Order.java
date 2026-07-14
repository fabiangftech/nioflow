package dev.nioflow.springwebfluxwithnioflow.model;

public record Order(String id, String customerId, int amountCents, boolean fraudulent) {

    public Order withFraud(boolean flagged) {
        return new Order(id, customerId, amountCents, flagged);
    }

    public boolean highValue() {
        return amountCents > 50_000;
    }
}
