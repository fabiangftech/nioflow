package dev.nioflow.springwebfluxwithnioflow.repository;

import dev.nioflow.springwebfluxwithnioflow.model.Order;
import org.springframework.stereotype.Repository;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Deliberately BLOCKING — this stands in for JDBC/JPA, the thing WebFlux has no
 * good answer for. In a plain WebFlux app this call would either block the Netty
 * event loop (fatal) or need publishOn(Schedulers.boundedElastic()), a capped
 * pool of platform threads that a slow database saturates.
 *
 * <p>Here it just runs in a nioflow stage, which means it runs on a VIRTUAL
 * worker: the only thread that parks is one that costs nothing to park.
 */
@Repository
public class OrderRepository {

    private static final Map<String, Order> ORDERS = new ConcurrentHashMap<>(Map.of(
            "1", new Order("1", "alice", 12_000, false),
            "2", new Order("2", "bob", 90_000, false),
            "3", new Order("3", "alice", 4_500, false)));

    /** Blocks the calling thread, like a real JDBC driver. */
    public Order findById(String id) {
        sleep(40);
        return ORDERS.get(id);   // null = unknown order
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(interrupted);
        }
    }
}
