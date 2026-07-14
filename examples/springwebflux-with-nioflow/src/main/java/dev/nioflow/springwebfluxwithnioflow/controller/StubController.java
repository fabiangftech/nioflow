package dev.nioflow.springwebfluxwithnioflow.controller;

import dev.nioflow.springwebfluxwithnioflow.model.Order;
import dev.nioflow.springwebfluxwithnioflow.model.Receipt;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

import java.time.Duration;

/**
 * The "remote services" the RemoteClient calls, so the example is a real HTTP
 * round trip and not a mock: the calls really do leave the process, and the
 * Netty event loop really does stay free while they are in flight.
 */
@RestController
@RequestMapping("/stub")
public class StubController {

    /** Flags the loud ones. Slow enough to be visible. */
    @PostMapping("/fraud")
    public Mono<Order> fraud(@RequestBody Order order) {
        return Mono.delay(Duration.ofMillis(60))
                .map(ignored -> order.withFraud(order.amountCents() > 80_000));
    }

    @PostMapping("/charge")
    public Mono<Receipt> charge(@RequestBody Order order) {
        return Mono.delay(Duration.ofMillis(80))
                .map(ignored -> new Receipt(order.id(),
                        order.fraudulent() ? "DECLINED" : "PAID",
                        order.fraudulent() ? 0 : order.amountCents()));
    }

    /** The one nobody waits for. Deliberately the slowest of the three. */
    @PostMapping("/notify")
    public Mono<String> notifyCustomer(@RequestBody Receipt receipt) {
        return Mono.delay(Duration.ofMillis(300))
                .map(ignored -> "notified:" + receipt.orderId());
    }
}
