package dev.nioflow.springwebfluxwithnioflow.client;

import dev.nioflow.springwebfluxwithnioflow.model.Order;
import dev.nioflow.springwebfluxwithnioflow.model.Receipt;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

/**
 * A real WebClient, calling the stub service over real HTTP. Every method
 * returns a Mono — and every one of them is used as an ordinary nioflow stage
 * through handleMono/adaptMono, with the engine's retry and budgets on top.
 */
@Component
public class RemoteClient {

    private final WebClient webClient;

    public RemoteClient(@Value("${stub.base-url:http://localhost:8080}") String baseUrl) {
        this.webClient = WebClient.create(baseUrl);
    }

    /** Scores the order for fraud. Returns the SAME type: a type-preserving stage. */
    public Mono<Order> score(Order order) {
        return webClient.post()
                .uri("/stub/fraud")
                .bodyValue(order)
                .retrieve()
                .bodyToMono(Order.class);
    }

    /** Charges the card. Re-types the pipeline: Order -> Receipt. */
    public Mono<Receipt> charge(Order order) {
        return webClient.post()
                .uri("/stub/charge")
                .bodyValue(order)
                .retrieve()
                .bodyToMono(Receipt.class);
    }

    /** A notification nobody should wait for: used inside a detached fork. */
    public Mono<String> notify(Receipt receipt) {
        return webClient.post()
                .uri("/stub/notify")
                .bodyValue(receipt)
                .retrieve()
                .bodyToMono(String.class);
    }
}
