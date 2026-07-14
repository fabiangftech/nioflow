package dev.nioflow.springwebfluxwithnioflow.controller;

import dev.nioflow.springwebfluxwithnioflow.model.Order;
import dev.nioflow.springwebfluxwithnioflow.model.Receipt;
import dev.nioflow.springwebfluxwithnioflow.service.OrderService;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * A nioflow pipeline IS the handler: the controller returns the Mono the
 * pipeline ends in, and Netty writes the response when it completes.
 */
@RestController
public class OrderController {

    private final OrderService orders;

    public OrderController(OrderService orders) {
        this.orders = orders;
    }

    /** filter() -> empty Mono -> 404. The two notions of "nothing" line up. */
    @GetMapping("/orders/{id}")
    public Mono<Order> find(@PathVariable String id) {
        return orders.find(id)
                .switchIfEmpty(Mono.error(new ResponseStatusException(HttpStatus.NOT_FOUND, "no order " + id)));
    }

    /** Blocking repo + two WebClient stages + a detached fork, behind one Mono. */
    @PostMapping("/orders/{id}/pay")
    public Mono<Receipt> pay(@PathVariable String id,
                             @RequestParam(defaultValue = "trace-1") String traceId) {
        return orders.pay(id, traceId)
                .switchIfEmpty(Mono.error(new ResponseStatusException(HttpStatus.NOT_FOUND, "no order " + id)));
    }

    /**
     * A Flux in, a Flux out. Concurrency is the backpressure knob; the ordering
     * guarantee comes from the engine's key(), not from serializing the stream.
     */
    @PostMapping("/orders/pay-all")
    public Flux<Receipt> payAll(@RequestParam(defaultValue = "1,2,3") String ids,
                                @RequestParam(defaultValue = "8") int concurrency) {
        return orders.payAll(Flux.fromArray(ids.split(",")), concurrency);
    }

    /**
     * The ingestion loop: an unknown id is a poison element, and it is dropped
     * (reported to the log) instead of killing the stream — which is what would
     * happen on /orders/pay-all.
     */
    @PostMapping("/orders/ingest")
    public Flux<Receipt> ingest(@RequestParam(defaultValue = "1,2,3") String ids,
                                @RequestParam(defaultValue = "8") int concurrency) {
        return orders.ingest(Flux.fromArray(ids.split(",")), concurrency);
    }

    /** Where each part of the request actually ran. The point of the example. */
    @GetMapping("/threads")
    public Mono<String> threads() {
        return orders.threads(Thread.currentThread().getName());
    }
}
