package dev.nioflow.springwebfluxwithnioflow;

import dev.nioflow.springwebfluxwithnioflow.model.Order;
import dev.nioflow.springwebfluxwithnioflow.model.Receipt;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.web.reactive.server.WebTestClient;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.boot.test.context.SpringBootTest.WebEnvironment.DEFINED_PORT;

/**
 * The example, exercised end to end over real HTTP — the WebClient stages really
 * do leave the process and come back.
 */
/*
 * A fixed port on purpose: the WebClient stages call the stub service, which IS
 * this application, so the base URL has to be known before the context starts —
 * which rules out RANDOM_PORT (the port is only assigned afterwards).
 */
@SpringBootTest(webEnvironment = DEFINED_PORT, properties = {
        "server.port=8089",
        "stub.base-url=http://localhost:8089"
})
class OrderEndpointsTest {

    private final WebTestClient client = WebTestClient.bindToServer()
            .baseUrl("http://localhost:8089")
            .responseTimeout(Duration.ofSeconds(10))
            .build();

    @Test
    void anUnknownOrderIsAnEmptyMonoAndThereforeA404() {
        client.get().uri("/orders/{id}", "does-not-exist")
                .exchange()
                .expectStatus().isNotFound();
    }

    @Test
    void aKnownOrderComesBackThroughTheBlockingRepository() {
        client.get().uri("/orders/{id}", "1")
                .exchange()
                .expectStatus().isOk()
                .expectBody(Order.class)
                .value(order -> assertEquals("alice", order.customerId()));
    }

    @Test
    void payRunsBlockingAndReactiveStagesInOneChain() {
        client.post().uri("/orders/{id}/pay", "1")
                .exchange()
                .expectStatus().isOk()
                .expectBody(Receipt.class)
                .value(receipt -> {
                    assertEquals("1", receipt.orderId());
                    assertEquals("PAID", receipt.status());
                    assertEquals(12_000, receipt.chargedCents());
                });
    }

    @Test
    void theFraudStageDeclinesTheLoudOnes() {
        // Order 2 is 90.000 cents: the fraud service (a real WebClient call)
        // flags it, and the charge stage declines it.
        client.post().uri("/orders/{id}/pay", "2")
                .exchange()
                .expectStatus().isOk()
                .expectBody(Receipt.class)
                .value(receipt -> assertEquals("DECLINED", receipt.status()));
    }

    @Test
    void theResponseDoesNotWaitForTheDetachedFork() {
        // The notify fork takes 300ms. The chain the caller waits for is
        // repo (40) + fraud (60) + charge (80) = ~180ms. If the response ever
        // took as long as the fork, fork() would not be detached at all.
        pay("3");                       // warm up: the first call pays for the
                                        // connection pool and the JIT, not for the fork
        long elapsedMillis = pay("3");

        assertTrue(elapsedMillis < 300,
                "the response waited for the 300ms fork: took " + elapsedMillis + "ms");
    }

    private long pay(String id) {
        long start = System.nanoTime();
        client.post().uri("/orders/{id}/pay", id)
                .exchange()
                .expectStatus().isOk();
        return (System.nanoTime() - start) / 1_000_000;
    }

    @Test
    void aFluxOfOrdersGoesThroughTheFlow() {
        client.post().uri("/orders/pay-all?ids=1,2,3&concurrency=8")
                .exchange()
                .expectStatus().isOk()
                .expectBodyList(Receipt.class)
                .value(receipts -> {
                    assertEquals(3, receipts.size());
                    // payAll has no fraud stage: every order charges straight through.
                    assertTrue(receipts.stream().allMatch(receipt -> "PAID".equals(receipt.status())),
                            "every order should have been charged: " + receipts);
                });
    }

    /**
     * The claim the whole example exists to make: YOUR code never runs on a Netty
     * event loop. And the trap that comes with it: an operator chained onto the
     * WebClient's Mono does — which is why work belongs in a stage, not in a
     * map() on the Mono.
     */
    @Test
    void stagesRunOnVirtualWorkersAndNeverOnNettysEventLoop() {
        String report = client.get().uri("/threads")
                .exchange()
                .expectStatus().isOk()
                .expectBody(String.class)
                .returnResult()
                .getResponseBody();

        // The controller was entered on a Netty event loop, and released at once.
        assertTrue(report.contains("controller="), report);

        // Every STAGE — before the reactive call and after it — ran on a virtual
        // worker. This is what lets the blocking repository call be safe.
        assertTrue(report.contains("stage=VIRTUAL:"),
                "a stage must run on a virtual worker: " + report);
        assertTrue(report.contains("afterReactiveStage=VIRTUAL:"),
                "the chain must resume on a worker after the Mono: " + report);

        // And the documented trap, asserted so nobody "fixes" the docs away: a
        // .map() chained on the Mono runs on the thread that completes it.
        assertTrue(report.contains("insideMonoOperator=platform:reactor-http-nio"),
                "an operator on the Mono runs on Netty — that is why work goes in stages: " + report);
    }
}
